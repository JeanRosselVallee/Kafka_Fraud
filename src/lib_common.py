import pandas
import time
import logging
import colorlog
import json
import os
from dataclasses import dataclass, fields, field, asdict
from kafka.admin import KafkaAdminClient
from kafka.consumer.fetcher import ConsumerRecord


# Configuration paths
path_servers_list = "./config/broker_servers.txt"
path_log_dir = "./log/"

app_log_level = logging.DEBUG  # DEBUG or INFO


@dataclass
class Transaction:
    """Single bank transaction record & mirrors Kafka message fields."""
    step: int
    type: str
    amount: float
    nameOrig: str
    oldbalanceOrg: float
    newbalanceOrig: float
    nameDest: str
    oldbalanceDest: float
    newbalanceDest: float
    isFraud: int


@dataclass
class Counters:
    """Per-cycle counters; zero-initialized.
    Derived fields (non_fraud_amount, fraud_ratio_pct) are computed later."""

    cycle_ref: int = field(default=0, init=False)
    tx_count: int = field(default=0, init=True)
    fraud_count: int = field(default=0, init=True)
    total_amount: float = field(default=0.0, init=True)
    fraud_amount: float = field(default=0.0, init=True)
    non_fraud_amount: float = field(default=None, init=False)
    fraud_ratio_pct: float = field(default=None, init=False)
    nb_cycles: int = field(default=1, init=False)
    time_elapsed: float = field(default=0.0, init=False)


@dataclass
class RollingWindow:
    """Holds rolling history of window aggregates &
    requested window duration (in seconds)."""

    duration: int = field(default=30, init=True)
    df_history: pandas.DataFrame = field(init=False)

    def get_and_historize_window(self, cycle: Cycle) -> None:
        """Aggregates current cycle history into a window totals row;
        appends it to window's history."""

        # Get totals of cycles in current window
        df_current_window = cycle.df_history.fillna(0).sum().to_frame().T

        # Set window's reference = last cycle's reference
        df_current_window.loc[0, "cycle_ref"] = cycle.idx

        # Append derived fields to DF current window
        total_amount = df_current_window["total_amount"].values[0]
        fraud_amount = df_current_window["fraud_amount"].values[0]
        # 1. Non-fraud amount
        df_current_window.loc[0, "non_fraud_amount"] = \
            total_amount - fraud_amount
        # 2. Fraud ratio
        if total_amount != 0:
            df_current_window.loc[0, "fraud_ratio_pct"] = \
                fraud_amount / total_amount * 100

        # Append current window to DF history
        self.df_history = pandas.concat([self.df_history,
                                         df_current_window])

    def log_window(self, cycle: Cycle) -> None:
        "Logs cycle detail and window totals;"
        "DEBUG shows detail, INFO shows totals only."

        # Detail of window's cycles
        str_detail = f"{cycle.df_history.fillna("").to_string(index=False)}"
        # Totals of window's cycles
        str_totals = f"{self.df_history.iloc[-1:].to_string(index=False)}"

        if cycle.logger.isEnabledFor(logging.DEBUG):
            cycle.logger.debug(
                f"\n{'=' * 49} Rolling Window #{cycle.idx:>5} {'=' * 41}" +
                f"\n{str_detail}" +
                f"\n{'_' * 65} Totals {'_' * 42}" +
                f"\n{str_totals}"
            )
        else:
            cycle.logger.info(
                f"\n{str_totals}"
            )

    def log_windows_summary(self, logger: logging.Logger) -> None:
        """Log full window history & global totals across all windows."""

        # 1. Log-print DF history of windows
        str_windows = f"{self.df_history.to_string(index=False)}"
        logger.info(f"\n{'=' * 46} Summary of Rolling Windows {'=' * 41}" +
                    f"\n{str_windows}")

        # 2. Log-print Totals out of all windows
        df_totals = self.df_history.sum().to_frame().T
        # Remove unused "cycle_ref" kept only for alignment with previous table
        df_totals.loc[df_totals.index[-1], "cycle_ref"] = None
        # Get global fraud ratio
        if df_totals["total_amount"].values[0] != 0:
            df_totals["fraud_ratio_pct"] = \
                df_totals["fraud_amount"] / df_totals["total_amount"] * 100
        str_totals = f"{df_totals.to_string(index=False)}"
        logger.info(f"\n{'=' * 46} Totals of Rolling Windows {'=' * 41}" +
                    f"\n{str_totals}")


@dataclass
class Cycle:
    """Handles per-cycle counters & historizes cycle & window aggregates."""

    idx: int = field(default=0, init=False)
    logger: object = field(init=True)  # init: required arg at instantiation
    duration: int = field(default=1, init=True)
    start: float = field(default=0.0, init=False)
    df_history: pandas.DataFrame = field(init=False)

    # "default_factory" calls Class' constructor
    counters: Counters = field(default_factory=Counters, init=False)
    counter_fieldnames: list[str] = field(
        default_factory=lambda: [f.name for f in fields(Counters)], init=False)
    # window: RollingWindow = field(default_factory=RollingWindow, init=True)

    def __post_init__(self) -> None:
        """Initializes empty cycle-history DF
        & set pandas float display format."""

        self.df_history = pandas.DataFrame(columns=self.counter_fieldnames)
        pandas.set_option("display.float_format", "{:,.2f}".format)

        self.reset_counters()

    def reset_counters(self) -> None:
        """Zeroes all counters & reset the cycle start time."""

        self.counters = Counters()
        self.start = time.time()

    def update_counters(self, transaction: Transaction) -> None:
        """Adds counts and amounts of 1 transaction to current cycle."""

        self.counters.tx_count += 1
        tx_amount = transaction.amount
        self.counters.total_amount += tx_amount

        if transaction.isFraud == 1:
            self.counters.fraud_count += 1
            self.counters.fraud_amount += tx_amount

    def historize_cycle(self, window_duration: int) -> None:
        """Appends current cycle's counters as a new row;
        truncates history to window_duration."""

        # Increment cycle index and set cycle reference
        self.idx += 1

        # Append current cycle's counters to DF history of cycles
        cycle_counters_dict = asdict(self.counters)
        self.df_history.loc[self.idx, self.counter_fieldnames] = \
            [cycle_counters_dict[col] for col in self.counter_fieldnames]

        # Set "cycle_ref" = current cycle's index
        self.df_history.loc[self.idx, "cycle_ref"] = self.idx

        # Truncate DF history to window duration
        self.df_history = self.df_history[-window_duration:]

        # Log cycles' stats of current window for Debugging
        # self.logger.debug(f"{self.df_history}")

    def check_1st_record(self, record: ConsumerRecord) -> None:
        """Debug-log the first consumed record's fields."""

        dict_record = record._asdict()
        str_record = f"{'\n'.join(f"{k:>40}: {v}"
                                  for k, v in dict_record.items())}"
        self.logger.debug("Transaction is found in record's field 'Value':\n" +
                          str_record)


def get_servers_list() -> list:
    """Read the Kafka broker list file, skipping blank lines."""

    with open(path_servers_list) as f:
        # Remove newline chars from  non-empty lines
        servers = [line.strip() for line in f if line.strip()]
        return servers


def get_nb_partitions(client: KafkaAdminClient, topic_name: str) -> int:
    """Return the current number of partitions for topic_name."""

    topic_metadata = client.describe_topics([topic_name])[0]
    current_nb = len(topic_metadata["partitions"])
    return current_nb


def convert_dict_to_bytes(dict_record: dict) -> bytes:
    """Producer value_serializer: dict -> JSON string -> UTF-8 bytes."""

    str_record = json.dumps(dict_record)
    bytes_record = str_record.encode()
    return bytes_record


def setup_logging(logfile_name: str) -> logging.Logger:
    """Configures logging:
    - root logger for other modules: colored output with gray background
    - fraud_detection logger: colored output with no background
    Returns the app logger.
    """

    global_format = "%(log_color)s%(asctime)s %(message)s"

    custom_colors = {
        "DEBUG": "cyan",
        "INFO": "green",
        "WARNING": "yellow",
        "ERROR": "red"
    }

    # Other Apps" logs with a gray background
    gray_bg = "bg_light_black"
    other_apps_colors = {k: f"{v},{gray_bg}" for k, v in custom_colors.items()}

    # 1. Root logger for other modules
    # Colored handler with gray background
    other_apps_handler = logging.StreamHandler()
    other_apps_handler.setFormatter(
        colorlog.ColoredFormatter(
            global_format,
            log_colors=other_apps_colors
        )
    )
    logging.getLogger().addHandler(other_apps_handler)
    logging.getLogger().setLevel(logging.INFO)

    # 2. App logger
    custom_logger = logging.getLogger("fraud_detection")
    custom_logger.setLevel(app_log_level)
    custom_logger.propagate = False  # Prevent duplicate output to root logger

    # 2.1 Console colored handler
    custom_handler = colorlog.StreamHandler()
    custom_handler.setFormatter(
        colorlog.ColoredFormatter(
            global_format,
            log_colors=custom_colors
        )
    )
    custom_logger.addHandler(custom_handler)

    # 2.2 File non-colored handler
    os.makedirs(path_log_dir, exist_ok=True)
    logfile_path = os.path.join(path_log_dir, logfile_name)
    file_handler = logging.FileHandler(logfile_path, mode="a")
    file_handler.setFormatter(logging.Formatter(
        "%(asctime)s %(levelname)s %(message)s"))
    file_handler.setLevel(logging.DEBUG)
    custom_logger.addHandler(file_handler)
    print(f"Cf. {logfile_path} for logs")

    return custom_logger
