import pandas
import time
import logging
import colorlog
import json
import os
from dataclasses import dataclass, field, asdict
from kafka.admin import KafkaAdminClient
from kafka.consumer.fetcher import ConsumerRecord


# Configuration paths
path_servers_list = "./config/broker_servers.txt"
path_log_dir = "./log/"


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
class Stats:
    """Mutable counters for one cycle & initialized with default 0 values."""
    tx_count: int = field(default=0, init=False)
    fraud_count: int = field(default=0, init=False)
    total_amount: float = field(default=0.0, init=False)
    fraud_amount: float = field(default=0.0, init=False)


@dataclass
class Cycle:
    """Tracks stats over fixed-duration windows & prints a summary."""
    logger: object = field(init=True)
    duration: int = field(default=60, init=True)
    start: float = field(default=0.0, init=False)
    is_1st_record: bool = field(default=True, init=False)
    df_summary: pandas.DataFrame = field(init=False)
    # stats' "factory" calls constructor Stats()
    stats: Stats = field(default_factory=Stats, init=False)

    def __post_init__(self) -> None:
        """DF Summary with named index & float 1000-separator format"""
        columns = ["fraud_count", "tx_count",
                   "total_amount", "fraud_amount", "non_fraud_amount",
                   "fraud_ratio_%"]
        self.df_summary = pandas.DataFrame(columns=columns)
        self.df_summary.index.name = "cycle_ref"
        pandas.set_option("display.float_format", "{:,.2f}".format)

        self.reset_counters()

    def reset_counters(self) -> None:
        """Zero all counters & restart cycle timer."""
        self.stats.fraud_count = 0
        self.stats.tx_count = 0
        self.stats.fraud_amount = 0
        self.stats.total_amount = 0
        self.start = time.time()

    def set_counters_per_transaction(self, transaction: Transaction) -> None:
        """Integrate one transaction to the current cycle's counters."""
        self.stats.tx_count += 1
        tx_amount = transaction.amount
        self.stats.total_amount += tx_amount

        if transaction.isFraud == 1:
            self.stats.fraud_count += 1
            self.stats.fraud_amount += tx_amount

    def get_counters_per_cycle(self) -> None:
        """Append current cycle's stats as a new DF row & log the row."""
        idx = len(self.df_summary)

        stats_dict = asdict(self.stats)
        fields = ["tx_count", "fraud_count", "total_amount", "fraud_amount"]
        self.df_summary.loc[idx, fields] = [stats_dict[col] for col in fields]

        # Derived fields: non-fraud amount & fraud ratio
        self.df_summary.loc[idx, "non_fraud_amount"] = \
            self.stats.total_amount - self.stats.fraud_amount
        if self.stats.total_amount:
            self.df_summary.loc[idx, "fraud_ratio_%"] = \
                self.stats.fraud_amount / self.stats.total_amount * 100

        # Log the new row in a readable key:value format
        dict_summary = self.df_summary.iloc[-1].to_dict()
        str_summary = "\n".join(f"{k:>40}: {v:>20,.0f}"
                                for k, v in dict_summary.items())
        self.logger.info(f"_______________ Cycle {idx:>5} ______________\n" +
                         str_summary)

    def get_summary_of_all_cycles(self) -> None:
        """Log the full per-cycle table plus a totals row."""
        # Per-cycle summary table
        summary_title = "_______________ SUMMARY ALL CYCLES _______________"
        str_summary = f"{self.df_summary.reset_index().to_string(index=False)}"
        self.logger.info(f"{summary_title}\n{str_summary}")

        # All-cycles' Totals table
        totals_title = "_____________________ TOTALS _____________________"
        df_totals = self.df_summary.sum().to_frame().T
        if df_totals["total_amount"].values[0] != 0:
            df_totals["fraud_ratio_%"] = \
                df_totals["fraud_amount"] / df_totals["total_amount"] * 100
        nb_cycles = len(self.df_summary)
        df_totals.insert(0, "nb_cycles", nb_cycles)
        str_totals = f"{df_totals.to_string(index=False)}"
        self.logger.info(f"{totals_title}\n{str_totals}")

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
    """Get the current number of partitions for a topic."""
    topic_metadata = client.describe_topics([topic_name])[0]
    current_nb = len(topic_metadata["partitions"])
    return current_nb


def convert_dict_to_bytes(dict_record: dict) -> bytes:
    """Producer value_serializer: dict -> JSON string -> UTF-8 bytes."""
    str_record = json.dumps(dict_record)
    bytes_record = str_record.encode()
    return bytes_record


def setup_logging(logfile_name: str) -> None:
    """
    Configure logging:
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
    custom_logger.setLevel(logging.DEBUG)
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
