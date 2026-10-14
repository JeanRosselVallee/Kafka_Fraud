import pandas
import time
import logging
import colorlog
from dataclasses import dataclass, field, asdict


@dataclass
class Transaction:
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
    tx_count: int = field(default=0, init=False)
    fraud_count: int = field(default=0, init=False)
    total_amount: float = field(default=0.0, init=False)
    fraud_amount: float = field(default=0.0, init=False)


@dataclass
class Cycle:
    logger: object = field(init=True)
    duration: int = field(default=60, init=True)
    start: float = field(default=0.0, init=False)
    end: float = field(default=0.0, init=False)
    df_summary: pandas.DataFrame = field(init=False)
    # stats calls constructor Stats()
    stats: Stats = field(default_factory=Stats, init=False)

    def __post_init__(self):
        # DF Summary
        columns = ["fraud_count", "tx_count",
                   "total_amount", "fraud_amount", "non_fraud_amount",
                   "fraud_ratio_%"]
        self.df_summary = pandas.DataFrame(columns=columns)
        self.df_summary.index.name = "cycle_ref"
        pandas.set_option("display.float_format", "{:,.2f}".format)

        self.reset_counters()

    def reset_counters(self):
        self.stats.fraud_count = 0
        self.stats.tx_count = 0
        self.stats.fraud_amount = 0
        self.stats.total_amount = 0
        self.start = time.time()

    def set_counters_per_transaction(self, transaction):
        self.stats.tx_count += 1
        tx_amount = transaction.amount
        self.stats.total_amount += tx_amount

        if transaction.isFraud == 1:
            self.stats.fraud_count += 1
            self.stats.fraud_amount += tx_amount

    def get_counters_per_cycle(self):
        idx = len(self.df_summary)

        stats_dict = asdict(self.stats)
        fields = ["tx_count", "fraud_count", "total_amount", "fraud_amount"]
        self.df_summary.loc[idx, fields] = [stats_dict[col] for col in fields]

        self.df_summary.loc[idx, "non_fraud_amount"] = \
            self.stats.total_amount - self.stats.fraud_amount
        if self.stats.total_amount:
            self.df_summary.loc[idx, "fraud_ratio_%"] = \
                self.stats.fraud_amount / self.stats.total_amount * 100

        dict_summary = self.df_summary.iloc[-1].to_dict()
        str_summary = "\n".join(f"{k:>40}: {v:>20,.0f}"
                                for k, v in dict_summary.items())
        self.logger.info(f"_______________ Cycle {idx:>5} ______________\n" +
                         str_summary)

    def get_summary_of_all_cycles(self):
        # Print Summary table
        summary_title = "_______________ SUMMARY ALL CYCLES _______________"
        str_summary = f"{self.df_summary.reset_index().to_string(index=False)}"
        self.logger.info(f"{summary_title}\n{str_summary}")

        # Print Totals table
        totals_title = "_____________________ TOTALS _____________________"
        df_totals = self.df_summary.sum().to_frame().T
        if df_totals["total_amount"].values[0] != 0:
            df_totals["fraud_ratio_%"] = \
                df_totals["fraud_amount"] / df_totals["total_amount"] * 100
        nb_cycles = len(self.df_summary)
        df_totals.insert(0, "nb_cycles", nb_cycles)
        str_totals = f"{df_totals.to_string(index=False)}"
        self.logger.info(f"{totals_title}\n{str_totals}")

    def check_1st_record(self, record):
        # Print record's contents as a dictionary
        dict_record = record._asdict()
        str_record = f"{'\n'.join(f"{k:>40}: {v}"
                                  for k, v in dict_record.items())}"
        self.logger.debug("Transaction is found in record's field 'Value':\n" +
                          str_record)


def setup_logging() -> None:
    """
    Set up logging configuration for the application:
    - a basic format handler for other modules
    - color format handler specifically for "fraud_detection" logger
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

    # 1. Basic format handler for other modules
    other_apps_handler = logging.StreamHandler()
    other_apps_handler.setFormatter(colorlog.ColoredFormatter(
            global_format,
            log_colors=other_apps_colors
        )
    )
    logging.getLogger().addHandler(other_apps_handler)
    logging.getLogger().setLevel(logging.INFO)

    # 2. Color format handler for app fraud_detection
    custom_handler = colorlog.StreamHandler()
    custom_handler.setFormatter(colorlog.ColoredFormatter(
        global_format,
        log_colors=custom_colors
    ))

    custom_logger = logging.getLogger("fraud_detection")
    custom_logger.addHandler(custom_handler)
    custom_logger.setLevel(logging.DEBUG)
    custom_logger.propagate = False  # Prevent duplicate output to root logger

    return custom_logger
