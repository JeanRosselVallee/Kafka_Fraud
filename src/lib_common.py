import pandas
import time
import logging
import colorlog


class CycleCounters:
    def __init__(self, logger, duration=60):

        self.duration = duration
        self.start = pandas.NaT
        self.end = pandas.NaT
        self.logger = logger

        # DF Counters
        columns = ["frauds", "transactions",
                   "amount", "fraud_amount", "non_fraud_amount",
                   "ratio_fraud_%"]
        self.df = pandas.DataFrame(columns=columns)
        self.df.index.name = "cycle_ref"
        self.reset_counters()

        pandas.set_option("display.float_format", "{:,.2f}".format)

    def reset_counters(self):
        idx = len(self.df)
        self.df.loc[idx, :] = 0
        self.start = time.time()

    def update_counters(self, transaction):
        nb_records = len(self.df)
        idx = nb_records - 1
        self.df.loc[idx, "transactions"] += 1
        transaction_amount = transaction["amount"]
        self.df.loc[idx, "amount"] += transaction_amount

        if transaction["isFraud"] == 1:
            self.df.loc[idx, "frauds"] += 1
            self.df.loc[idx, "fraud_amount"]+= transaction_amount

    def set_counters_per_cycle(self):
        nb_records = len(self.df)
        idx = nb_records - 1
        total_amount_per_cycle = self.df.loc[idx, "amount"]
        fraud_amount_per_cycle = self.df.loc[idx, "fraud_amount"]
        self.df.loc[idx, "non_fraud_amount"] = \
            total_amount_per_cycle - fraud_amount_per_cycle
        if total_amount_per_cycle:
            self.df.loc[idx, "ratio_fraud_%"] = \
                fraud_amount_per_cycle / total_amount_per_cycle * 100
        return f"____________ Cycle {self.df.index[-1]} ___________\n" + \
            self.get_beautified_dict_as_string(self.df.iloc[-1].to_dict())

    def get_summary(self):
        # Print global stats as a DF (omit index)

        # Summary
        summary_title = "_______________ SUMMARY ALL CYCLES _______________"
        self.logger.info(f"{summary_title}\n" +
                         f"{self.df.reset_index().to_string(index=False)}")

        # Totals
        df_totals = self.df.sum().to_frame().T
        if df_totals["amount"].values[0] != 0:
            df_totals["ratio_fraud_%"] = \
                df_totals["fraud_amount"] / df_totals["amount"] * 100
        df_totals.insert(0, "nb_cycles", len(self.df))
        totals_title = "_____________________ TOTALS _____________________"
        self.logger.info(f"{totals_title}\n" +
                         f"{df_totals.to_string(index=False)}")

    def get_beautified_dict_as_string(self, dict_i):
        return "\n".join(f"{k:>40}: {v:>20,.0f}" for k, v in dict_i.items())


def debug_record(record):
    # Print record contents as a dictionary
    record_contents = f"{'\n'.join(f"{k}: {v}" for k, v in record._asdict().items())}"
    return "Transaction is found in received Record's field 'Value':\n" + \
        record_contents


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
