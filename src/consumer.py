# Syntax: python src/consumer.py

# Goal: get stats out of a Kafka flow of bank transactions' data
# 1. aggregation per cycle (1s):
#   - counts of all transactions & frauds
#   - amounts of all transactions, fraud
# 2. aggregation per rolling window (30 cycles)
#   - same as above, plus,
#   - amounts of non_frauds
#   - ratio of amounts of frauds vs. total
# 3. A summary with totals of all windows is displayed after user interruption.

import time
import os
import lib_common as cmn
import pandas
from kafka import KafkaConsumer
from kafka.serializer import JsonSerializer
from kafka.consumer.fetcher import ConsumerRecord
from logging import Logger

# Configuration
cycle_duration = 1  # in seconds. Requirement: cycle_duration = 1s
window_size = 3  # in seconds. Requirement: window_size = 30s
topic_name = "transactions"
group_id = "CG_fraud_detection"
process_records_from = "latest"  # "earliest"
logfile_prefix = "consumer"


def create_consumer(topic_name: str, process_records_from: str,
                    group_id: int) -> KafkaConsumer:
    # Get Kafka Brokers' list
    SERVERS = cmn.get_servers_list()

    try:
        consumer = KafkaConsumer(
            topic_name,
            bootstrap_servers=SERVERS,
            value_deserializer=JsonSerializer(),
            auto_offset_reset=process_records_from,
            group_id=group_id
        )
        print('INFO: Consumer created')
        return consumer

    except Exception as e:
        print(f"ERROR: Consumer creation failed:\n{e}")
        exit(1)


def init_consumer(
        cycle_duration: int, consumer_record_i: ConsumerRecord,
        process_records_from: str, window: cmn.RollingWindow
        ) -> tuple[int, Logger]:

    # Initialize Logger
    process_pid = os.getpid()
    logfile_name = f"{logfile_prefix}_{process_pid}.log"
    logger = cmn.setup_logging(logfile_name)

    # Initialize Cycle
    cycle = cmn.Cycle(logger, cycle_duration)
    cycle.start = time.time()
    logger.info(f"Counters from a Kafka flow on topic '{topic_name}' in " +
                f"rolling windows of {window_size} cycles that process " +
                f"records from the {process_records_from} one on.")

    # Initialize Window
    window.df_history = cycle.df_history.copy()

    # Initialize Testing: print 1st record
    if "test" in topic_name:
        cycle.check_1st_record(consumer_record_i)

    return cycle, logger


def process_records_in_cycles(consumer: KafkaConsumer, cycle_duration: int,
                              process_records_from: str,
                              window: cmn.RollingWindow) -> pandas:
    # Process records received from Broker
    process_started = False
    try:
        # Get stats of each window except the last one
        for consumer_record_i in consumer:

            if not process_started:
                cycle, logger = init_consumer(
                    cycle_duration, consumer_record_i,
                    process_records_from, window)
                process_started = True

            update_counters(cycle, consumer_record_i, window)

    except KeyboardInterrupt:
        # Last-window & windows' summary are printed after user interruption
        logger.debug("User Interruption")

        aggregate_window_cycles(cycle, window)

        # Print a summary of all windows
        print_windows_summary(window, logger)

        # Return the totals table for Unit Tests
        return window.df_history


def update_counters(cycle: cmn.Cycle, record_i: ConsumerRecord,
                    window: cmn.RollingWindow) -> cmn.Cycle:

    # Extract transaction from Record's field 'Value'
    transaction_i = cmn.Transaction(**record_i.value)

    # Update current cycle counters with current transaction
    cycle.append_transaction_to_cycle(transaction_i)

    # Detect when cycle ends
    current_time = time.time()
    cycle.counters.time_elapsed = current_time - cycle.start
    cycle_ended = (cycle.counters.time_elapsed > cycle.duration_wished)

    if cycle_ended:
        aggregate_window_cycles(cycle, window)

        # Reset cycle counters
        cycle.reset_counters()


def aggregate_window_cycles(cycle: cmn.Cycle, window: cmn.RollingWindow) \
                           -> None:
    """Historize & print cycles & totals of current window"""

    # Historize current cycle counters
    cycle.store_cycle(window.size)

    # Get current window counters & historize it
    cycle.get_and_historize_window(window)

    # Log-print window counters
    cycle.print_window(window)


def print_windows_summary(window: cmn.RollingWindow, logger: Logger) -> None:

    # Print DF history of windows
    str_windows = f"{window.df_history.to_string(index=False)}"
    logger.info(f"\n{'=' * 46} Summary of Rolling Windows {'=' * 41}" +
                f"\n{str_windows}")

    # Print Totals out of all windows
    df_totals = window.df_history.sum().to_frame().T
    df_totals.loc[df_totals.index[-1], "cycle_ref"] = None
    df_totals["fraud_ratio_pct"] = \
        df_totals["fraud_amount"] / df_totals["total_amount"] * 100
    str_totals = f"{df_totals.to_string(index=False)}"
    logger.info(f"\n{'=' * 46} Totals of Rolling Windows {'=' * 41}" +
                f"\n{str_totals}")


# ___________________________ Main Process ___________________________


def main() -> pandas:

    window = cmn.RollingWindow(size=window_size)

    # Create Consumer
    consumer = create_consumer(topic_name, process_records_from, group_id)

    # Process records received from Broker
    df_totals = process_records_in_cycles(consumer, cycle_duration,
                                          process_records_from, window)

    # Stop all activity linked to the consumer
    consumer.close()

    # Return the totals table for Unit Tests
    return df_totals


# Execute main() only if the script is called directly
# not on import, where __name__ = module's name
script_is_called_directly = (__name__ == "__main__")
if script_is_called_directly:
    main()
