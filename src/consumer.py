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
cycle_duration = 1  # Cycle length in seconds (requirement: 1s)
window_duration = 3  # Rolling window length in seconds (requirement: 30s)
topic_name = "transactions"
group_id = "CG_fraud_detection"
process_records_from = "latest"  # latest or earliest
file_prefix = "consumer"
output_dir = "./data/"


def create_consumer(topic_name: str, process_records_from: str,
                    group_id: int) -> KafkaConsumer:
    """Create a KafkaConsumer subscribed to topic_name; exit on failure."""

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


def init_cycle_and_window(
        cycle_duration: int, consumer_record_i: ConsumerRecord,
        process_records_from: str, window: cmn.RollingWindow
        ) -> tuple[cmn.Cycle, Logger]:
    """Initialize Cycle, Window, logger; return cycle & logger."""

    # Initialize Logger: 1 logfile per consumer
    process_pid = os.getpid()
    logfile_name = f"{file_prefix}_{process_pid}.log"
    logger = cmn.setup_logging(logfile_name)

    # Initialize Output: 1 CSV file per consumer
    window.csv_file_path = \
        f"{output_dir}{file_prefix}_results_{process_pid}.csv"

    # Initialize Cycle
    cycle = cmn.Cycle(logger, cycle_duration)
    cycle.start = time.time()
    logger.info(f"Counters from a Kafka flow on topic '{topic_name}' in " +
                f"rolling windows of {window_duration} cycles that process " +
                f"records from the {process_records_from} one on.")

    # Initialize Window: window's df_history mirrors empty cycle's df_history
    window.df_history = cycle.df_history.copy()

    # Initialize Testing: check 1st record's format & contents
    if "test" in topic_name:
        cycle.check_1st_record(consumer_record_i)

    return cycle, logger


def process_records_in_cycles(consumer: KafkaConsumer, cycle_duration: int,
                              process_records_from: str,
                              window: cmn.RollingWindow) -> pandas:
    """Process records, updating cycle & window counters;
    on KeyboardInterrupt, log summary and return window history."""

    # Flag on existence of cycle & window instances
    cycle_and_window_created = False
    try:
        # Get stats of each window except the last one
        for consumer_record_i in consumer:

            # Creates cycle, window & logger when record reception starts
            if not cycle_and_window_created:
                cycle, logger = init_cycle_and_window(
                    cycle_duration, consumer_record_i,
                    process_records_from, window)
                cycle_and_window_created = True

            process_record(cycle, consumer_record_i, window)

    except KeyboardInterrupt:
        # Get stats of last-window & log windows' summary on user interruption
        logger.debug("User Interruption")

        # Flush last (partial) cycle into window
        aggregate_window_cycles(cycle, window)

        # Print a summary of all windows
        window.log_windows_summary(cycle.logger)

        # Return window totals table for pytest
        return window.df_history


def process_record(cycle: cmn.Cycle, record_i: ConsumerRecord,
                   window: cmn.RollingWindow) -> cmn.Cycle:
    """Parse one record, update cycle counters;
    on cycle end, aggregate into window and reset."""

    # Extract transaction from Record's field 'Value'
    transaction_i = cmn.Transaction(**record_i.value)

    # Update current cycle counters with current transaction
    cycle.update_counters(transaction_i)

    # Cycle ends when its expected duration is exceeded
    current_time = time.time()
    cycle.counters.time_elapsed = current_time - cycle.start
    cycle_ended = (cycle.counters.time_elapsed > cycle.duration)

    if cycle_ended:
        aggregate_window_cycles(cycle, window)

        # Reset cycle counters
        cycle.reset_counters()


def aggregate_window_cycles(cycle: cmn.Cycle, window: cmn.RollingWindow) \
                           -> None:
    """Historize & print cycles & totals of current window"""

    # Historize current cycle counters
    cycle.historize_cycle(window.duration)

    # Get current window counters & historize it
    window.get_and_historize_window(cycle)

    # Log-print window counters
    window.log_window(cycle)


# ___________________________ Main Process ___________________________


def main() -> pandas:
    """Create consumer, process records in cycles, close consumer,
    return window history."""

    window = cmn.RollingWindow(duration=window_duration)

    # Create Consumer
    consumer = create_consumer(topic_name, process_records_from, group_id)

    # Process records received from Broker
    df_totals = process_records_in_cycles(consumer, cycle_duration,
                                          process_records_from, window)

    # Stop all activity linked to the consumer
    consumer.close()

    # Return the totals table for pytest
    return df_totals


# Execute main() only if the script is called directly
# not on import, where __name__ = module's name
script_is_called_directly = (__name__ == "__main__")
if script_is_called_directly:
    main()
