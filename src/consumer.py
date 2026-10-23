# ToDo's:
# + Debug mode with cycle details

# Syntax: python src/consumer.py
# Goal: get stats of bank transactions from Kafka
# - stats are computed during cycles of a fixed duration
#   - frauds, transactions
#   - fraud_amount, non_fraud_amount
#   - ratio fraud_amount / amount
# - totals are computed for all cycles

import time
import os
import lib_common as cmn
import pandas
from kafka import KafkaConsumer
from kafka.serializer import JsonSerializer
from kafka.consumer.fetcher import ConsumerRecord
from logging import Logger

# Set Consumer & Cycle variables
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


def init_process(cycle_duration: int, consumer_record_i: ConsumerRecord,
                 process_records_from: str, window: cmn.RollingWindow) -> tuple[int, Logger]:
    # Create Logger
    process_pid = os.getpid()
    logfile_name = f"{logfile_prefix}_{process_pid}.log"
    logger = cmn.setup_logging(logfile_name)

    # Start cycle counting
    cycle = cmn.Cycle(logger, cycle_duration)
    cycle.start = time.time()
    logger.info("Started to get stats per cycle from " +
                f"{process_records_from} record on.")

    # Initialize window stats
    columns = ["cycle_ref"] + list(cycle.df.columns)
    window.df = pandas.DataFrame(columns=columns)

    # Testing on this script requires to print 1st record contents
    if "test" in topic_name:
        cycle.check_1st_record(consumer_record_i)

    return cycle, logger


def process_1_record(cycle: cmn.Cycle, record_i: ConsumerRecord,
                     window: cmn.RollingWindow) -> cmn.Cycle:
    # Get transaction from Record's field 'Value'
    transaction_i = cmn.Transaction(**record_i.value)

    # Update current cycle's stats
    cycle.set_counters_per_transaction(transaction_i)

    # Print stats when cycle ends
    current_time = time.time()
    cycle.stats.time_elapsed = current_time - cycle.start
    cycle_ended = (cycle.stats.time_elapsed > cycle.duration_wished)
    if cycle_ended:
        # Increment cycle index
        cycle.idx += 1
        cycle.set_counters_per_cycle(window)
        # Get window stats
        #if cycle.idx % window.size == 0:
        # Print a summary of all cycles
        cycle.set_counters_per_window(window)
        #print(f"cycle.idx={cycle.idx}  window.idx={window.idx}")

        cycle.reset_counters(window.size)
    return cycle


def process_records_in_cycles(consumer: KafkaConsumer, cycle_duration: int,
                              process_records_from: str, window: cmn.RollingWindow) -> pandas:
    # Process records received from Broker
    process_started = False
    try:
        # Get stats of each cycle except the last one
        for consumer_record_i in consumer:

            if not process_started:
                cycle, logger = init_process(cycle_duration, consumer_record_i,
                                             process_records_from, window)
                process_started = True

            cycle = process_1_record(cycle, consumer_record_i, window)

    except KeyboardInterrupt:
        # Last-cycle's a global stats are printed after user interruption
        logger.debug("User Interruption")

        # Increment cycle index
        cycle.idx += 1
        window.idx += 1

        # Print stats for last cycle
        cycle.set_counters_per_cycle(window)

        # Print a summary of all cycles
        cycle.set_counters_per_window(window)

        str_windows = f"{window.df.to_string(index=False)}"
        logger.info(f"\n{'=' * 41} Summary of Rolling Windows {'=' * 41}" +
                    f"\n{str_windows}")

        # Return the totals table for Unit Tests
        return window.df


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
