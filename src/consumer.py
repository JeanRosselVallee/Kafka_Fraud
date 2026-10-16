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
from kafka import KafkaConsumer
from kafka.serializer import JsonSerializer

# Set Consumer & Cycle variables
cycle_duration = 2 #60  # seconds
topic_name = "transactions"
group_id = "CG_fraud_detection"
# process_records_from = "earliest"
process_records_from = "latest"
cycle_counting_started = False
process_pid = os.getpid()

# Get Kafka Brokers' list
SERVERS = cmn.get_servers_list()

# ___________________ Create Consumer ____________________
try:
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=SERVERS,
        value_deserializer=JsonSerializer(),
        auto_offset_reset=process_records_from,
        group_id=group_id
    )
    print('INFO: Consumer created')
except Exception as e:
    print(f"ERROR: Consumer creation failed:\n{e}")
    exit(1)

# ________________ Consume records & generate stats ________________

try:
    # Get stats of each cycle except the last one
    for consumer_record_i in consumer:

        if not cycle_counting_started:

            # Create Logger
            logfile_name = f"consumer_{process_pid}.log"
            logger = cmn.setup_logging(logfile_name)

            # Start cycle counting
            cycle = cmn.Cycle(logger, cycle_duration)
            cycle.start = time.time()
            cycle_counting_started = True

            # Print 1st record contents as a dictionary
            cycle.check_1st_record(consumer_record_i)

            logger.info("Get stats per cycle from " +
                        f"{process_records_from} record on.")

        # Get transaction from Record's field 'Value'
        transaction_i = cmn.Transaction(**consumer_record_i.value)

        # Update current cycle's stats
        cycle.set_counters_per_transaction(transaction_i)

        # Print stats when cycle ends
        current_time = time.time()
        cycle_ended = (current_time - cycle.start > cycle.duration)
        if cycle_ended:
            cycle.get_counters_per_cycle()
            cycle.reset_counters()

except KeyboardInterrupt:
    # Last-cycle's a global stats are printed after user interruption
    logger.debug("User Interruption")

    # Print stats for last cycle
    cycle.get_counters_per_cycle()

    # Print a summary of all cycles
    cycle.get_summary_of_all_cycles()

    exit(0)
