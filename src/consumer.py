# Syntax: python src/consumer.py
# Goal: get stats of bank transactions from Kafka
# - stats are computed during cycles of a fixed duration
#   - frauds, transactions
#   - fraud_amount, non_fraud_amount
#   - ratio fraud_amount / amount
# - totals are computed for all cycles

import time
import lib_common as cmn
from kafka import KafkaConsumer
from kafka.serializer import JsonSerializer

# Set Consumer & Cycle parameters
cycle_duration = 2 #60  # seconds
consumer_topic = "transactions"
# process_records_from = "earliest"
process_records_from = "latest"

# Create Logger
logger = cmn.setup_logging()

# Get Kafka Brokers' list
# newline chars are removed from non-empty lines
with open("./config/broker_servers.txt") as f:
    SERVERS = [line.strip() for line in f if line.strip()]

# ___________________ Create Consumer ____________________
try:
    consumer = KafkaConsumer(
        consumer_topic,
        bootstrap_servers=SERVERS,
        value_deserializer=JsonSerializer(),
        auto_offset_reset=process_records_from
    )
    logger.info('Consumer created')
except Exception as e:
    logger.error(f"ERROR: Consumer creation failed:\n{e}")
    exit(1)

# ____________________ Generate Stats ____________________

# Instantiate cycle counters
cycle = cmn.CycleCounters(logger, cycle_duration)
# Flag to initialize cycle counters
stats_started = False

try:
    for consumer_record_i in consumer:

        if not stats_started:
            # Print 1st record contents as a dictionary
            logger.debug(cmn.debug_record(consumer_record_i))
            logger.info("Get stats per cycle (from " +
                        f"{process_records_from} record on)")
            stats_started = True

        # Get transaction from Record's field 'Value'
        transaction_i = consumer_record_i.value

        # Update stats for current cycle
        cycle.update_counters(transaction_i)

        # Print stats when cycle ends
        current_time = time.time()
        cycle_ended = (current_time - cycle.start > cycle.duration)
        if cycle_ended:
            logger.info(cycle.set_counters_per_cycle())
            cycle.reset_counters()

except KeyboardInterrupt:
    # Last-cycle's a global stats are printed after user interruption
    logger.debug("User Interruption")

    # Print stats for last cycle
    logger.info(cycle.set_counters_per_cycle())

    # Print a summary of all cycles
    cycle.get_summary()

    exit(0)
