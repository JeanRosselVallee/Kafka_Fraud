# Syntax: python src/consumer.py
# Goal : ingest bank transactions from Kafka

import time
import json
import lib_common as cmn
import logging
from kafka import KafkaConsumer
from kafka.serializer import JsonSerializer

# Logging
cmn.setup_logging()
APP_LOGGER_NAME = "fraud_detection"
logger = logging.getLogger(APP_LOGGER_NAME)

# Get Kafka Brokers' list 
with open("./config/broker_servers.txt") as f:
    # Remove newline chars from  non-empty lines
    SERVERS = [line.strip() for line in f if line.strip()]

# Consumer creation
byte_to_dict = lambda byte_i: json.loads(byte_i.decode("ascii"))
topic = "transactions"
# process_from = "earliest"
process_from = "latest"

try:
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=SERVERS,
        value_deserializer=JsonSerializer(),  # byte_to_dict,
        auto_offset_reset=process_from
    )
    logger.info('Consumer created')
except Exception as e:
    logger.error(f"Consumer creation failed: {e}")

# Check connection to Brokers
logger.info(f"Available partitions ids: " +
            f"{consumer.partitions_for_topic(topic)}")

# Load transactions as dictionaries

stats_cycle = 2 #60  # seconds

# Flag to print 1st record
print_record_1 = True

# Counters
cycle_count = 0
# - stats-per-cycle
cycle_tx_count = 0
cycle_fraud_count = 0
# - cumulative stats
aggreg_tx_count = 0
aggreg_fraud_count = 0
aggreg_total_amount = 0
aggreg_fraud_amount = 0

# stats_start = time.time()
# logger.debug(f"Consumer started receiving messages")
try:
    for consumer_record_i in consumer:
        transaction_i = consumer_record_i.value

        # Print 1st record contents
        if print_record_1:
            logger.debug(f"1st Record:\n{consumer_record_i}")
            logger.debug(f"1st Transaction:\n{transaction_i}")
            print_record_1 = False
            logger.info(f"{10*'='} Consumer Stats per cycle " +
                        f"(from {process_from}) {10*'='}")
            stats_start = time.time()
            logger.debug("Consumer received 1st message")
        
        # Print stats every cycle
        current_time = time.time()
        if current_time - stats_start > stats_cycle:
            logger.info(
                f"Cycle #{cycle_count:>4}: {cycle_fraud_count:>6,} frauds, " +
                f"{cycle_tx_count:>20,} transactions received"
            )
            cycle_count += 1

            stats_start = current_time
            cycle_tx_count = 0
            cycle_fraud_count = 0

        # Stats per cycle
        cycle_tx_count += 1
        aggreg_tx_count += 1
        current_tx_amount = transaction_i['amount']
        aggreg_total_amount += current_tx_amount

        if transaction_i['isFraud'] == 1:
            cycle_fraud_count += 1
            aggreg_fraud_count += 1
            aggreg_fraud_amount += current_tx_amount

except KeyboardInterrupt:
    logger.info(
        f"Cyc.#{cycle_count:>4}: {cycle_fraud_count:>6,} frauds, " +
        f"{cycle_tx_count:>20,} transactions received"
    )
    aggreg_non_fraud_amount = aggreg_total_amount - aggreg_fraud_amount
    if aggreg_total_amount != 0:
        ratio_aggreg_fraud = aggreg_fraud_amount / aggreg_total_amount
    else:
        ratio_aggreg_fraud = 0

    logger.info("====================== TOTALS =======================")
    logger.info(f"Nb of Frauds \t= {aggreg_fraud_count:>20}")
    logger.info(f"Nb Transactions \t= {aggreg_tx_count:>20}")
    logger.info(f"Fraud Amount \t= {aggreg_fraud_amount:23.2f} $")
    logger.info(f"Non-Fraud Amount \t= {aggreg_non_fraud_amount:23.2f} $")
    logger.info(f"Amount Ratio \t= {ratio_aggreg_fraud:24.2%}")

