# Syntax: python src/producer.py
# Goal : stream bank transactions to Kafka

import time
import pandas as pd
import math
import json
import lib_common as cmn
import logging
from kafka import KafkaProducer

# Logging
cmn.setup_logging()
APP_LOGGER_NAME = "fraud_detection"
logger = logging.getLogger(APP_LOGGER_NAME)

# Get Kafka Brokers' list 
with open("./config/broker_servers.txt") as f:
    # Remove newline chars from  non-empty lines
    SERVERS = [line.strip() for line in f if line.strip()]

# Source = Parquet's URL
source_folder = "~/Documents/Dev/git_projects/Kafka_Fraud/data"
mock_file = source_folder + "/transactions.parquet"

max_sample_size = 300
max_batch_size = 20

# Producer (Fire & Forget)
dict_to_string_to_bytes = lambda dict_i: json.dumps(dict_i).encode()

try:
    producer = KafkaProducer(
        bootstrap_servers=SERVERS,
        value_serializer=dict_to_string_to_bytes
    )
    logger.info('Producer created')
except Exception as e:
    logger.error(f"Producer creation failed: {e}")
    exit(1)

# Check connection to Brokers
topic = "transactions"
logger.info(f"Created partition's id(s): {producer.partitions_for(topic)}")

# Load records from Parquet file as DF (transactions are in a String field)
df_records = pd.read_parquet(mock_file)
num_steps = max(df_records["step"])
current_step = 0

# Loop over steps
# while True:
while current_step < 5:
    # Increase "step" in a cycle (1..num_steps)
    current_step = (current_step + 1) % num_steps

    # Get a sample for current step
    filter_step = (df_records["step"] == current_step)
    df_sample = df_records[filter_step].head(max_sample_size)
    sample_size = df_sample.shape[0]

    # List start positions of batches
    batch_start_positions = range(0, sample_size, max_batch_size)

    # Iterate batches
    for batch_start_i in batch_start_positions:
        batch_end_i = batch_start_i + max_batch_size
        df_batch_i = df_sample.iloc[batch_start_i:batch_end_i]

        option_to_get_a_list = "records"
        records_as_dicts = df_batch_i.to_dict(option_to_get_a_list)
        for record_as_dict in records_as_dicts:
            # Send record to Kafka  
            producer.send(topic, record_as_dict)

    # Delay to check the reception of records on the Consumer
    time.sleep(1)

    logger.info(f"Sent {sample_size:,} records for step {current_step}")

    # Send remaining records in buffer
    producer.flush()
