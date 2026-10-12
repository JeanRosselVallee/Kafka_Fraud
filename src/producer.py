# Syntax: python src/producer.py
# Goal : stream bank transactions to Kafka

import time
import pandas as pd
import math
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
series_to_bytes = lambda series_i: series_i.to_json().encode()
try:
    producer = KafkaProducer(
        bootstrap_servers=SERVERS,
        value_serializer=series_to_bytes
    )
    logger.info('Producer created')
except Exception as e:
    logger.error(f"Producer creation failed: {e}")
    exit(1)

# Check connection to Brokers
topic = "transactions"
logger.info(f"Created partition's id(s): {producer.partitions_for(topic)}")

# Load transactions as DF
df_transactions = pd.read_parquet(mock_file)
num_steps = max(df_transactions["step"])
current_step = 0

# Loop over steps
# while True:
while current_step < 2:
    # Increase "step" in a cycle (1..num_steps)
    current_step = (current_step + 1) % num_steps

    # Get a sample for current step
    filter_step = (df_transactions["step"] == current_step)
    df_sample = df_transactions[filter_step].head(max_sample_size)
    sample_size = df_sample.shape[0]
    logger.info(f"Sent {sample_size:,} transactions for step {current_step}")

    # Get number of batches in the sample
    n_batches = math.ceil(sample_size / max_batch_size)

    # List start positions of batches
    batch_start_positions = range(0, sample_size, max_batch_size)

    # Iterate batches
    for batch_start_i in batch_start_positions:
        batch_end_i = batch_start_i + max_batch_size
        df_batch_i = df_sample.iloc[batch_start_i:batch_end_i]
    
        # Send each transaction in current batch
        for _, series_transaction in df_batch_i.iterrows():
            producer.send(topic, series_transaction)
        time.sleep(1 / n_batches)

    # Send remaining records in buffer
    producer.flush()
