# Syntax: python src/producer.py
# Goal : stream bank transactions to Kafka by a Fire & Forget producer

import time
import pandas
import lib_common as cmn
import logging
from kafka import KafkaProducer

# Set Producer parameters
topic_name = "transactions"
max_sample_size = 300
max_batch_size = 20

actual_sent_records = 0

# Source = Parquet's URL
source_folder = "~/Documents/Dev/git_projects/Kafka_Fraud/data"
mock_file = source_folder + "/transactions.parquet"

# Logging
cmn.setup_logging("producer.log")
APP_LOGGER_NAME = "fraud_detection"
logger = logging.getLogger(APP_LOGGER_NAME)

# Get Kafka Brokers' list
SERVERS = cmn.get_servers_list()

# ___________________ Create Producer ____________________
try:
    producer = KafkaProducer(
        bootstrap_servers=SERVERS,
        value_serializer=cmn.convert_dict_to_bytes
    )
    logger.info('Producer created')
except Exception as e:
    logger.error(f"ERROR: Producer creation failed: {e}")
    exit(1)

# Check connection to Brokers
# "partitions_for()" connects to Broker & creates topic & partition on-the-fly
logger.info(f"Partition's id(s): {producer.partitions_for(topic_name)}")

# Load records from Parquet file as DF
# Transactions are found as strings in field "Value" of records
df_records = pandas.read_parquet(mock_file)

# Loop over "steps"
# A "step" is a cyclic sequence corresponding to a set of transactions
num_steps = max(df_records["step"])
current_step = 0

# while True:
while current_step < 20 :
    # Increment cyclic sequence "step"
    current_step = (current_step + 1) % num_steps

    # Load sample of records corresponding to current step into a DF
    filter_step = (df_records["step"] == current_step)
    df_sample = df_records[filter_step].head(max_sample_size)
    sample_size = df_sample.shape[0]

    # Fragment sample DF into DF batches
    # List start positions of DF batches
    batch_start_positions = range(0, sample_size, max_batch_size)

    # Iterate on DF batches
    for start in batch_start_positions:
        end = start + max_batch_size
        df_batch_i = df_sample.iloc[start:end]
        # Convert each DF batch to a list of Dict records
        option_get_dicts = "records"
        dict_records = df_batch_i.to_dict(option_get_dicts)

        # Iterate on Dict records
        for dict_record in dict_records:
            # ___________ Send record to Kafka _____________
            producer.send(topic_name, dict_record)

    # Print summary of sent records in current step
    logger.info(f"Step {current_step}: Sent {sample_size:,} records ")
    actual_sent_records += sample_size

    # Delay to check the flow of batches in the Consumer
    time.sleep(1)

    # Send remaining records in buffer
    producer.flush()

# Print global summary of sent records
logger.info(f"Total sent records: {actual_sent_records:,}")
