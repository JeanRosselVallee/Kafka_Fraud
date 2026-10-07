# Goal : stream bank transactions to Kafka
# Steps :
# 1. Get df_sample of 300 transactions
#    - source = Parquet's 'URL
#    - filter by step (between 1 & num_steps)
# 2. Send to topic "transactions" 
#    - batches of 20 
#    - wait 1 second after each batch
# 4. Repeat until all transactions are sent, then start over from the beginning

import time
import json
import pandas as pd
import math
from kafka import KafkaProducer

# Kafka Bokers
SERVERS = [
    "108.131.108.60:9092",
    "3.249.43.106:9092",
    "63.35.224.191:9092"
]

source_folder = "~/Documents/Dev/git_projects/Kafka_Fraud/data"
mock_file = source_folder + "/transactions.parquet"

max_sample_size = 300
max_batch_size = 20

# Producer Fire & Forget
producer = KafkaProducer(
    bootstrap_servers=SERVERS,
    value_serializer=lambda x: json.dumps(x).encode()  # converts dict to bytes
)
print("Producer connected to Kafka brokers", producer.bootstrap_connected())

df_transactions = pd.read_parquet(mock_file)
num_steps = max(df_transactions["step"])
current_step = 1

while True:
    print("Current step", current_step)

    filter_step = (df_transactions["step"] == current_step)
    df_sample = df_transactions[filter_step].head(max_sample_size)
    sample_size = df_sample.shape[0]
    n_batches = math.ceil(sample_size / max_batch_size)

    sample_index_of_record_1_in_batch = range(0, sample_size, max_batch_size)
    for batch_start_i in sample_index_of_record_1_in_batch:
        batch_end_i = batch_start_i + max_batch_size
        df_batch = df_sample.iloc[batch_start_i:batch_end_i]
        for _, transaction_j in df_batch.iterrows():
            producer.send(
                "transactions",  # Topic
                transaction_j.to_json(),  # Message
            )
        time.sleep(1 / n_batches)

    current_step = (current_step + 1) % num_steps
