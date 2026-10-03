import time
import json
import pandas as pd

from kafka import KafkaProducer

SERVERS = [
    # TODO : Mettre les adresses des brokers Kafka ici (avec le port)
]

producer = KafkaProducer(
    bootstrap_servers=SERVERS,
    value_serializer=lambda x: json.dumps(x).encode()  # On encode en JSON puis en byte array
)

data = pd.read_parquet(
    "https://blent-learning-user-ressources.s3.eu-west-3.amazonaws.com/projects/a06e2b/transactions.parquet"
)
num_data_steps = max(data["step"])
current_step = 1

while True:
    print("Current step", current_step)

    sample = data[data["step"] == current_step]
    if sample.shape[0] > 300:  # On ne prend qu'au maximum 300 transactions
        sample = sample.sample(n=300)

    n_batches = round(sample.shape[0] / 20)
    if n_batches == 0:
        n_batches = 1
    rows_per_batch = int(sample.shape[0] / n_batches)
    for i in range(n_batches):
        for j in range(rows_per_batch):
            producer.send("transactions", sample.iloc[i * rows_per_batch + j].to_json())
        time.sleep(1 / n_batches)

    current_step = (current_step + 1) % num_data_steps