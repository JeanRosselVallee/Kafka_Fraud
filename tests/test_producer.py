# Syntax: pytest ./tests/test_producer.py -v -s

# This test requires the user to KB interrupt the producer
# after the 1st message is sent

import pandas as pd
import json
from kafka import KafkaConsumer
import producer
import lib_common as cmn


def test_producer_with_testing_consumer(monkeypatch) -> None:
    # monkeypatch is pytest's fixture that replaces attributes & functions

    # Replace global cmn.topic_name
    topic_name = "test_producer"
    monkeypatch.setattr(producer, "topic_name", topic_name)
    monkeypatch.setattr(producer, "logfile_name", "test_producer.log")

    # Replace pandas.read_parquet() call by producer.py
    # Replace by a DF with a single record
    record_produced = {"step": 1, "amount": 100.0, "isFraud": 0}
    df = pd.DataFrame([record_produced])
    monkeypatch.setattr(producer.pandas, "read_parquet", lambda _: df)

    # Test that Producer's main() sends record_produced
    producer.main()

    # Create Testing consumer
    testing_consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=cmn.get_servers_list(),
        auto_offset_reset="earliest",
        consumer_timeout_ms=5000,
        group_id="CG_producer_test",
    )

    # Consumer receives a single record
    record_consumed = next(iter(testing_consumer), None)
    testing_consumer.close()

    # Get transactions out of Byte records
    tx_produced = json.dumps(record_produced).encode('utf-8')
    tx_consumed = record_consumed.value if record_consumed else None

    # Assert consumed transaction == produced transaction
    result = f"\ntransaction sent by producer=\t\t{tx_produced}" + \
             f"\ntransaction received by consumer:\t{tx_consumed}"
    print(f"Expected result: received & sent transactions must match:{result}")
    failure_message = f"Received & sent transactions did not match:{result}"
    assert tx_consumed == tx_produced, failure_message
