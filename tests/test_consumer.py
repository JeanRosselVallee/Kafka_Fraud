# Syntax: pytest ./tests/test_consumer.py -v -s

# This test requires the user to KB interrupt the consumer
# after the 1st message is consumed

from kafka import KafkaProducer
from kafka.serializer import JsonSerializer
import consumer
import lib_common as cmn

str_segment = "=" * 15
transaction_amount = 9839.64


def test_consumer_with_basic_producer(monkeypatch) -> None:
    # monkeypatch is pytest's fixture that replaces attributes & functions

    # Replace global variables in consumer.py
    topic_name = "test_consumer"
    monkeypatch.setattr(consumer, "topic_name", topic_name)
    monkeypatch.setattr(consumer, "process_records_from", "earliest")
    monkeypatch.setattr(consumer, "group_id", "CG_test_consumer")
    monkeypatch.setattr(consumer, "logfile_prefix", "test_consumer")

    # Single Transaction record to be sent by the producer
    record = {
        'step': 1,
        'type': 'PAYMENT',
        'amount': transaction_amount,
        'isFraud': 0,
        'nameOrig': 'C1231006815',
        'oldbalanceOrg': 170136.0,
        'newbalanceOrig': 160296.36,
        'nameDest': 'M1979787155',
        'oldbalanceDest': 0.0,
        'newbalanceDest': 0.0
    }

    # Create Producer
    print(f"INFO: {str_segment} Creating Producer {str_segment}")
    basic_producer = KafkaProducer(
        bootstrap_servers=cmn.get_servers_list(),
        value_serializer=JsonSerializer()
    )

    # Send 1 record directly
    print(f"INFO: {str_segment} sending 1 record {str_segment}")
    basic_producer.send(topic_name, record)
    basic_producer.flush()
    basic_producer.close()

    # Test that Consumer's main() reads & updates cycle counters
    print(f"INFO: {str_segment} Starting consumer {str_segment}")
    df_totals = consumer.main()

    # Assert the cycle counted exactly one transaction
    print(f"INFO: {str_segment} Asserting on total counters {str_segment}")
    print("Expected results: nb_cycles=1 & tx_count=1 & " +
          f"total_amount={transaction_amount}")

    assert df_totals['nb_cycles'].values[0] == 1, "Wrong nb_cycles"
    assert df_totals['tx_count'].values[0] == 1, "Wrong tx_count"
    assert df_totals['total_amount'].values[0] == transaction_amount, \
           "Wrong total_amount"
