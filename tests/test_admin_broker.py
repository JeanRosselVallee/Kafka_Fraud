# Syntax: pytest tests/test_admin_broker.py

# Run twice to test incrementing partitions
# - from 1 to 2 at topic creation
# - from 2 to 3 at topic update

import admin_broker


def test_increment_nb_of_partitions_by_admin(monkeypatch) -> None:
    # monkeypatch is pytest's fixture that replaces attributes & functions

    # Replace global variables in consumer.py
    monkeypatch.setattr(admin_broker, "topic_name", "test_admin_broker")
    monkeypatch.setattr(admin_broker, "logfile_name", "test_admin_broker.log")

    # Test that admin_broker's main() increments the number of partitions
    nb_before, nb_after = admin_broker.main()

    # Assert the increment
    print(f"nb_before={nb_before} should increment to nb_after={nb_after}")
    failure_message = f"nb_after={nb_after} while nb_before={nb_before}"
    assert nb_after == nb_before + 1, failure_message
