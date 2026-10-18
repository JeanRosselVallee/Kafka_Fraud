# Syntax: python src/admin_broker.py
# Goal: Increment number of partitions

import lib_common as cmn
from kafka.admin import KafkaAdminClient, NewTopic, NewPartitions

topic_name = "transactions"

# Logging
logger = cmn.setup_logging("admin_broker.log")


def create_broker_admin(logger) -> KafkaAdminClient:

    # Get Kafka Brokers' list
    SERVERS = cmn.get_servers_list()

    try:
        admin = KafkaAdminClient(bootstrap_servers=SERVERS)
        logger.info("Broker Admin created")
        return admin
    except Exception as e:
        logger.error(f"ERROR: Broker Admin creation failed:\n{e}")
        exit(1)


def create_topic_with_partition(logger, admin, topic_name) -> int:
    try:
        topic = NewTopic(
            name=topic_name,
            num_partitions=1,
            replication_factor=1
        )
        admin.create_topics([topic])
        logger.info("Topic created")
        nb_partitions = cmn.get_nb_partitions(admin, topic_name)
        return nb_partitions
    except Exception as e:
        logger.error(f"ERROR: Topic creation failed:\n{e}")


def add_1_partition_to_topic(logger, admin, topic_name, nb_before) -> int:
    try:
        # Increment number of partitions
        admin.create_partitions({
            topic_name: NewPartitions(total_count=nb_before+1)
        })
        nb_partitions = cmn.get_nb_partitions(admin, topic_name)
        return nb_partitions
    except Exception as e:
        logger.error(f"ERROR: Topic update failed:\n{e}")

# ___________________________ Main Process ___________________________


def main():
    # Create Admin to manage topics & its partitions
    admin = create_broker_admin(logger)

    # Check number of partitions before increment
    nb_before = cmn.get_nb_partitions(admin, topic_name)
    logger.info(f"{nb_before} partition(s) before update.")

    # Increment number of partitions
    topic_exists = (topic_name in admin.list_topics())
    if topic_exists:
        nb_after = add_1_partition_to_topic(logger, admin, topic_name, nb_before)
    else:
        nb_after = create_topic_with_partition(logger, admin, topic_name)

    # Check number of partitions was incremented
    logger.info(f"{nb_after} partition(s) after update.")


# Execute main() only if the script is called directly
# not on import, where __name__ = module's name
script_is_called_directly = (__name__ == "__main__")
if script_is_called_directly:
    main()
