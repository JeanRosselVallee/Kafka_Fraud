# Syntax: python src/admin_broker.py
# Goal: Increment number of partitions

import lib_common as cmn
from kafka.admin import KafkaAdminClient, NewTopic, NewPartitions

topic_name = "transactions"

# Create Logger
logger = cmn.setup_logging("admin_broker.log")

# Get Kafka Brokers' list
SERVERS = cmn.get_servers_list()

# ___________________ Create Broker Admin ___________________
try:
    admin_client = KafkaAdminClient(bootstrap_servers=SERVERS)
    logger.info("Broker Admin created")
except Exception as e:
    logger.error(f"ERROR: Broker Admin creation failed:\n{e}")
    exit(1)

topic_is_nonexistent = (topic_name not in admin_client.list_topics())
if topic_is_nonexistent:
    try:
        # Create topic with 1 partition
        topic = NewTopic(
            name=topic_name,
            num_partitions=1,
            replication_factor=1
        )
        admin_client.create_topics([topic])
        logger.info("Topic created with 1 partition")
    except Exception as e:
        logger.error(f"ERROR: Topic creation failed:\n{e}")
else:
    try:
        # Print number of partitions before update
        nb_before = cmn.get_nb_partitions(admin_client, topic_name)
        logger.info(f"{nb_before} partition(s) before update.")

        # Increment number of partitions
        admin_client.create_partitions({
            topic_name: NewPartitions(total_count=nb_before+1)
        })

        # Print number of partitions after update
        nb_after = cmn.get_nb_partitions(admin_client, topic_name)
        logger.info(f"{nb_after} partition(s) after update.")

    except Exception as e:
        logger.error(f"ERROR: Topic update failed:\n{e}")
