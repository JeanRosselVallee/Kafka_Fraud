# Syntax: python src/producer.py
# Goal : stream bank transactions to Kafka by a Fire & Forget producer

import pandas
import lib_common as cmn
import logging
from kafka import KafkaProducer

# Set Producer parameters
topic_name = "transactions"
max_sample_size = 300
max_batch_size = 20

# Source = Parquet's URL
source_folder = "~/Documents/Dev/git_projects/Kafka_Fraud/data"
mock_file = source_folder + "/transactions.parquet"

# Logging
cmn.setup_logging("producer.log")
APP_LOGGER_NAME = "fraud_detection"
logger = logging.getLogger(APP_LOGGER_NAME)


def create_producer(logger):
    # Get Kafka Brokers' list
    SERVERS = cmn.get_servers_list()

    try:
        producer = KafkaProducer(
            bootstrap_servers=SERVERS,
            value_serializer=cmn.convert_dict_to_bytes
        )
        logger.info('Producer created')
        return producer
    except Exception as e:
        logger.error(f"ERROR: Producer creation failed: {e}")
        exit(1)

    # Check connection to Brokers
    # partitions_for() connects to Broker & creates topic & partition
    logger.info(f"Partition's id(s): {producer.partitions_for(topic_name)}")


def send_records_to_broker(producer, topic_name, df_records,
                           max_sample_size, max_batch_size, logger):
    # Loop over "steps"
    # A "step" is a cyclic sequence corresponding to a set of transactions
    num_steps = max(df_records["step"])
    current_step = 0
    total_sent_records = 0
    try:
        # while True:
        while current_step < 20   :
            # Increment cyclic sequence "step"
            current_step = (current_step + 1) % num_steps

            # Select a DF sample of records corresponding to current step
            filter_step = (df_records["step"] == current_step)
            df_sample = df_records[filter_step].head(max_sample_size)
            sample_size = df_sample.shape[0]

            # Fragment DF sample into batches
            # Get a list of start positions of DF batches
            batch_start_positions = range(0, sample_size, max_batch_size)

            # Process each batch as a list of Dict records
            for start in batch_start_positions:
                end = start + max_batch_size
                df_batch_i = df_sample.iloc[start:end]
                # Convert each DF batch to a list of Dict records
                option_get_dicts = "records"
                dict_records = df_batch_i.to_dict(option_get_dicts)

                # Send Dict records 1-by-1 
                for dict_record in dict_records:
                    # ___________ Send record to Kafka _____________
                    producer.send(topic_name, dict_record)

            # Print summary of sent records in current step
            logger.info(f"Step {current_step:>4}: Sent {sample_size:,} records ")
            total_sent_records += sample_size

            # Send remaining records in buffer
            producer.flush()

        # Print global summary of sent records
        logger.info(f"Total sent records: {total_sent_records:,}")

    except KeyboardInterrupt:
        # Print total sent records after user interruption
        logger.debug("User Interruption")
        logger.info(f"Total sent records: {total_sent_records:,}")
        # Send remaining records in buffer
        producer.flush()

        exit(0)


# ___________________________ Main Process ___________________________


def main():
    # Create Producer
    producer = create_producer(logger)

    # Load records from Parquet file as DF
    # Transactions are found as string at a times in field "Value" of records
    df_records = pandas.read_parquet(mock_file)

    # Send records
    # Get a sample, process it by batches, send records 1-by-1
    send_records_to_broker(producer, topic_name, df_records,
                           max_sample_size, max_batch_size, logger)


# Execute main() only if the script is called directly
# not on import, where __name__ = module's name
script_is_called_directly = (__name__ == "__main__")
if script_is_called_directly:
    main()
