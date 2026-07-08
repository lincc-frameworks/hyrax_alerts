from hyrax.datasets.kafka_stream_dataset import KafkaStreamDataset

class BaseConsumer(KafkaStreamDataset):
    """Base consumer class for consuming messages from a Kafka topic."""
    def __init__(self, config, data_location):
        super().__init__(config, data_location)

    def pre_filter(self, batch):
        """Pre-filter the batch of messages before processing."""
        # Implement any pre-filtering logic here
        return batch

    def pre_process(self, batch):
        """Pre-process the batch of messages before processing."""
        # Implement any pre-processing logic here
        return batch