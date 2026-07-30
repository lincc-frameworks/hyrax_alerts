from hyrax.datasets import KafkaStreamDataset

from .base_consumer import HyraxAlertsBaseConsumer


class HyraxKafkaConsumer(HyraxAlertsBaseConsumer, KafkaStreamDataset):
    """Base class for Hyrax Kafka consumers.

    Works out of the box with any basic Kafka stream configuration. To setup,
    simply provide the necessary configuration parameters in the config file.

    The required config fields in the data_request of the hyrax config are:
    - "dataset_class": "hyrax_alerts.consumers.kafka_consumer.HyraxKafkaConsumer"
    - "data_location": "kafka://{your kafka server}/{your kafka topic}"
    + any other hyrax data_request fields.
    """

    def __init__(self, config, data_location=None):
        """The __init__ for `KafkaStreamDataset` and `HyraxAlertsBaseConsumer`
        handle all of the functionality for this class. This class is primarily a convenience class that
        allows for the use of Kafka streams with HyraxAlerts without needing to implement a custom consumer
        class.
        """
        HyraxAlertsBaseConsumer.__init__(self, config=config, data_location=data_location)
        KafkaStreamDataset.__init__(self, config=config, data_location=data_location)

    def peek_sample(self):
        """Overwrite of the `KafkaStreamDataset` function to maintain pre_filter and pre_process
        consistency when setting up the model for streaming inference.
        """
        while not self._stop.is_set():
            sample = super().peek_sample()
            sample = self.pre_filter([sample])
            if sample:
                sample = sample[0] if isinstance(sample, list) else sample
                sample = self.pre_process([sample])
                return sample[0] if isinstance(sample, list) else sample
            else:
                # if the sample didn't pass our filtering, then reset the buffer
                self._buffered = []
