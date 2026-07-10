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
