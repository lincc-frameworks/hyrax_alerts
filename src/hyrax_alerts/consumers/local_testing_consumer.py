import numpy as np

from hyrax_alerts.logging_utils import get_logger

from .kafka_consumer import HyraxKafkaConsumer

logger = get_logger(__name__)


class LocalTestingConsumer(HyraxKafkaConsumer):
    """A consumer for local testing.

    This class is a specialized consumer that inherits from `HyraxKafkaConsumer`.
    It is designed to handle the specific requirements of local testing.

    This consumer is generally meant to consume messages from a local kafka topic.
    The messages are sent to the topic from the Hyrax script stream_test/producer.py.
    https://github.com/lincc-frameworks/hyrax/blob/main/stream_test/producer.py
    """

    def __init__(self, config, data_location=None):
        """Initialize the local testing consumer.

        Parameters
        ----------
        config : dict
            The configuration dictionary for the consumer.
        data_location : str, optional
            The location of the data stream. Defaults to None.
        """
        super().__init__(config=config, data_location=data_location)

    def get_image(self, sample):
        """Get the image from a sample."""
        return sample.get("image")

    def get_label(self, sample):
        """Get the label from a sample."""
        return sample.get("label")

    def get_value(self, sample):
        """Get the value from a sample."""
        return sample.get("value")

    def get_timestamp(self, sample):
        """Get the timestamp from a sample."""
        return sample.get("timestamp")

    def get_id(self, sample):
        """Get the ID from a sample."""
        return sample.get("id")

    def get_time_series(self, sample):
        """Get the time series from a sample."""
        return sample.get("time_series")

    def collate_time_series(self, samples):
        """Collate time series data from a list of samples.

        Creates a collated time series array and a corresponding mask indicating valid values.
        Placeholder values for missing data are set to -1, and the time series
        are padded or truncated to a length of 8.

        Parameters
        ----------
        samples : list
            A list of sample dictionaries. Idealy the dictionary should contain a
            'time_series' key with a list of values.

        Returns
        -------
        dict
            A dictionary containing the collated time series data and the corresponding mask.
            Keys are 'time_series' and 'time_series_mask'.
        """

        collated_data = {}

        collated_data["time_series"] = np.array(
            [
                np.pad(
                    sample.get("time_series", []),
                    (0, max(0, 8 - len(sample.get("time_series", [])))),
                    mode="constant",
                    constant_values=-1,
                )[:8]
                for sample in samples
            ]
        )

        collated_data["time_series_mask"] = np.array(
            [[value != -1 for value in series] for series in collated_data["time_series"]]
        )

        return collated_data
