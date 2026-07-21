from .kafka_consumer import HyraxKafkaConsumer


class BabamulConsumer(HyraxKafkaConsumer):
    """A consumer for the Babamul data stream.

    This class is a specialized consumer that inherits from `HyraxKafkaConsumer`.
    It is designed to handle the specific requirements of the Babamul data stream.
    """

    def __init__(self, config, data_location=None):
        """Initialize the Babamul consumer.

        Parameters
        ----------
        config : dict
            The configuration dictionary for the consumer.
        data_location : str, optional
            The location of the data stream. Defaults to None.
        """
        super().__init__(config=config, data_location=data_location)
        # whether or not convert the alert to a babamul alert object
        self.raw_alert = config["hyrax_alerts"]["consumer"]["BabamulConsumer"]["raw_alert"]

    def _decode(self, msg):
        """Decode the incoming message from the Babamul data stream.

        This method deserializes the message and returns it in a usable format.

        Parameters
        ----------
        msg : bytes
            The incoming message to be decoded.

        Returns
        -------
        dict
            The decoded message as a dictionary.
        """
        # try:
        from babamul.avro import deserialize_alert

        # except ImportError as err:
        #     raise ImportError("babamul package is not installed") from err
        alert = deserialize_alert(msg.value())
        return alert
