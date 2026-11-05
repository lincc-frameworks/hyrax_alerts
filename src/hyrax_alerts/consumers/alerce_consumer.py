import io
from typing import Any, cast

import fastavro

from .kafka_consumer import HyraxKafkaConsumer


class AlerceConsumer(HyraxKafkaConsumer):
    """A consumer for the ALeRCE broker's Kafka output streams.

    ALeRCE publishes Avro object container files, so the writer schema travels inside the
    message value and decoding needs nothing beyond `fastavro.reader`.

    Two ALeRCE conventions are worth knowing, and both are handled in configuration rather
    than here:

    - Topics are suffixed with a date and rotate daily (``lc_classifier_20260820``), with
      a 48 hour retention. Rather than resubscribing each night, give
      ``[data_set.KafkaStreamDataset].topics`` a regular expression -- librdkafka treats a
      leading ``^`` as a pattern and discovers new matching topics on its own, so
      ``topics = ["^lc_classifier_[0-9]{8}"]`` follows the rotation automatically.
    - Access uses SASL_PLAINTEXT with SCRAM-SHA-256, and consumer group ids must start
      with the prefix ALeRCE assigns you. Both belong in ``credentials_file`` and
      ``group_id`` respectively.

    This class provides only the decode step and the alert's primary identifier. Getters
    for a specific ALeRCE topic live in a subclass such as `AlerceStampClassifierConsumer`
    or `AlerceLightCurveClassifierConsumer`. Users needing the array- and map-valued fields
    (the probability maps, the features record) should subclass and add a ``get_*`` method
    plus a matching ``collate_*`` hook; `BabamulPhotometryConsumer` is the worked example.
    """

    def __init__(self, config, data_location=None):
        """Initialize the ALeRCE consumer.

        Parameters
        ----------
        config : dict
            The configuration dictionary for the consumer.
        data_location : str, optional
            The location of the data stream. Defaults to None.
        """
        super().__init__(config=config, data_location=data_location)

        # ALeRCE's ZTF-era topics key alerts on "candid". Keeping this configurable means
        # the same class serves an LSST payload keyed on "diaSourceId" without a subclass.
        self.id_field = config["hyrax_alerts"]["consumer"]["AlerceConsumer"]["id_field"]

    def _decode(self, msg):
        """Decode an incoming message from an ALeRCE stream.

        Parameters
        ----------
        msg : confluent_kafka.Message
            The incoming message to be decoded.

        Returns
        -------
        dict
            The decoded alert as a dictionary.
        """
        reader = fastavro.reader(io.BytesIO(msg.value()))
        result = cast(dict[str, Any], next(reader))
        return result

    def get_candid(self, msg):
        """Extract the alert's primary identifier from the incoming message.

        Reads whichever field ``[hyrax_alerts.consumer.AlerceConsumer].id_field`` names,
        defaulting to ``candid``.

        Parameters
        ----------
        msg : dict
            The decoded alert.

        Returns
        -------
        int
            The alert identifier.
        """
        return msg[self.id_field]


class AlerceStampClassifierConsumer(AlerceConsumer):
    """A consumer for ALeRCE's stamp classifier topics.

    The stamp classifier runs on the alert's image cutouts to give an early classification.
    Its payload is ``objectId``, ``candid``, and a fixed-field ``probabilities`` record
    holding one float per class, so every class probability is a plain scalar lookup.
    """

    def get_object_id(self, msg):
        """Get the ALeRCE object id the alert belongs to."""
        return msg["objectId"]

    def get_sn_prob(self, msg):
        """Get the stamp classifier's supernova probability."""
        return msg["probabilities"]["SN"]

    def get_agn_prob(self, msg):
        """Get the stamp classifier's active galactic nucleus probability."""
        return msg["probabilities"]["AGN"]

    def get_vs_prob(self, msg):
        """Get the stamp classifier's variable star probability."""
        return msg["probabilities"]["VS"]

    def get_asteroid_prob(self, msg):
        """Get the stamp classifier's asteroid probability."""
        return msg["probabilities"]["asteroid"]

    def get_bogus_prob(self, msg):
        """Get the stamp classifier's bogus-detection probability."""
        return msg["probabilities"]["bogus"]


class AlerceLightCurveClassifierConsumer(AlerceConsumer):
    """A consumer for ALeRCE's light curve classifier topics.

    The light curve classifier runs on objects with at least six detections per band, so
    this stream is a subset of the alerts ALeRCE processes. Its payload is ``oid``,
    ``candid``, a wide ``features`` record, and an ``lc_classification`` record holding the
    winning ``class`` plus ``probabilities`` and ``hierarchical`` maps.

    Only the scalars are exposed. ``probabilities`` and ``hierarchical`` are Avro maps and
    ``features`` is a record of roughly 180 nullable floats; turning any of them into a
    fixed-width vector needs a caller-supplied class or feature ordering, so they are left
    in the decoded alert for a subclass or ``pre_process`` function to handle.
    """

    def get_oid(self, msg):
        """Get the ALeRCE object id the alert belongs to."""
        return msg["oid"]

    def get_class(self, msg):
        """Get the winning class name from the hierarchical random forest, as a string."""
        return msg["lc_classification"]["class"]
