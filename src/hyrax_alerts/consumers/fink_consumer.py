import io
import json
from pathlib import Path
from typing import Any, cast

import fastavro

from hyrax_alerts.logging_utils import get_logger

from .kafka_consumer import HyraxKafkaConsumer

logger = get_logger(__name__)

# Fink ships the writer schema with every message, so in practice only a handful of
# schema versions are ever live at once (one per deployed Fink release). The cache is
# bounded anyway so that a broker cycling through many versions cannot grow it without
# limit; when it fills it is cleared wholesale rather than evicted one entry at a time.
SCHEMA_CACHE_SIZE = 8


class FinkConsumer(HyraxKafkaConsumer):
    """A consumer for the Fink broker's Kafka livestream.

    Fink differs from other brokers supported here in how it frames alerts: the Avro
    writer schema is not embedded in the message value, it is carried as JSON in the
    Kafka message *key*, and the value holds a bare schemaless Avro datum. Decoding is
    therefore a two-step process, mirroring ``fink_client.consumer.AlertConsumer``:
    parse the schema from the key, then read the value against it.

    Topics correspond to Fink filters (one filter is one substream), and credentials are
    issued per user at registration. Both the bootstrap servers and the SASL settings
    belong in the hyrax ``[data_set.KafkaStreamDataset]`` config -- see ``credentials_file``
    for the SASL username/password/mechanism -- not in this class.

    This class provides only the decode step and the alert's primary identifier. Getters
    for a specific Fink payload live in a subclass such as `FinkLsstConsumer`. Users
    needing array-valued fields (forced-source photometry, cutouts) should subclass and
    add a ``get_*`` method plus a matching ``collate_*`` hook; `BabamulPhotometryConsumer`
    is the worked example.
    """

    def __init__(self, config, data_location=None):
        """Initialize the Fink consumer.

        Parameters
        ----------
        config : dict
            The configuration dictionary for the consumer.
        data_location : str, optional
            The location of the data stream. Defaults to None.
        """
        super().__init__(config=config, data_location=data_location)

        # Parsed-schema cache, keyed on the raw message key that produced it.
        self._schema_cache: dict[str, dict] = {}

        # Older Fink streams put a bare version string in the key instead of the schema
        # itself. There is nothing to parse in that case, so fall back to a schema the
        # user supplies on disk. Loaded eagerly so a bad path fails at startup rather
        # than on the first alert.
        schema_path = config["hyrax_alerts"]["consumer"]["FinkConsumer"]["schema_path"]
        self.fallback_schema = None
        if schema_path:
            path = Path(schema_path)
            if not path.is_file():
                raise ValueError(f"provided Fink schema file '{schema_path}' does not exist.")
            self.fallback_schema = fastavro.schema.load_schema(str(path))

    def _schema_for(self, key):
        """Return the parsed Avro schema to decode a message carrying ``key``.

        Parameters
        ----------
        key : str or None
            The Kafka message key, already decoded to text. Fink writes the full writer
            schema here as JSON; older streams write a bare version string instead.

        Returns
        -------
        dict
            The parsed schema, suitable for `fastavro.schemaless_reader`.

        Raises
        ------
        ValueError
            If the key carries no usable schema and no fallback ``schema_path`` is set.
        """
        if key:
            cached = self._schema_cache.get(key)
            if cached is not None:
                return cached

            try:
                parsed = fastavro.schema.parse_schema(json.loads(key))
            except json.JSONDecodeError:
                # Not a schema -- an old-style version string. Fall through to the
                # configured schema below.
                logger.debug("Fink message key is not a JSON schema; using the configured schema_path.")
            else:
                if len(self._schema_cache) >= SCHEMA_CACHE_SIZE:
                    self._schema_cache.clear()
                self._schema_cache[key] = parsed
                return parsed

        if self.fallback_schema is not None:
            return self.fallback_schema

        raise ValueError(
            "Cannot decode the Fink alert: the Kafka message key does not contain a JSON "
            "schema, and no fallback schema was configured. Set "
            "[hyrax_alerts.consumer.FinkConsumer].schema_path to an .avsc file matching "
            "the stream."
        )

    def _decode(self, msg):
        """Decode an incoming message from the Fink livestream.

        The writer schema is read from the message key and the value is decoded against
        it with `fastavro.schemaless_reader`. Parsed schemas are cached, because the Fink
        LSST schema carries roughly a hundred nested fields and reparsing it for every
        alert is expensive.

        Parameters
        ----------
        msg : confluent_kafka.Message
            The incoming message to be decoded.

        Returns
        -------
        dict
            The decoded alert as a dictionary.
        """
        key = msg.key()
        if isinstance(key, bytes):
            key = key.decode("utf-8")

        schema = self._schema_for(key)
        result = fastavro.schemaless_reader(io.BytesIO(msg.value()), schema)
        return cast(dict[str, Any], result)

    def get_dia_source_id(self, msg):
        """Extract the DIA source id from the incoming message.

        This is the alert's primary identifier; set ``primary_id_field = "dia_source_id"``
        in the data request to use it.

        Parameters
        ----------
        msg : dict
            The decoded alert.

        Returns
        -------
        int
            The DIA source id.
        """
        return msg["diaSourceId"]


class FinkLsstConsumer(FinkConsumer):
    """A consumer for Fink's LSST alert stream.

    Adds scalar getters for the documented Fink/LSST payload, whose top-level fields are
    ``diaSourceId``, ``diaSource``, ``prvDiaSources``, ``prvDiaForcedSources``,
    ``diaObject``, ``clf`` (Fink's science-module scores), ``xm``, the three cutouts, and
    ``lsst_schema_version``.

    Only flat scalar lookups are provided. The array-valued fields -- ``prvDiaSources``,
    ``prvDiaForcedSources`` and the cutouts -- are left untouched in the decoded alert,
    where a ``pre_process`` function or a subclass can reach them.
    """

    def get_ra(self, msg):
        """Get the right ascension of the triggering DIA source, in degrees."""
        return msg["diaSource"]["ra"]

    def get_dec(self, msg):
        """Get the declination of the triggering DIA source, in degrees."""
        return msg["diaSource"]["dec"]

    def get_mjd(self, msg):
        """Get the TAI midpoint of the exposure, as an MJD."""
        return msg["diaSource"]["midpointMjdTai"]

    def get_psf_flux(self, msg):
        """Get the PSF flux of the triggering DIA source, in nJy."""
        return msg["diaSource"]["psfFlux"]

    def get_psf_flux_err(self, msg):
        """Get the uncertainty on the PSF flux, in nJy."""
        return msg["diaSource"]["psfFluxErr"]

    def get_snr(self, msg):
        """Get the signal-to-noise ratio of the triggering DIA source."""
        return msg["diaSource"]["snr"]

    def get_band(self, msg):
        """Get the LSST filter band (one of u, g, r, i, z, y) as a string."""
        return msg["diaSource"]["band"]

    def get_reliability(self, msg):
        """Get the real/bogus reliability score of the triggering DIA source."""
        return msg["diaSource"]["reliability"]

    def get_extendedness(self, msg):
        """Get the extendedness of the triggering DIA source (0 point-like, 1 extended)."""
        return msg["diaSource"]["extendedness"]

    def get_snn_sn_vs_others(self, msg):
        """Get Fink's SuperNNova supernova-versus-others score."""
        return msg["clf"]["snnSnVsOthers_score"]

    def get_cats_score(self, msg):
        """Get the score from Fink's CATS classifier."""
        return msg["clf"]["cats_score"]

    def get_early_snia_score(self, msg):
        """Get Fink's early type-Ia supernova score."""
        return msg["clf"]["earlySNIa_score"]

    def get_n_dia_sources(self, msg):
        """Get the number of DIA sources associated with the alert's DIA object."""
        return msg["diaObject"]["nDiaSources"]
