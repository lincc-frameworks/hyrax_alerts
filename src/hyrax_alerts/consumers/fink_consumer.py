# ruff: noqa: N802

import io
import json
from pathlib import Path
from typing import Any, cast

import fastavro
import numpy as np
from hyrax.datasets.collate_utils import collate_as_1d_light_curve

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

    def get_dia_source_id(self, alert):
        """Extract the DIA source id from the incoming message.

        This is the alert's primary identifier; set ``primary_id_field = "dia_source_id"``
        in the data request to use it.

        Parameters
        ----------
        alert : dict
            The decoded alert.

        Returns
        -------
        int
            The DIA source id.
        """
        return alert["diaSourceId"]


class FinkLsstConsumer(FinkConsumer):
    """A consumer for Fink's LSST alert stream.

    Adds a getter for every field of the documented Fink/LSST payload that Hyrax can
    request, following the naming scheme ``get_<record>_<field>``, e.g.
    ``get_diaSource_ra`` or ``get_clf_snnSnVsOthers_score``. The field lists follow
    https://lsst.fink-portal.org/schemas.

    * ``diaSource``, ``diaObject``, ``clf`` and ``xm`` getters return one scalar.
    * ``prvDiaSources`` and ``prvDiaForcedSources`` getters return a 1D numpy array with
      one entry per previous source (empty if there are none). Null entries become NaN
      for float fields, and ``-1`` / ``False`` / ``""`` for integer, boolean and string
      fields.
    * The cutouts are not exposed; they stay in the decoded alert, where a
      ``pre_process`` function or a subclass can reach them.

    A field missing from a given alert raises ``KeyError``; subclass and override the
    getter if your stream omits it.

    No guarantee is made that the getters in this consumer will continue to match
    the schema defined by Fink. If schema keys are missing from this consumer,
    you should inherit from this consumer and implement the missing getters as needed.

    The schema as defined by Fink can be seen here:
    https://lsst.fink-portal.org/schemas
    This consumer was created when the advertised Fink schema version was
    f:lsst_schema_version lsst.v11_2
    The Fink broker version was f:fink_broker_version 5.0rc0
    """

    # --- Helpers ---
    @staticmethod
    def _get_light_curve_values(alert, list_name, key, dtype, null_value=None):
        """Pull ``key`` from every entry of ``alert[list_name]`` as a numpy array.

        ``null_value`` replaces null entries, for dtypes (integers, booleans, strings)
        that cannot represent a null. Float fields need none: null becomes NaN.
        """
        sources = alert.get(list_name) or []
        values = [s[key] for s in sources]
        if null_value is not None:
            values = [null_value if v is None else v for v in values]
        return np.array(values, dtype=dtype)

    @staticmethod
    def _collate_padded(samples, field, dtype):
        """Pad the 1D arrays in ``samples[i][field]`` into one (batch, max_len) array.

        Unlike Hyrax's ``collate_as_1d_light_curve``, which always produces float32, this
        keeps ``dtype`` (int64 ids, float64 MJDs, bools, strings). For strings, the width
        is that of the longest value in the batch.

        Returns
        -------
        dict
            ``field`` - (batch, max_len) array of padded values
            ``field_lengths`` - int64 array (batch) of true light curve lengths
            ``field_mask`` - int64 array (batch, max_len) denoting data vs. padding
        """
        vals = [s[field] for s in samples]
        lengths = np.array([len(v) for v in vals], dtype=np.int64)
        max_len = int(lengths.max())

        if dtype is np.str_:
            width = max([1] + [len(x) for v in vals for x in v])
            padded = np.full((len(vals), max_len), "", dtype=f"U{width}")
        else:
            padded = np.zeros((len(vals), max_len), dtype=dtype)
        mask = np.zeros((len(vals), max_len), dtype=np.int64)
        for i, v in enumerate(vals):
            padded[i, : lengths[i]] = v
            mask[i, : lengths[i]] = 1

        return {field: padded, field + "_lengths": lengths, field + "_mask": mask}

    # ==================== Getters ====================

    # --- getters: clf ---
    def get_clf_cats_class(self, alert):
        """CATS classifier broad class prediction with the highest probability."""
        return alert["clf"]["cats_class"]

    def get_clf_cats_score(self, alert):
        """CATS classifier highest probability (0...1)."""
        return alert["clf"]["cats_score"]

    def get_clf_earlySNIa_score(self, alert):
        """Score (0...1) for the early SN Ia classifier (binary classifier)."""
        return alert["clf"]["earlySNIa_score"]

    def get_clf_elephant_kstest_science(self, alert):
        """hostless indicator in the science image from the ELEPHANT pipeline."""
        return alert["clf"]["elephant_kstest_science"]

    def get_clf_elephant_kstest_template(self, alert):
        """hostless indicator in the template image from the ELEPHANT pipeline."""
        return alert["clf"]["elephant_kstest_template"]

    def get_clf_snnSnVsOthers_score(self, alert):
        """Score (0...1) for the SN classifier (binary classifier) using SuperNNova."""
        return alert["clf"]["snnSnVsOthers_score"]

    # --- getters: diaObject ---
    def get_diaObject_dec(self, alert):
        """Declination coordinate of the position of the object [deg]."""
        return alert["diaObject"]["dec"]

    def get_diaObject_decErr(self, alert):
        """Angular error of dec [deg]."""
        return alert["diaObject"]["decErr"]

    def get_diaObject_diaObjectId(self, alert):
        """Unique identifier of this DiaObject."""
        return alert["diaObject"]["diaObjectId"]

    def get_diaObject_firstDiaSourceMjdTai(self, alert):
        """Time of the first diaSource, expressed as Modified Julian Date, International Atomic Time."""
        return alert["diaObject"]["firstDiaSourceMjdTai"]

    def get_diaObject_g_fpFluxMean(self, alert):
        """Weighted mean forced photometry flux for g filter [nJy]."""
        return alert["diaObject"]["g_fpFluxMean"]

    def get_diaObject_g_fpFluxMeanErr(self, alert):
        """Standard error of g_fpFluxMean [nJy]."""
        return alert["diaObject"]["g_fpFluxMeanErr"]

    def get_diaObject_g_psfFluxErrMean(self, alert):
        """Mean of the g band flux errors [nJy]."""
        return alert["diaObject"]["g_psfFluxErrMean"]

    def get_diaObject_g_psfFluxMax(self, alert):
        """Maximum observed g band fluxes [nJy]."""
        return alert["diaObject"]["g_psfFluxMax"]

    def get_diaObject_g_psfFluxMaxSlope(self, alert):
        """Maximum slope between g band flux obsevations max(delta_flux/delta_time) [nJy/d]."""
        return alert["diaObject"]["g_psfFluxMaxSlope"]

    def get_diaObject_g_psfFluxMean(self, alert):
        """Weighted mean point-source model magnitude for g filter [nJy]."""
        return alert["diaObject"]["g_psfFluxMean"]

    def get_diaObject_g_psfFluxMeanErr(self, alert):
        """Standard error of g_psfFluxMean [nJy]."""
        return alert["diaObject"]["g_psfFluxMeanErr"]

    def get_diaObject_g_psfFluxMin(self, alert):
        """Minimum observed g band fluxes [nJy]."""
        return alert["diaObject"]["g_psfFluxMin"]

    def get_diaObject_g_psfFluxNdata(self, alert):
        """The number of g-band data points."""
        return alert["diaObject"]["g_psfFluxNdata"]

    def get_diaObject_g_psfFluxSigma(self, alert):
        """Standard deviation of the distribution of g_psfFlux [nJy]."""
        return alert["diaObject"]["g_psfFluxSigma"]

    def get_diaObject_g_scienceFluxMean(self, alert):
        """Weighted mean forced photometry flux for g filter [nJy]."""
        return alert["diaObject"]["g_scienceFluxMean"]

    def get_diaObject_g_scienceFluxMeanErr(self, alert):
        """Standard error of g_scienceFluxMean [nJy]."""
        return alert["diaObject"]["g_scienceFluxMeanErr"]

    def get_diaObject_i_fpFluxMean(self, alert):
        """Weighted mean forced photometry flux for i filter [nJy]."""
        return alert["diaObject"]["i_fpFluxMean"]

    def get_diaObject_i_fpFluxMeanErr(self, alert):
        """Standard error of i_fpFluxMean [nJy]."""
        return alert["diaObject"]["i_fpFluxMeanErr"]

    def get_diaObject_i_psfFluxErrMean(self, alert):
        """Mean of the i band flux errors [nJy]."""
        return alert["diaObject"]["i_psfFluxErrMean"]

    def get_diaObject_i_psfFluxMax(self, alert):
        """Maximum observed i band fluxes [nJy]."""
        return alert["diaObject"]["i_psfFluxMax"]

    def get_diaObject_i_psfFluxMaxSlope(self, alert):
        """Maximum slope between i band flux obsevations max(delta_flux/delta_time) [nJy/d]."""
        return alert["diaObject"]["i_psfFluxMaxSlope"]

    def get_diaObject_i_psfFluxMean(self, alert):
        """Weighted mean point-source model magnitude for i filter [nJy]."""
        return alert["diaObject"]["i_psfFluxMean"]

    def get_diaObject_i_psfFluxMeanErr(self, alert):
        """Standard error of i_psfFluxMean [nJy]."""
        return alert["diaObject"]["i_psfFluxMeanErr"]

    def get_diaObject_i_psfFluxMin(self, alert):
        """Minimum observed i band fluxes [nJy]."""
        return alert["diaObject"]["i_psfFluxMin"]

    def get_diaObject_i_psfFluxNdata(self, alert):
        """The number of i-band data points."""
        return alert["diaObject"]["i_psfFluxNdata"]

    def get_diaObject_i_psfFluxSigma(self, alert):
        """Standard deviation of the distribution of i_psfFlux [nJy]."""
        return alert["diaObject"]["i_psfFluxSigma"]

    def get_diaObject_i_scienceFluxMean(self, alert):
        """Weighted mean forced photometry flux for i filter [nJy]."""
        return alert["diaObject"]["i_scienceFluxMean"]

    def get_diaObject_i_scienceFluxMeanErr(self, alert):
        """Standard error of i_scienceFluxMean [nJy]."""
        return alert["diaObject"]["i_scienceFluxMeanErr"]

    def get_diaObject_lastDiaSourceMjdTai(self, alert):
        """Time of the most recent non-forced DIASource for this object, expressed as Modified Julian Date,
        International Atomic Time.
        """
        return alert["diaObject"]["lastDiaSourceMjdTai"]

    def get_diaObject_nDiaSources(self, alert):
        """Number of DiaSources associated with this DiaObject within the last 12 months."""
        return alert["diaObject"]["nDiaSources"]

    def get_diaObject_observation_reason(self, alert):
        """Scheduler reason for the image containing this diaSource (RTN-097)."""
        return alert["diaObject"]["observation_reason"]

    def get_diaObject_r_fpFluxMean(self, alert):
        """Weighted mean forced photometry flux for r filter [nJy]."""
        return alert["diaObject"]["r_fpFluxMean"]

    def get_diaObject_r_fpFluxMeanErr(self, alert):
        """Standard error of r_fpFluxMean [nJy]."""
        return alert["diaObject"]["r_fpFluxMeanErr"]

    def get_diaObject_r_psfFluxErrMean(self, alert):
        """Mean of the r band flux errors [nJy]."""
        return alert["diaObject"]["r_psfFluxErrMean"]

    def get_diaObject_r_psfFluxMax(self, alert):
        """Maximum observed r band fluxes [nJy]."""
        return alert["diaObject"]["r_psfFluxMax"]

    def get_diaObject_r_psfFluxMaxSlope(self, alert):
        """Maximum slope between r band flux obsevations max(delta_flux/delta_time) [nJy/d]."""
        return alert["diaObject"]["r_psfFluxMaxSlope"]

    def get_diaObject_r_psfFluxMean(self, alert):
        """Weighted mean point-source model magnitude for r filter [nJy]."""
        return alert["diaObject"]["r_psfFluxMean"]

    def get_diaObject_r_psfFluxMeanErr(self, alert):
        """Standard error of r_psfFluxMean [nJy]."""
        return alert["diaObject"]["r_psfFluxMeanErr"]

    def get_diaObject_r_psfFluxMin(self, alert):
        """Minimum observed r band fluxes [nJy]."""
        return alert["diaObject"]["r_psfFluxMin"]

    def get_diaObject_r_psfFluxNdata(self, alert):
        """The number of r-band data points."""
        return alert["diaObject"]["r_psfFluxNdata"]

    def get_diaObject_r_psfFluxSigma(self, alert):
        """Standard deviation of the distribution of r_psfFlux [nJy]."""
        return alert["diaObject"]["r_psfFluxSigma"]

    def get_diaObject_r_scienceFluxMean(self, alert):
        """Weighted mean forced photometry flux for r filter [nJy]."""
        return alert["diaObject"]["r_scienceFluxMean"]

    def get_diaObject_r_scienceFluxMeanErr(self, alert):
        """Standard error of r_scienceFluxMean [nJy]."""
        return alert["diaObject"]["r_scienceFluxMeanErr"]

    def get_diaObject_ra(self, alert):
        """Right ascension coordinate of the position of the object [deg]."""
        return alert["diaObject"]["ra"]

    def get_diaObject_ra_dec_Cov(self, alert):
        """Covariance between Right Ascension (RA*cos(Dec)) and dec [deg**2]."""
        return alert["diaObject"]["ra_dec_Cov"]

    def get_diaObject_raErr(self, alert):
        """Angular error of Right Ascension (dRA*cos(Dec)) [deg]."""
        return alert["diaObject"]["raErr"]

    def get_diaObject_target_name(self, alert):
        """Scheduler target for the image containing this diaSource (RTN-097)."""
        return alert["diaObject"]["target_name"]

    def get_diaObject_u_fpFluxMean(self, alert):
        """Weighted mean forced photometry flux for u filter [nJy]."""
        return alert["diaObject"]["u_fpFluxMean"]

    def get_diaObject_u_fpFluxMeanErr(self, alert):
        """Standard error of u_fpFluxMean [nJy]."""
        return alert["diaObject"]["u_fpFluxMeanErr"]

    def get_diaObject_u_psfFluxErrMean(self, alert):
        """Mean of the u band flux errors [nJy]."""
        return alert["diaObject"]["u_psfFluxErrMean"]

    def get_diaObject_u_psfFluxMax(self, alert):
        """Maximum observed u band fluxes [nJy]."""
        return alert["diaObject"]["u_psfFluxMax"]

    def get_diaObject_u_psfFluxMaxSlope(self, alert):
        """Maximum slope between u band flux obsevations max(delta_flux/delta_time) [nJy/d]."""
        return alert["diaObject"]["u_psfFluxMaxSlope"]

    def get_diaObject_u_psfFluxMean(self, alert):
        """Weighted mean point-source model magnitude for u filter [nJy]."""
        return alert["diaObject"]["u_psfFluxMean"]

    def get_diaObject_u_psfFluxMeanErr(self, alert):
        """Standard error of u_psfFluxMean [nJy]."""
        return alert["diaObject"]["u_psfFluxMeanErr"]

    def get_diaObject_u_psfFluxMin(self, alert):
        """Minimum observed u band fluxes [nJy]."""
        return alert["diaObject"]["u_psfFluxMin"]

    def get_diaObject_u_psfFluxNdata(self, alert):
        """The number of u-band data points."""
        return alert["diaObject"]["u_psfFluxNdata"]

    def get_diaObject_u_psfFluxSigma(self, alert):
        """Standard deviation of the distribution of u_psfFlux [nJy]."""
        return alert["diaObject"]["u_psfFluxSigma"]

    def get_diaObject_u_scienceFluxMean(self, alert):
        """Weighted mean forced photometry flux for u filter [nJy]."""
        return alert["diaObject"]["u_scienceFluxMean"]

    def get_diaObject_u_scienceFluxMeanErr(self, alert):
        """Standard error of u_scienceFluxMean [nJy]."""
        return alert["diaObject"]["u_scienceFluxMeanErr"]

    def get_diaObject_validityStartMjdTai(self, alert):
        """Processing time when validity of this diaObject starts, expressed as Modified Julian Date,
        International Atomic Time.
        """
        return alert["diaObject"]["validityStartMjdTai"]

    def get_diaObject_y_fpFluxMean(self, alert):
        """Weighted mean forced photometry flux for y filter [nJy]."""
        return alert["diaObject"]["y_fpFluxMean"]

    def get_diaObject_y_fpFluxMeanErr(self, alert):
        """Standard error of y_fpFluxMean [nJy]."""
        return alert["diaObject"]["y_fpFluxMeanErr"]

    def get_diaObject_y_psfFluxErrMean(self, alert):
        """Mean of the y band flux errors [nJy]."""
        return alert["diaObject"]["y_psfFluxErrMean"]

    def get_diaObject_y_psfFluxMax(self, alert):
        """Maximum observed y band fluxes [nJy]."""
        return alert["diaObject"]["y_psfFluxMax"]

    def get_diaObject_y_psfFluxMaxSlope(self, alert):
        """Maximum slope between y band flux obsevations max(delta_flux/delta_time) [nJy/d]."""
        return alert["diaObject"]["y_psfFluxMaxSlope"]

    def get_diaObject_y_psfFluxMean(self, alert):
        """Weighted mean point-source model magnitude for y filter [nJy]."""
        return alert["diaObject"]["y_psfFluxMean"]

    def get_diaObject_y_psfFluxMeanErr(self, alert):
        """Standard error of y_psfFluxMean [nJy]."""
        return alert["diaObject"]["y_psfFluxMeanErr"]

    def get_diaObject_y_psfFluxMin(self, alert):
        """Minimum observed y band fluxes [nJy]."""
        return alert["diaObject"]["y_psfFluxMin"]

    def get_diaObject_y_psfFluxNdata(self, alert):
        """The number of y-band data points."""
        return alert["diaObject"]["y_psfFluxNdata"]

    def get_diaObject_y_psfFluxSigma(self, alert):
        """Standard deviation of the distribution of y_psfFlux [nJy]."""
        return alert["diaObject"]["y_psfFluxSigma"]

    def get_diaObject_y_scienceFluxMean(self, alert):
        """Weighted mean forced photometry flux for y filter [nJy]."""
        return alert["diaObject"]["y_scienceFluxMean"]

    def get_diaObject_y_scienceFluxMeanErr(self, alert):
        """Standard error of y_scienceFluxMean [nJy]."""
        return alert["diaObject"]["y_scienceFluxMeanErr"]

    def get_diaObject_z_fpFluxMean(self, alert):
        """Weighted mean forced photometry flux for z filter [nJy]."""
        return alert["diaObject"]["z_fpFluxMean"]

    def get_diaObject_z_fpFluxMeanErr(self, alert):
        """Standard error of z_fpFluxMean [nJy]."""
        return alert["diaObject"]["z_fpFluxMeanErr"]

    def get_diaObject_z_psfFluxErrMean(self, alert):
        """Mean of the z band flux errors [nJy]."""
        return alert["diaObject"]["z_psfFluxErrMean"]

    def get_diaObject_z_psfFluxMax(self, alert):
        """Maximum observed z band fluxes [nJy]."""
        return alert["diaObject"]["z_psfFluxMax"]

    def get_diaObject_z_psfFluxMaxSlope(self, alert):
        """Maximum slope between z band flux obsevations max(delta_flux/delta_time) [nJy/d]."""
        return alert["diaObject"]["z_psfFluxMaxSlope"]

    def get_diaObject_z_psfFluxMean(self, alert):
        """Weighted mean point-source model magnitude for z filter [nJy]."""
        return alert["diaObject"]["z_psfFluxMean"]

    def get_diaObject_z_psfFluxMeanErr(self, alert):
        """Standard error of z_psfFluxMean [nJy]."""
        return alert["diaObject"]["z_psfFluxMeanErr"]

    def get_diaObject_z_psfFluxMin(self, alert):
        """Minimum observed z band fluxes [nJy]."""
        return alert["diaObject"]["z_psfFluxMin"]

    def get_diaObject_z_psfFluxNdata(self, alert):
        """The number of z-band data points."""
        return alert["diaObject"]["z_psfFluxNdata"]

    def get_diaObject_z_psfFluxSigma(self, alert):
        """Standard deviation of the distribution of z_psfFlux [nJy]."""
        return alert["diaObject"]["z_psfFluxSigma"]

    def get_diaObject_z_scienceFluxMean(self, alert):
        """Weighted mean forced photometry flux for z filter [nJy]."""
        return alert["diaObject"]["z_scienceFluxMean"]

    def get_diaObject_z_scienceFluxMeanErr(self, alert):
        """Standard error of z_scienceFluxMean [nJy]."""
        return alert["diaObject"]["z_scienceFluxMeanErr"]

    # --- getters: diaSource ---
    def get_diaSource_apFlux(self, alert):
        """Flux in a 12 pixel radius aperture on the difference image [nJy]."""
        return alert["diaSource"]["apFlux"]

    def get_diaSource_apFlux_flag(self, alert):
        """General aperture flux algorithm failure flag; set if anything went wrong when measuring aperture
        fluxes.
        """
        return alert["diaSource"]["apFlux_flag"]

    def get_diaSource_apFlux_flag_apertureTruncated(self, alert):
        """The source is closer to the edge of the measurement image than the aperture radius."""
        return alert["diaSource"]["apFlux_flag_apertureTruncated"]

    def get_diaSource_apFluxErr(self, alert):
        """Estimated uncertainty of apFlux [nJy]."""
        return alert["diaSource"]["apFluxErr"]

    def get_diaSource_band(self, alert):
        """Filter band this source was observed with."""
        return alert["diaSource"]["band"]

    def get_diaSource_bboxSize(self, alert):
        """Size of the square bounding box that fully contains the detection footprint [pixel]."""
        return alert["diaSource"]["bboxSize"]

    def get_diaSource_centroid_flag(self, alert):
        """General centroid algorithm failure flag; set if anything went wrong when fitting the centroid."""
        return alert["diaSource"]["centroid_flag"]

    def get_diaSource_dec(self, alert):
        """Declination coordinate of the center of this diaSource [deg]."""
        return alert["diaSource"]["dec"]

    def get_diaSource_decErr(self, alert):
        """Angular error of dec [deg]."""
        return alert["diaSource"]["decErr"]

    def get_diaSource_detector(self, alert):
        """Id of the detector where this diaSource was measured."""
        return alert["diaSource"]["detector"]

    def get_diaSource_diaObjectId(self, alert):
        """Id of the diaObject this source was associated with, if any."""
        return alert["diaSource"]["diaObjectId"]

    def get_diaSource_diaSourceId(self, alert):
        """Identifier of the triggering DiaSource."""
        return alert["diaSource"]["diaSourceId"]

    def get_diaSource_dipoleAngle(self, alert):
        """Maximum likelihood fit of the angle between the meridian through the centroid and the dipole
        direction (bearing, from negative to positive lobe) [deg].
        """
        return alert["diaSource"]["dipoleAngle"]

    def get_diaSource_dipoleChi2(self, alert):
        """Chi^2 statistic of the model fit."""
        return alert["diaSource"]["dipoleChi2"]

    def get_diaSource_dipoleFitAttempted(self, alert):
        """Attempted to fit a dipole model to this source."""
        return alert["diaSource"]["dipoleFitAttempted"]

    def get_diaSource_dipoleFluxDiff(self, alert):
        """Maximum likelihood value for the difference of absolute fluxes of the two lobes for a dipole
        model [nJy].
        """
        return alert["diaSource"]["dipoleFluxDiff"]

    def get_diaSource_dipoleFluxDiffErr(self, alert):
        """Uncertainty of dipoleFluxDiff [nJy]."""
        return alert["diaSource"]["dipoleFluxDiffErr"]

    def get_diaSource_dipoleLength(self, alert):
        """Maximum likelihood value for the lobe separation in dipole model [arcsec]."""
        return alert["diaSource"]["dipoleLength"]

    def get_diaSource_dipoleMeanFlux(self, alert):
        """Maximum likelihood value for the mean absolute flux of the two lobes for a dipole model [nJy]."""
        return alert["diaSource"]["dipoleMeanFlux"]

    def get_diaSource_dipoleMeanFluxErr(self, alert):
        """Uncertainty of dipoleMeanFlux [nJy]."""
        return alert["diaSource"]["dipoleMeanFluxErr"]

    def get_diaSource_dipoleNdata(self, alert):
        """The number of data points (pixels) used to fit the model."""
        return alert["diaSource"]["dipoleNdata"]

    def get_diaSource_exposureTime(self, alert):
        """Effective exposure time for this diaSource [s]."""
        return alert["diaSource"]["exposureTime"]

    def get_diaSource_extendedness(self, alert):
        """A measure of extendedness, computed by comparing an object's moment-based traced radius to the
        PSF moments.
        """
        return alert["diaSource"]["extendedness"]

    def get_diaSource_forced_PsfFlux_flag(self, alert):
        """Forced PSF photometry on science image failed."""
        return alert["diaSource"]["forced_PsfFlux_flag"]

    def get_diaSource_forced_PsfFlux_flag_edge(self, alert):
        """Forced PSF flux on science image was too close to the edge of the image to use the full
        PSF model.
        """
        return alert["diaSource"]["forced_PsfFlux_flag_edge"]

    def get_diaSource_forced_PsfFlux_flag_noGoodPixels(self, alert):
        """Forced PSF flux not enough non-rejected pixels in data to attempt the fit."""
        return alert["diaSource"]["forced_PsfFlux_flag_noGoodPixels"]

    def get_diaSource_glint_trail(self, alert):
        """This flag is set if the source is likely part of a glint trail from an artifical satellite."""
        return alert["diaSource"]["glint_trail"]

    def get_diaSource_isDipole(self, alert):
        """Source well fit by a dipole."""
        return alert["diaSource"]["isDipole"]

    def get_diaSource_isNegative(self, alert):
        """The original peak on the PSF-convolved detection difference image was negative."""
        return alert["diaSource"]["isNegative"]

    def get_diaSource_ixx(self, alert):
        """Adaptive second moment of the source intensity [arcsec**2]."""
        return alert["diaSource"]["ixx"]

    def get_diaSource_ixxPSF(self, alert):
        """Adaptive second moment of the PSF measured at the location of the source [arcsec**2]."""
        return alert["diaSource"]["ixxPSF"]

    def get_diaSource_ixy(self, alert):
        """Adaptive second moment of the source intensity [arcsec**2]."""
        return alert["diaSource"]["ixy"]

    def get_diaSource_ixyPSF(self, alert):
        """Adaptive second moment of the PSF measured at the location of the source [arcsec**2]."""
        return alert["diaSource"]["ixyPSF"]

    def get_diaSource_iyy(self, alert):
        """Adaptive second moment of the source intensity [arcsec**2]."""
        return alert["diaSource"]["iyy"]

    def get_diaSource_iyyPSF(self, alert):
        """Adaptive second moment of the PSF measured at the location of the source [arcsec**2]."""
        return alert["diaSource"]["iyyPSF"]

    def get_diaSource_midpointMjdTai(self, alert):
        """Effective mid-visit time for this diaSource, expressed as Modified Julian Date, International
        Atomic Time [d].
        """
        return alert["diaSource"]["midpointMjdTai"]

    def get_diaSource_mixedInstruments(self, alert):
        """Set if the template instrument is known and differs from the instrument that observed the
        diaSource.
        """
        return alert["diaSource"]["mixedInstruments"]

    def get_diaSource_observation_reason(self, alert):
        """Scheduler reason for the image containing this diaSource (RTN-097)."""
        return alert["diaSource"]["observation_reason"]

    def get_diaSource_parentDiaSourceId(self, alert):
        """Id of the parent diaSource this diaSource has been deblended from, if any."""
        return alert["diaSource"]["parentDiaSourceId"]

    def get_diaSource_pixelFlags(self, alert):
        """General pixel flags failure; set if anything went wrong when setting pixels flags from this
        footprint's mask.
        """
        return alert["diaSource"]["pixelFlags"]

    def get_diaSource_pixelFlags_bad(self, alert):
        """Bad pixel in the DiaSource footprint."""
        return alert["diaSource"]["pixelFlags_bad"]

    def get_diaSource_pixelFlags_cr(self, alert):
        """Cosmic ray in the DiaSource footprint."""
        return alert["diaSource"]["pixelFlags_cr"]

    def get_diaSource_pixelFlags_crCenter(self, alert):
        """Cosmic ray in the 3x3 region around the centroid."""
        return alert["diaSource"]["pixelFlags_crCenter"]

    def get_diaSource_pixelFlags_edge(self, alert):
        """Some of the source footprint is outside usable exposure region (masked EDGE or centroid off
        image).
        """
        return alert["diaSource"]["pixelFlags_edge"]

    def get_diaSource_pixelFlags_injected(self, alert):
        """A pixel in the diaSource footprint contains at least one pixel modified by injecting fake
        sources.
        """
        return alert["diaSource"]["pixelFlags_injected"]

    def get_diaSource_pixelFlags_injected_template(self, alert):
        """A pixel in the diaSource footprint contains at least one pixel modified by injecting fake
        sources into the template.
        """
        return alert["diaSource"]["pixelFlags_injected_template"]

    def get_diaSource_pixelFlags_injected_templateCenter(self, alert):
        """A pixel in the 3x3 region around the centroid contains at least one pixel modified by injecting
        fake sources into the template.
        """
        return alert["diaSource"]["pixelFlags_injected_templateCenter"]

    def get_diaSource_pixelFlags_injectedCenter(self, alert):
        """A pixel in the 3x3 region around the centroid contains at least one pixel modified by injecting
        fake sources.
        """
        return alert["diaSource"]["pixelFlags_injectedCenter"]

    def get_diaSource_pixelFlags_interpolated(self, alert):
        """Interpolated pixel in the DiaSource footprint."""
        return alert["diaSource"]["pixelFlags_interpolated"]

    def get_diaSource_pixelFlags_interpolatedCenter(self, alert):
        """Interpolated pixel in the 3x3 region around the centroid."""
        return alert["diaSource"]["pixelFlags_interpolatedCenter"]

    def get_diaSource_pixelFlags_nodata(self, alert):
        """NO_DATA pixel in the source footprint."""
        return alert["diaSource"]["pixelFlags_nodata"]

    def get_diaSource_pixelFlags_nodataCenter(self, alert):
        """NO_DATA pixel in the 3x3 region around the centroid."""
        return alert["diaSource"]["pixelFlags_nodataCenter"]

    def get_diaSource_pixelFlags_offimage(self, alert):
        """DiaSource center is off image."""
        return alert["diaSource"]["pixelFlags_offimage"]

    def get_diaSource_pixelFlags_saturated(self, alert):
        """Saturated pixel in the DiaSource footprint."""
        return alert["diaSource"]["pixelFlags_saturated"]

    def get_diaSource_pixelFlags_saturatedCenter(self, alert):
        """Saturated pixel in the 3x3 region around the centroid."""
        return alert["diaSource"]["pixelFlags_saturatedCenter"]

    def get_diaSource_pixelFlags_spike(self, alert):
        """The diaSource footprint includes pixels on a predicted diffraction spike of a bright star."""
        return alert["diaSource"]["pixelFlags_spike"]

    def get_diaSource_pixelFlags_streak(self, alert):
        """Streak in the DiaSource footprint."""
        return alert["diaSource"]["pixelFlags_streak"]

    def get_diaSource_pixelFlags_streakCenter(self, alert):
        """Streak in the 3x3 region around the centroid."""
        return alert["diaSource"]["pixelFlags_streakCenter"]

    def get_diaSource_pixelFlags_suspect(self, alert):
        """DiaSource's footprint includes pixels masked as SUSPECT that are approaching the saturation
        threshold.
        """
        return alert["diaSource"]["pixelFlags_suspect"]

    def get_diaSource_pixelFlags_suspectCenter(self, alert):
        """The 3x3 region around the centroid of the DiaSource includes pixels masked as SUSPECT that are
        approaching the saturation threshold
        """
        return alert["diaSource"]["pixelFlags_suspectCenter"]

    def get_diaSource_psfChi2(self, alert):
        """Chi^2 statistic of the point source model fit."""
        return alert["diaSource"]["psfChi2"]

    def get_diaSource_psfFlux(self, alert):
        """Flux for Point Source model."""
        return alert["diaSource"]["psfFlux"]

    def get_diaSource_psfFlux_flag(self, alert):
        """Failure to derive linear least-squares fit of psf model."""
        return alert["diaSource"]["psfFlux_flag"]

    def get_diaSource_psfFlux_flag_edge(self, alert):
        """Object was too close to the edge of the image to use the full PSF model."""
        return alert["diaSource"]["psfFlux_flag_edge"]

    def get_diaSource_psfFlux_flag_noGoodPixels(self, alert):
        """Not enough non-rejected pixels in data to attempt the fit."""
        return alert["diaSource"]["psfFlux_flag_noGoodPixels"]

    def get_diaSource_psfFluxErr(self, alert):
        """Uncertainty of psfFlux [nJy]."""
        return alert["diaSource"]["psfFluxErr"]

    def get_diaSource_psfLnL(self, alert):
        """Natural log likelihood of the observed data given the point source model."""
        return alert["diaSource"]["psfLnL"]

    def get_diaSource_psfNdata(self, alert):
        """The number of data points (pixels) used to fit the point source model."""
        return alert["diaSource"]["psfNdata"]

    def get_diaSource_ra(self, alert):
        """Right ascension coordinate of the center of this diaSource [deg]."""
        return alert["diaSource"]["ra"]

    def get_diaSource_ra_dec_Cov(self, alert):
        """Covariance between Right Ascension (RA*cos(Dec)) and dec [deg**2]."""
        return alert["diaSource"]["ra_dec_Cov"]

    def get_diaSource_raErr(self, alert):
        """Angular error of Right Ascension (dRA*cos(Dec)) [deg]."""
        return alert["diaSource"]["raErr"]

    def get_diaSource_reliability(self, alert):
        """Probability (0-1) that the diaSource is astrophysical, derived from a machine learning model."""
        return alert["diaSource"]["reliability"]

    def get_diaSource_reliabilityVersion(self, alert):
        """Version of the reliability model."""
        return alert["diaSource"]["reliabilityVersion"]

    def get_diaSource_scienceFlux(self, alert):
        """Forced photometry flux for a point source model measured on the visit image centered at
        DiaSource position [nJy].
        """
        return alert["diaSource"]["scienceFlux"]

    def get_diaSource_scienceFluxErr(self, alert):
        """Estimated uncertainty of scienceFlux [nJy]."""
        return alert["diaSource"]["scienceFluxErr"]

    def get_diaSource_shape_flag(self, alert):
        """General source shape algorithm failure flag; set if anything went wrong when measuring the
        shape.
        """
        return alert["diaSource"]["shape_flag"]

    def get_diaSource_shape_flag_no_pixels(self, alert):
        """No pixels were useable for computing shape measurement."""
        return alert["diaSource"]["shape_flag_no_pixels"]

    def get_diaSource_shape_flag_not_contained(self, alert):
        """Center not contained in footprint bounding box."""
        return alert["diaSource"]["shape_flag_not_contained"]

    def get_diaSource_shape_flag_parent_source(self, alert):
        """This source is a parent source; we should only be measuring on deblended children in difference
        imaging.
        """
        return alert["diaSource"]["shape_flag_parent_source"]

    def get_diaSource_snr(self, alert):
        """The signal-to-noise ratio at which this source was detected in the difference image."""
        return alert["diaSource"]["snr"]

    def get_diaSource_ssObjectId(self, alert):
        """Id of the ssObject this source was associated with, if any."""
        return alert["diaSource"]["ssObjectId"]

    def get_diaSource_target_name(self, alert):
        """Scheduler target for the image containing this diaSource (RTN-097)."""
        return alert["diaSource"]["target_name"]

    def get_diaSource_templateFlux(self, alert):
        """Forced photometry flux for a point source model measured on the template image centered at the
        DiaObject position [nJy].
        """
        return alert["diaSource"]["templateFlux"]

    def get_diaSource_templateFluxErr(self, alert):
        """Uncertainty of templateFlux [nJy]."""
        return alert["diaSource"]["templateFluxErr"]

    def get_diaSource_templateInstrument(self, alert):
        """Name of the instrument whose observations were coadded to make the template, or UNKNOWN if it is
        not known.
        """
        return alert["diaSource"]["templateInstrument"]

    def get_diaSource_templateMjdMax(self, alert):
        """Latest mid-exposure time of the template input images that cover the diaSource centroid,
        expressed as Modified Julian Date, International Atomic Time [d].
        """
        return alert["diaSource"]["templateMjdMax"]

    def get_diaSource_templateMjdMean(self, alert):
        """Weighted mean of the mid-exposure times of the template input images that cover the diaSource
        centroid, expressed as Modified Julian Date, International Atomic Time.
        """
        return alert["diaSource"]["templateMjdMean"]

    def get_diaSource_templateMjdMin(self, alert):
        """Earliest mid-exposure time of the template input images that cover the diaSource centroid,
        expressed as Modified Julian Date, International Atomic Time [d].
        """
        return alert["diaSource"]["templateMjdMin"]

    def get_diaSource_templateVersion(self, alert):
        """Version of the template."""
        return alert["diaSource"]["templateVersion"]

    def get_diaSource_timeProcessedMjdTai(self, alert):
        """Time when the image was processed and this DiaSource record was generated, expressed as Modified
        Julian Date, International Atomic Time.
        """
        return alert["diaSource"]["timeProcessedMjdTai"]

    def get_diaSource_timeWithdrawnMjdTai(self, alert):
        """Time when this record was marked invalid, expressed as Modified Julian Date, International
        Atomic Time.
        """
        return alert["diaSource"]["timeWithdrawnMjdTai"]

    def get_diaSource_trail_flag(self, alert):
        """The trailed source fitting algorithm failed."""
        return alert["diaSource"]["trail_flag"]

    def get_diaSource_trail_flag_edge(self, alert):
        """This flag is set if a trailed source extends onto or past edge pixels."""
        return alert["diaSource"]["trail_flag_edge"]

    def get_diaSource_trailAlgorithm(self, alert):
        """Integer key indicating which algorithm was used to measure trailed source."""
        return alert["diaSource"]["trailAlgorithm"]

    def get_diaSource_trailAngle(self, alert):
        """Maximum likelihood fit of the angle between the meridian through the centroid and the trail
        direction (bearing) [deg].
        """
        return alert["diaSource"]["trailAngle"]

    def get_diaSource_trailAngleErr(self, alert):
        """Uncertainty of trailAngle [deg]."""
        return alert["diaSource"]["trailAngleErr"]

    def get_diaSource_trailChi2(self, alert):
        """Chi^2 statistic of the trailed source model fit."""
        return alert["diaSource"]["trailChi2"]

    def get_diaSource_trailDec(self, alert):
        """Declination coordinate of centroid for trailed source model [deg]."""
        return alert["diaSource"]["trailDec"]

    def get_diaSource_trailDecErr(self, alert):
        """Angular error of trailDec [deg]."""
        return alert["diaSource"]["trailDecErr"]

    def get_diaSource_trailFlux(self, alert):
        """Flux for a trailed source model."""
        return alert["diaSource"]["trailFlux"]

    def get_diaSource_trailFluxErr(self, alert):
        """Uncertainty of trailFlux [nJy]."""
        return alert["diaSource"]["trailFluxErr"]

    def get_diaSource_trailLength(self, alert):
        """Maximum likelihood fit of trail length [arcsec]."""
        return alert["diaSource"]["trailLength"]

    def get_diaSource_trailLengthErr(self, alert):
        """Uncertainty of trailLength [arcsec]."""
        return alert["diaSource"]["trailLengthErr"]

    def get_diaSource_trailNdata(self, alert):
        """The number of data points (pixels) used to fit the trailed source model."""
        return alert["diaSource"]["trailNdata"]

    def get_diaSource_trailRa(self, alert):
        """Right ascension coordinate of centroid for trailed source model [deg]."""
        return alert["diaSource"]["trailRa"]

    def get_diaSource_trailRaErr(self, alert):
        """Angular error of trailRa (dRA*cos(Dec)) [deg]."""
        return alert["diaSource"]["trailRaErr"]

    def get_diaSource_visit(self, alert):
        """Id of the visit where this diaSource was measured."""
        return alert["diaSource"]["visit"]

    def get_diaSource_x(self, alert):
        """x position computed by a centroiding algorithm [pixel]."""
        return alert["diaSource"]["x"]

    def get_diaSource_xErr(self, alert):
        """Uncertainty of x [pixel]."""
        return alert["diaSource"]["xErr"]

    def get_diaSource_y(self, alert):
        """y position computed by a centroiding algorithm [pixel]."""
        return alert["diaSource"]["y"]

    def get_diaSource_yErr(self, alert):
        """Uncertainty of y [pixel]."""
        return alert["diaSource"]["yErr"]

    # --- getters: schema version ---
    def get_lsst_schema_version(self, alert):
        """Get the LSST alert schema version the alert was produced with."""
        return alert["lsst_schema_version"]

    # --- getters: prvDiaForcedSources ---
    def get_prvDiaForcedSources_band(self, alert):
        """Filter band this source was observed with."""
        return self._get_light_curve_values(alert, "prvDiaForcedSources", "band", np.str_, null_value="")

    def get_prvDiaForcedSources_dec(self, alert):
        """Declination coordinate of the position of the DiaObject [deg]."""
        return self._get_light_curve_values(alert, "prvDiaForcedSources", "dec", np.float64)

    def get_prvDiaForcedSources_detector(self, alert):
        """Id of the detector where this forcedSource was measured."""
        return self._get_light_curve_values(alert, "prvDiaForcedSources", "detector", np.int64)

    def get_prvDiaForcedSources_diaForcedSourceId(self, alert):
        """Unique id."""
        return self._get_light_curve_values(alert, "prvDiaForcedSources", "diaForcedSourceId", np.int64)

    def get_prvDiaForcedSources_diaObjectId(self, alert):
        """Id of the DiaObject that this DiaForcedSource was associated with."""
        return self._get_light_curve_values(alert, "prvDiaForcedSources", "diaObjectId", np.int64)

    def get_prvDiaForcedSources_midpointMjdTai(self, alert):
        """Effective mid-visit time for this diaForcedSource, expressed as Modified Julian Date,
        International Atomic Time [d].
        """
        return self._get_light_curve_values(alert, "prvDiaForcedSources", "midpointMjdTai", np.float64)

    def get_prvDiaForcedSources_psfFlux(self, alert):
        """Point Source model flux [nJy]."""
        return self._get_light_curve_values(alert, "prvDiaForcedSources", "psfFlux", np.float32)

    def get_prvDiaForcedSources_psfFluxErr(self, alert):
        """Uncertainty of psfFlux [nJy]."""
        return self._get_light_curve_values(alert, "prvDiaForcedSources", "psfFluxErr", np.float32)

    def get_prvDiaForcedSources_ra(self, alert):
        """Right ascension coordinate of the position of the DiaObject [deg]."""
        return self._get_light_curve_values(alert, "prvDiaForcedSources", "ra", np.float64)

    def get_prvDiaForcedSources_scienceFlux(self, alert):
        """Forced photometry flux for a point source model measured on the visit image centered at the
        DiaObject position [nJy].
        """
        return self._get_light_curve_values(alert, "prvDiaForcedSources", "scienceFlux", np.float32)

    def get_prvDiaForcedSources_scienceFluxErr(self, alert):
        """Uncertainty of scienceFlux [nJy]."""
        return self._get_light_curve_values(alert, "prvDiaForcedSources", "scienceFluxErr", np.float32)

    def get_prvDiaForcedSources_timeProcessedMjdTai(self, alert):
        """Time when this record was generated, expressed as Modified Julian Date, International
        Atomic Time.
        """
        return self._get_light_curve_values(alert, "prvDiaForcedSources", "timeProcessedMjdTai", np.float64)

    def get_prvDiaForcedSources_timeWithdrawnMjdTai(self, alert):
        """Time when this record was marked invalid, expressed as Modified Julian Date, International
        Atomic Time.
        """
        return self._get_light_curve_values(alert, "prvDiaForcedSources", "timeWithdrawnMjdTai", np.float64)

    def get_prvDiaForcedSources_visit(self, alert):
        """Id of the visit where this forcedSource was measured."""
        return self._get_light_curve_values(alert, "prvDiaForcedSources", "visit", np.int64)

    # --- getters: prvDiaSources ---
    def get_prvDiaSources_apFlux(self, alert):
        """Flux in a 12 pixel radius aperture on the difference image [nJy]."""
        return self._get_light_curve_values(alert, "prvDiaSources", "apFlux", np.float32)

    def get_prvDiaSources_apFlux_flag(self, alert):
        """General aperture flux algorithm failure flag; set if anything went wrong when measuring aperture
        fluxes.
        """
        return self._get_light_curve_values(alert, "prvDiaSources", "apFlux_flag", np.bool_, null_value=False)

    def get_prvDiaSources_apFlux_flag_apertureTruncated(self, alert):
        """The source is closer to the edge of the measurement image than the aperture radius."""
        return self._get_light_curve_values(
            alert, "prvDiaSources", "apFlux_flag_apertureTruncated", np.bool_, null_value=False
        )

    def get_prvDiaSources_apFluxErr(self, alert):
        """Estimated uncertainty of apFlux [nJy]."""
        return self._get_light_curve_values(alert, "prvDiaSources", "apFluxErr", np.float32)

    def get_prvDiaSources_band(self, alert):
        """Filter band this source was observed with."""
        return self._get_light_curve_values(alert, "prvDiaSources", "band", np.str_, null_value="")

    def get_prvDiaSources_bboxSize(self, alert):
        """Size of the square bounding box that fully contains the detection footprint [pixel]."""
        return self._get_light_curve_values(alert, "prvDiaSources", "bboxSize", np.int64)

    def get_prvDiaSources_centroid_flag(self, alert):
        """General centroid algorithm failure flag; set if anything went wrong when fitting the centroid."""
        return self._get_light_curve_values(
            alert, "prvDiaSources", "centroid_flag", np.bool_, null_value=False
        )

    def get_prvDiaSources_dec(self, alert):
        """Declination coordinate of the center of this diaSource [deg]."""
        return self._get_light_curve_values(alert, "prvDiaSources", "dec", np.float64)

    def get_prvDiaSources_decErr(self, alert):
        """Angular error of dec [deg]."""
        return self._get_light_curve_values(alert, "prvDiaSources", "decErr", np.float32)

    def get_prvDiaSources_detector(self, alert):
        """Id of the detector where this diaSource was measured."""
        return self._get_light_curve_values(alert, "prvDiaSources", "detector", np.int64)

    def get_prvDiaSources_diaObjectId(self, alert):
        """Id of the diaObject this source was associated with, if any."""
        return self._get_light_curve_values(alert, "prvDiaSources", "diaObjectId", np.int64, null_value=-1)

    def get_prvDiaSources_diaSourceId(self, alert):
        """Identifier of the triggering DiaSource."""
        return self._get_light_curve_values(alert, "prvDiaSources", "diaSourceId", np.int64)

    def get_prvDiaSources_dipoleAngle(self, alert):
        """Maximum likelihood fit of the angle between the meridian through the centroid and the dipole
        direction (bearing, from negative to positive lobe) [deg].
        """
        return self._get_light_curve_values(alert, "prvDiaSources", "dipoleAngle", np.float32)

    def get_prvDiaSources_dipoleChi2(self, alert):
        """Chi^2 statistic of the model fit."""
        return self._get_light_curve_values(alert, "prvDiaSources", "dipoleChi2", np.float32)

    def get_prvDiaSources_dipoleFitAttempted(self, alert):
        """Attempted to fit a dipole model to this source."""
        return self._get_light_curve_values(
            alert, "prvDiaSources", "dipoleFitAttempted", np.bool_, null_value=False
        )

    def get_prvDiaSources_dipoleFluxDiff(self, alert):
        """Maximum likelihood value for the difference of absolute fluxes of the two lobes for a dipole
        model [nJy].
        """
        return self._get_light_curve_values(alert, "prvDiaSources", "dipoleFluxDiff", np.float32)

    def get_prvDiaSources_dipoleFluxDiffErr(self, alert):
        """Uncertainty of dipoleFluxDiff [nJy]."""
        return self._get_light_curve_values(alert, "prvDiaSources", "dipoleFluxDiffErr", np.float32)

    def get_prvDiaSources_dipoleLength(self, alert):
        """Maximum likelihood value for the lobe separation in dipole model [arcsec]."""
        return self._get_light_curve_values(alert, "prvDiaSources", "dipoleLength", np.float32)

    def get_prvDiaSources_dipoleMeanFlux(self, alert):
        """Maximum likelihood value for the mean absolute flux of the two lobes for a dipole model [nJy]."""
        return self._get_light_curve_values(alert, "prvDiaSources", "dipoleMeanFlux", np.float32)

    def get_prvDiaSources_dipoleMeanFluxErr(self, alert):
        """Uncertainty of dipoleMeanFlux [nJy]."""
        return self._get_light_curve_values(alert, "prvDiaSources", "dipoleMeanFluxErr", np.float32)

    def get_prvDiaSources_dipoleNdata(self, alert):
        """The number of data points (pixels) used to fit the model."""
        return self._get_light_curve_values(alert, "prvDiaSources", "dipoleNdata", np.int64)

    def get_prvDiaSources_exposureTime(self, alert):
        """Effective exposure time for this diaSource [s]."""
        return self._get_light_curve_values(alert, "prvDiaSources", "exposureTime", np.float32)

    def get_prvDiaSources_extendedness(self, alert):
        """A measure of extendedness, computed by comparing an object's moment-based traced radius to the
        PSF moments.
        """
        return self._get_light_curve_values(alert, "prvDiaSources", "extendedness", np.float32)

    def get_prvDiaSources_forced_PsfFlux_flag(self, alert):
        """Forced PSF photometry on science image failed."""
        return self._get_light_curve_values(
            alert, "prvDiaSources", "forced_PsfFlux_flag", np.bool_, null_value=False
        )

    def get_prvDiaSources_forced_PsfFlux_flag_edge(self, alert):
        """Forced PSF flux on science image was too close to the edge of the image to use the full
        PSF model.
        """
        return self._get_light_curve_values(
            alert, "prvDiaSources", "forced_PsfFlux_flag_edge", np.bool_, null_value=False
        )

    def get_prvDiaSources_forced_PsfFlux_flag_noGoodPixels(self, alert):
        """Forced PSF flux not enough non-rejected pixels in data to attempt the fit."""
        return self._get_light_curve_values(
            alert, "prvDiaSources", "forced_PsfFlux_flag_noGoodPixels", np.bool_, null_value=False
        )

    def get_prvDiaSources_glint_trail(self, alert):
        """This flag is set if the source is likely part of a glint trail from an artifical satellite."""
        return self._get_light_curve_values(alert, "prvDiaSources", "glint_trail", np.bool_, null_value=False)

    def get_prvDiaSources_isDipole(self, alert):
        """Source well fit by a dipole."""
        return self._get_light_curve_values(alert, "prvDiaSources", "isDipole", np.bool_, null_value=False)

    def get_prvDiaSources_isNegative(self, alert):
        """The original peak on the PSF-convolved detection difference image was negative."""
        return self._get_light_curve_values(alert, "prvDiaSources", "isNegative", np.bool_, null_value=False)

    def get_prvDiaSources_ixx(self, alert):
        """Adaptive second moment of the source intensity [arcsec**2]."""
        return self._get_light_curve_values(alert, "prvDiaSources", "ixx", np.float32)

    def get_prvDiaSources_ixxPSF(self, alert):
        """Adaptive second moment of the PSF measured at the location of the source [arcsec**2]."""
        return self._get_light_curve_values(alert, "prvDiaSources", "ixxPSF", np.float32)

    def get_prvDiaSources_ixy(self, alert):
        """Adaptive second moment of the source intensity [arcsec**2]."""
        return self._get_light_curve_values(alert, "prvDiaSources", "ixy", np.float32)

    def get_prvDiaSources_ixyPSF(self, alert):
        """Adaptive second moment of the PSF measured at the location of the source [arcsec**2]."""
        return self._get_light_curve_values(alert, "prvDiaSources", "ixyPSF", np.float32)

    def get_prvDiaSources_iyy(self, alert):
        """Adaptive second moment of the source intensity [arcsec**2]."""
        return self._get_light_curve_values(alert, "prvDiaSources", "iyy", np.float32)

    def get_prvDiaSources_iyyPSF(self, alert):
        """Adaptive second moment of the PSF measured at the location of the source [arcsec**2]."""
        return self._get_light_curve_values(alert, "prvDiaSources", "iyyPSF", np.float32)

    def get_prvDiaSources_midpointMjdTai(self, alert):
        """Effective mid-visit time for this diaSource, expressed as Modified Julian Date, International
        Atomic Time [d].
        """
        return self._get_light_curve_values(alert, "prvDiaSources", "midpointMjdTai", np.float64)

    def get_prvDiaSources_mixedInstruments(self, alert):
        """Set if the template instrument is known and differs from the instrument that observed the
        diaSource.
        """
        return self._get_light_curve_values(alert, "prvDiaSources", "mixedInstruments", np.bool_)

    def get_prvDiaSources_observation_reason(self, alert):
        """Scheduler reason for the image containing this diaSource (RTN-097)."""
        return self._get_light_curve_values(
            alert, "prvDiaSources", "observation_reason", np.str_, null_value=""
        )

    def get_prvDiaSources_parentDiaSourceId(self, alert):
        """Id of the parent diaSource this diaSource has been deblended from, if any."""
        return self._get_light_curve_values(
            alert, "prvDiaSources", "parentDiaSourceId", np.int64, null_value=-1
        )

    def get_prvDiaSources_pixelFlags(self, alert):
        """General pixel flags failure; set if anything went wrong when setting pixels flags from this
        footprint's mask.
        """
        return self._get_light_curve_values(alert, "prvDiaSources", "pixelFlags", np.bool_, null_value=False)

    def get_prvDiaSources_pixelFlags_bad(self, alert):
        """Bad pixel in the DiaSource footprint."""
        return self._get_light_curve_values(
            alert, "prvDiaSources", "pixelFlags_bad", np.bool_, null_value=False
        )

    def get_prvDiaSources_pixelFlags_cr(self, alert):
        """Cosmic ray in the DiaSource footprint."""
        return self._get_light_curve_values(
            alert, "prvDiaSources", "pixelFlags_cr", np.bool_, null_value=False
        )

    def get_prvDiaSources_pixelFlags_crCenter(self, alert):
        """Cosmic ray in the 3x3 region around the centroid."""
        return self._get_light_curve_values(
            alert, "prvDiaSources", "pixelFlags_crCenter", np.bool_, null_value=False
        )

    def get_prvDiaSources_pixelFlags_edge(self, alert):
        """Some of the source footprint is outside usable exposure region (masked EDGE or centroid off
        image).
        """
        return self._get_light_curve_values(
            alert, "prvDiaSources", "pixelFlags_edge", np.bool_, null_value=False
        )

    def get_prvDiaSources_pixelFlags_injected(self, alert):
        """A pixel in the diaSource footprint contains at least one pixel modified by injecting fake
        sources.
        """
        return self._get_light_curve_values(
            alert, "prvDiaSources", "pixelFlags_injected", np.bool_, null_value=False
        )

    def get_prvDiaSources_pixelFlags_injected_template(self, alert):
        """A pixel in the diaSource footprint contains at least one pixel modified by injecting fake
        sources into the template.
        """
        return self._get_light_curve_values(
            alert, "prvDiaSources", "pixelFlags_injected_template", np.bool_, null_value=False
        )

    def get_prvDiaSources_pixelFlags_injected_templateCenter(self, alert):
        """A pixel in the 3x3 region around the centroid contains at least one pixel modified by injecting
        fake sources into the template.
        """
        return self._get_light_curve_values(
            alert, "prvDiaSources", "pixelFlags_injected_templateCenter", np.bool_, null_value=False
        )

    def get_prvDiaSources_pixelFlags_injectedCenter(self, alert):
        """A pixel in the 3x3 region around the centroid contains at least one pixel modified by injecting
        fake sources.
        """
        return self._get_light_curve_values(
            alert, "prvDiaSources", "pixelFlags_injectedCenter", np.bool_, null_value=False
        )

    def get_prvDiaSources_pixelFlags_interpolated(self, alert):
        """Interpolated pixel in the DiaSource footprint."""
        return self._get_light_curve_values(
            alert, "prvDiaSources", "pixelFlags_interpolated", np.bool_, null_value=False
        )

    def get_prvDiaSources_pixelFlags_interpolatedCenter(self, alert):
        """Interpolated pixel in the 3x3 region around the centroid."""
        return self._get_light_curve_values(
            alert, "prvDiaSources", "pixelFlags_interpolatedCenter", np.bool_, null_value=False
        )

    def get_prvDiaSources_pixelFlags_nodata(self, alert):
        """NO_DATA pixel in the source footprint."""
        return self._get_light_curve_values(
            alert, "prvDiaSources", "pixelFlags_nodata", np.bool_, null_value=False
        )

    def get_prvDiaSources_pixelFlags_nodataCenter(self, alert):
        """NO_DATA pixel in the 3x3 region around the centroid."""
        return self._get_light_curve_values(
            alert, "prvDiaSources", "pixelFlags_nodataCenter", np.bool_, null_value=False
        )

    def get_prvDiaSources_pixelFlags_offimage(self, alert):
        """DiaSource center is off image."""
        return self._get_light_curve_values(
            alert, "prvDiaSources", "pixelFlags_offimage", np.bool_, null_value=False
        )

    def get_prvDiaSources_pixelFlags_saturated(self, alert):
        """Saturated pixel in the DiaSource footprint."""
        return self._get_light_curve_values(
            alert, "prvDiaSources", "pixelFlags_saturated", np.bool_, null_value=False
        )

    def get_prvDiaSources_pixelFlags_saturatedCenter(self, alert):
        """Saturated pixel in the 3x3 region around the centroid."""
        return self._get_light_curve_values(
            alert, "prvDiaSources", "pixelFlags_saturatedCenter", np.bool_, null_value=False
        )

    def get_prvDiaSources_pixelFlags_spike(self, alert):
        """The diaSource footprint includes pixels on a predicted diffraction spike of a bright star."""
        return self._get_light_curve_values(
            alert, "prvDiaSources", "pixelFlags_spike", np.bool_, null_value=False
        )

    def get_prvDiaSources_pixelFlags_streak(self, alert):
        """Streak in the DiaSource footprint."""
        return self._get_light_curve_values(
            alert, "prvDiaSources", "pixelFlags_streak", np.bool_, null_value=False
        )

    def get_prvDiaSources_pixelFlags_streakCenter(self, alert):
        """Streak in the 3x3 region around the centroid."""
        return self._get_light_curve_values(
            alert, "prvDiaSources", "pixelFlags_streakCenter", np.bool_, null_value=False
        )

    def get_prvDiaSources_pixelFlags_suspect(self, alert):
        """DiaSource's footprint includes pixels masked as SUSPECT that are approaching the saturation
        threshold.
        """
        return self._get_light_curve_values(
            alert, "prvDiaSources", "pixelFlags_suspect", np.bool_, null_value=False
        )

    def get_prvDiaSources_pixelFlags_suspectCenter(self, alert):
        """The 3x3 region around the centroid of the DiaSource includes pixels masked as SUSPECT that are
        approaching the saturation threshold
        """
        return self._get_light_curve_values(
            alert, "prvDiaSources", "pixelFlags_suspectCenter", np.bool_, null_value=False
        )

    def get_prvDiaSources_psfChi2(self, alert):
        """Chi^2 statistic of the point source model fit."""
        return self._get_light_curve_values(alert, "prvDiaSources", "psfChi2", np.float32)

    def get_prvDiaSources_psfFlux(self, alert):
        """Flux for Point Source model."""
        return self._get_light_curve_values(alert, "prvDiaSources", "psfFlux", np.float32)

    def get_prvDiaSources_psfFlux_flag(self, alert):
        """Failure to derive linear least-squares fit of psf model."""
        return self._get_light_curve_values(
            alert, "prvDiaSources", "psfFlux_flag", np.bool_, null_value=False
        )

    def get_prvDiaSources_psfFlux_flag_edge(self, alert):
        """Object was too close to the edge of the image to use the full PSF model."""
        return self._get_light_curve_values(
            alert, "prvDiaSources", "psfFlux_flag_edge", np.bool_, null_value=False
        )

    def get_prvDiaSources_psfFlux_flag_noGoodPixels(self, alert):
        """Not enough non-rejected pixels in data to attempt the fit."""
        return self._get_light_curve_values(
            alert, "prvDiaSources", "psfFlux_flag_noGoodPixels", np.bool_, null_value=False
        )

    def get_prvDiaSources_psfFluxErr(self, alert):
        """Uncertainty of psfFlux [nJy]."""
        return self._get_light_curve_values(alert, "prvDiaSources", "psfFluxErr", np.float32)

    def get_prvDiaSources_psfLnL(self, alert):
        """Natural log likelihood of the observed data given the point source model."""
        return self._get_light_curve_values(alert, "prvDiaSources", "psfLnL", np.float32)

    def get_prvDiaSources_psfNdata(self, alert):
        """The number of data points (pixels) used to fit the point source model."""
        return self._get_light_curve_values(alert, "prvDiaSources", "psfNdata", np.int64)

    def get_prvDiaSources_ra(self, alert):
        """Right ascension coordinate of the center of this diaSource [deg]."""
        return self._get_light_curve_values(alert, "prvDiaSources", "ra", np.float64)

    def get_prvDiaSources_ra_dec_Cov(self, alert):
        """Covariance between Right Ascension (RA*cos(Dec)) and dec [deg**2]."""
        return self._get_light_curve_values(alert, "prvDiaSources", "ra_dec_Cov", np.float32)

    def get_prvDiaSources_raErr(self, alert):
        """Angular error of Right Ascension (dRA*cos(Dec)) [deg]."""
        return self._get_light_curve_values(alert, "prvDiaSources", "raErr", np.float32)

    def get_prvDiaSources_reliability(self, alert):
        """Probability (0-1) that the diaSource is astrophysical, derived from a machine learning model."""
        return self._get_light_curve_values(alert, "prvDiaSources", "reliability", np.float32)

    def get_prvDiaSources_reliabilityVersion(self, alert):
        """Version of the reliability model."""
        return self._get_light_curve_values(
            alert, "prvDiaSources", "reliabilityVersion", np.str_, null_value=""
        )

    def get_prvDiaSources_scienceFlux(self, alert):
        """Forced photometry flux for a point source model measured on the visit image centered at
        DiaSource position [nJy].
        """
        return self._get_light_curve_values(alert, "prvDiaSources", "scienceFlux", np.float32)

    def get_prvDiaSources_scienceFluxErr(self, alert):
        """Estimated uncertainty of scienceFlux [nJy]."""
        return self._get_light_curve_values(alert, "prvDiaSources", "scienceFluxErr", np.float32)

    def get_prvDiaSources_shape_flag(self, alert):
        """General source shape algorithm failure flag; set if anything went wrong when measuring the
        shape.
        """
        return self._get_light_curve_values(alert, "prvDiaSources", "shape_flag", np.bool_, null_value=False)

    def get_prvDiaSources_shape_flag_no_pixels(self, alert):
        """No pixels were useable for computing shape measurement."""
        return self._get_light_curve_values(
            alert, "prvDiaSources", "shape_flag_no_pixels", np.bool_, null_value=False
        )

    def get_prvDiaSources_shape_flag_not_contained(self, alert):
        """Center not contained in footprint bounding box."""
        return self._get_light_curve_values(
            alert, "prvDiaSources", "shape_flag_not_contained", np.bool_, null_value=False
        )

    def get_prvDiaSources_shape_flag_parent_source(self, alert):
        """This source is a parent source; we should only be measuring on deblended children in difference
        imaging.
        """
        return self._get_light_curve_values(
            alert, "prvDiaSources", "shape_flag_parent_source", np.bool_, null_value=False
        )

    def get_prvDiaSources_snr(self, alert):
        """The signal-to-noise ratio at which this source was detected in the difference image."""
        return self._get_light_curve_values(alert, "prvDiaSources", "snr", np.float32)

    def get_prvDiaSources_ssObjectId(self, alert):
        """Id of the ssObject this source was associated with, if any."""
        return self._get_light_curve_values(alert, "prvDiaSources", "ssObjectId", np.int64, null_value=-1)

    def get_prvDiaSources_target_name(self, alert):
        """Scheduler target for the image containing this diaSource (RTN-097)."""
        return self._get_light_curve_values(alert, "prvDiaSources", "target_name", np.str_, null_value="")

    def get_prvDiaSources_templateFlux(self, alert):
        """Forced photometry flux for a point source model measured on the template image centered at the
        DiaObject position [nJy].
        """
        return self._get_light_curve_values(alert, "prvDiaSources", "templateFlux", np.float32)

    def get_prvDiaSources_templateFluxErr(self, alert):
        """Uncertainty of templateFlux [nJy]."""
        return self._get_light_curve_values(alert, "prvDiaSources", "templateFluxErr", np.float32)

    def get_prvDiaSources_templateInstrument(self, alert):
        """Name of the instrument whose observations were coadded to make the template, or UNKNOWN if it is
        not known.
        """
        return self._get_light_curve_values(alert, "prvDiaSources", "templateInstrument", np.str_)

    def get_prvDiaSources_templateMjdMax(self, alert):
        """Latest mid-exposure time of the template input images that cover the diaSource centroid,
        expressed as Modified Julian Date, International Atomic Time [d].
        """
        return self._get_light_curve_values(alert, "prvDiaSources", "templateMjdMax", np.float64)

    def get_prvDiaSources_templateMjdMean(self, alert):
        """Weighted mean of the mid-exposure times of the template input images that cover the diaSource
        centroid, expressed as Modified Julian Date, International Atomic Time.
        """
        return self._get_light_curve_values(alert, "prvDiaSources", "templateMjdMean", np.float64)

    def get_prvDiaSources_templateMjdMin(self, alert):
        """Earliest mid-exposure time of the template input images that cover the diaSource centroid,
        expressed as Modified Julian Date, International Atomic Time [d].
        """
        return self._get_light_curve_values(alert, "prvDiaSources", "templateMjdMin", np.float64)

    def get_prvDiaSources_templateVersion(self, alert):
        """Version of the template."""
        return self._get_light_curve_values(alert, "prvDiaSources", "templateVersion", np.int64)

    def get_prvDiaSources_timeProcessedMjdTai(self, alert):
        """Time when the image was processed and this DiaSource record was generated, expressed as Modified
        Julian Date, International Atomic Time.
        """
        return self._get_light_curve_values(alert, "prvDiaSources", "timeProcessedMjdTai", np.float64)

    def get_prvDiaSources_timeWithdrawnMjdTai(self, alert):
        """Time when this record was marked invalid, expressed as Modified Julian Date, International
        Atomic Time.
        """
        return self._get_light_curve_values(alert, "prvDiaSources", "timeWithdrawnMjdTai", np.float64)

    def get_prvDiaSources_trail_flag(self, alert):
        """The trailed source fitting algorithm failed."""
        return self._get_light_curve_values(alert, "prvDiaSources", "trail_flag", np.bool_, null_value=False)

    def get_prvDiaSources_trail_flag_edge(self, alert):
        """This flag is set if a trailed source extends onto or past edge pixels."""
        return self._get_light_curve_values(
            alert, "prvDiaSources", "trail_flag_edge", np.bool_, null_value=False
        )

    def get_prvDiaSources_trailAlgorithm(self, alert):
        """Integer key indicating which algorithm was used to measure trailed source."""
        return self._get_light_curve_values(alert, "prvDiaSources", "trailAlgorithm", np.int64, null_value=-1)

    def get_prvDiaSources_trailAngle(self, alert):
        """Maximum likelihood fit of the angle between the meridian through the centroid and the trail
        direction (bearing) [deg].
        """
        return self._get_light_curve_values(alert, "prvDiaSources", "trailAngle", np.float32)

    def get_prvDiaSources_trailAngleErr(self, alert):
        """Uncertainty of trailAngle [deg]."""
        return self._get_light_curve_values(alert, "prvDiaSources", "trailAngleErr", np.float32)

    def get_prvDiaSources_trailChi2(self, alert):
        """Chi^2 statistic of the trailed source model fit."""
        return self._get_light_curve_values(alert, "prvDiaSources", "trailChi2", np.float32)

    def get_prvDiaSources_trailDec(self, alert):
        """Declination coordinate of centroid for trailed source model [deg]."""
        return self._get_light_curve_values(alert, "prvDiaSources", "trailDec", np.float64)

    def get_prvDiaSources_trailDecErr(self, alert):
        """Angular error of trailDec [deg]."""
        return self._get_light_curve_values(alert, "prvDiaSources", "trailDecErr", np.float32)

    def get_prvDiaSources_trailFlux(self, alert):
        """Flux for a trailed source model."""
        return self._get_light_curve_values(alert, "prvDiaSources", "trailFlux", np.float32)

    def get_prvDiaSources_trailFluxErr(self, alert):
        """Uncertainty of trailFlux [nJy]."""
        return self._get_light_curve_values(alert, "prvDiaSources", "trailFluxErr", np.float32)

    def get_prvDiaSources_trailLength(self, alert):
        """Maximum likelihood fit of trail length [arcsec]."""
        return self._get_light_curve_values(alert, "prvDiaSources", "trailLength", np.float32)

    def get_prvDiaSources_trailLengthErr(self, alert):
        """Uncertainty of trailLength [arcsec]."""
        return self._get_light_curve_values(alert, "prvDiaSources", "trailLengthErr", np.float32)

    def get_prvDiaSources_trailNdata(self, alert):
        """The number of data points (pixels) used to fit the trailed source model."""
        return self._get_light_curve_values(alert, "prvDiaSources", "trailNdata", np.int64)

    def get_prvDiaSources_trailRa(self, alert):
        """Right ascension coordinate of centroid for trailed source model [deg]."""
        return self._get_light_curve_values(alert, "prvDiaSources", "trailRa", np.float64)

    def get_prvDiaSources_trailRaErr(self, alert):
        """Angular error of trailRa (dRA*cos(Dec)) [deg]."""
        return self._get_light_curve_values(alert, "prvDiaSources", "trailRaErr", np.float32)

    def get_prvDiaSources_visit(self, alert):
        """Id of the visit where this diaSource was measured."""
        return self._get_light_curve_values(alert, "prvDiaSources", "visit", np.int64)

    def get_prvDiaSources_x(self, alert):
        """x position computed by a centroiding algorithm [pixel]."""
        return self._get_light_curve_values(alert, "prvDiaSources", "x", np.float32)

    def get_prvDiaSources_xErr(self, alert):
        """Uncertainty of x [pixel]."""
        return self._get_light_curve_values(alert, "prvDiaSources", "xErr", np.float32)

    def get_prvDiaSources_y(self, alert):
        """y position computed by a centroiding algorithm [pixel]."""
        return self._get_light_curve_values(alert, "prvDiaSources", "y", np.float32)

    def get_prvDiaSources_yErr(self, alert):
        """Uncertainty of y [pixel]."""
        return self._get_light_curve_values(alert, "prvDiaSources", "yErr", np.float32)

    # --- getters: ssSource ---
    def get_ssSource_designation(self, alert):
        """The unpacked primary provisional designation for this object."""
        return alert["ssSource"]["designation"]

    def get_ssSource_diaDistanceRank(self, alert):
        """The rank of the diaSourceId-identified source in terms of its closeness to the predicted SSO
        position.
        """
        return alert["ssSource"]["diaDistanceRank"]

    def get_ssSource_diaSourceId(self, alert):
        """Identifier of the triggering DiaSource."""
        return alert["ssSource"]["diaSourceId"]

    def get_ssSource_eclBeta(self, alert):
        """Ecliptic latitude, converted from the observed coordinates."""
        return alert["ssSource"]["eclBeta"]

    def get_ssSource_eclLambda(self, alert):
        """Ecliptic longitude, converted from the observed coordinates."""
        return alert["ssSource"]["eclLambda"]

    def get_ssSource_elongation(self, alert):
        """Solar elongation of the object at the time of observation."""
        return alert["ssSource"]["elongation"]

    def get_ssSource_ephDec(self, alert):
        """Predicted ICRS declination from the orbit in mpc_orbits."""
        return alert["ssSource"]["ephDec"]

    def get_ssSource_ephOffset(self, alert):
        """Total observed versus predicted angular separation on the sky."""
        return alert["ssSource"]["ephOffset"]

    def get_ssSource_ephOffsetAlongTrack(self, alert):
        """Offset between observed and predicted position in the along-track direction on the sky."""
        return alert["ssSource"]["ephOffsetAlongTrack"]

    def get_ssSource_ephOffsetCrossTrack(self, alert):
        """Offset between observed and predicted position in the cross-track direction on the sky."""
        return alert["ssSource"]["ephOffsetCrossTrack"]

    def get_ssSource_ephOffsetDec(self, alert):
        """Offset between observed and predicted position in declination."""
        return alert["ssSource"]["ephOffsetDec"]

    def get_ssSource_ephOffsetRa(self, alert):
        """Offset between observed and predicted position in the R.A."""
        return alert["ssSource"]["ephOffsetRa"]

    def get_ssSource_ephRa(self, alert):
        """Predicted ICRS right ascension from the orbit in mpc_orbits."""
        return alert["ssSource"]["ephRa"]

    def get_ssSource_ephRate(self, alert):
        """Total predicted on-sky angular rate of motion."""
        return alert["ssSource"]["ephRate"]

    def get_ssSource_ephRateDec(self, alert):
        """Predicted on-sky angular rate in the declination direction."""
        return alert["ssSource"]["ephRateDec"]

    def get_ssSource_ephRateRa(self, alert):
        """Predicted on-sky angular rate in the R.A."""
        return alert["ssSource"]["ephRateRa"]

    def get_ssSource_ephVmag(self, alert):
        """Predicted magnitude in V band, computed from mpc_orbits data including the mpc_orbits-provided
        (H, G) estimates
        """
        return alert["ssSource"]["ephVmag"]

    def get_ssSource_galLat(self, alert):
        """Galactic latitude, converted from the observed coordinates."""
        return alert["ssSource"]["galLat"]

    def get_ssSource_galLon(self, alert):
        """Galactic longitude, converted from the observed coordinates."""
        return alert["ssSource"]["galLon"]

    def get_ssSource_helio_vtot(self, alert):
        """The magnitude of the heliocentric velocity vector, sqrt(vx*vx + vy*vy + vz*vz)."""
        return alert["ssSource"]["helio_vtot"]

    def get_ssSource_helio_vx(self, alert):
        """Cartesian heliocentric X velocity at light-emission time (ICRS)."""
        return alert["ssSource"]["helio_vx"]

    def get_ssSource_helio_vy(self, alert):
        """Cartesian heliocentric Y velocity at light-emission time (ICRS)."""
        return alert["ssSource"]["helio_vy"]

    def get_ssSource_helio_vz(self, alert):
        """Cartesian heliocentric Z velocity at light-emission time (ICRS)."""
        return alert["ssSource"]["helio_vz"]

    def get_ssSource_helio_x(self, alert):
        """Cartesian heliocentric X coordinate at light-emission time (ICRS)."""
        return alert["ssSource"]["helio_x"]

    def get_ssSource_helio_y(self, alert):
        """Cartesian heliocentric Y coordinate at light-emission time (ICRS)."""
        return alert["ssSource"]["helio_y"]

    def get_ssSource_helio_z(self, alert):
        """Cartesian heliocentric Z coordinate at light-emission time (ICRS)."""
        return alert["ssSource"]["helio_z"]

    def get_ssSource_helioRange(self, alert):
        """Heliocentric distance (r) at light-emission time."""
        return alert["ssSource"]["helioRange"]

    def get_ssSource_helioRangeRate(self, alert):
        """Heliocentric radial velocity (rdot); positive values indicate motion away from the Sun."""
        return alert["ssSource"]["helioRangeRate"]

    def get_ssSource_phaseAngle(self, alert):
        """Phase angle between the Sun, object, and observer."""
        return alert["ssSource"]["phaseAngle"]

    def get_ssSource_ssObjectId(self, alert):
        """Id of the ssObject this source was associated with, if any."""
        return alert["ssSource"]["ssObjectId"]

    def get_ssSource_topo_vtot(self, alert):
        """The magnitude of the topocentric velocity vector, sqrt(vx*vx + vy*vy + vz*vz)."""
        return alert["ssSource"]["topo_vtot"]

    def get_ssSource_topo_vx(self, alert):
        """Cartesian topocentric X velocity at light-emission time (ICRS)."""
        return alert["ssSource"]["topo_vx"]

    def get_ssSource_topo_vy(self, alert):
        """Cartesian topocentric Y velocity at light-emission time (ICRS)."""
        return alert["ssSource"]["topo_vy"]

    def get_ssSource_topo_vz(self, alert):
        """Cartesian topocentric Z velocity at light-emission time (ICRS)."""
        return alert["ssSource"]["topo_vz"]

    def get_ssSource_topo_x(self, alert):
        """Cartesian topocentric X coordinate at light-emission time (ICRS)."""
        return alert["ssSource"]["topo_x"]

    def get_ssSource_topo_y(self, alert):
        """Cartesian topocentric Y coordinate at light-emission time (ICRS)."""
        return alert["ssSource"]["topo_y"]

    def get_ssSource_topo_z(self, alert):
        """Cartesian topocentric Z coordinate at light-emission time (ICRS)."""
        return alert["ssSource"]["topo_z"]

    def get_ssSource_topoRange(self, alert):
        """Topocentric distance (delta) at light-emission time."""
        return alert["ssSource"]["topoRange"]

    def get_ssSource_topoRangeRate(self, alert):
        """Topocentric radial (line-of-sight) velocity (deldot); positive values indicate motion away from
        the observer.
        """
        return alert["ssSource"]["topoRangeRate"]

    # --- getters: xm ---
    def get_xm_gaiadr3_DR3Name(self, alert):
        """Unique source designation of closest source from Gaia catalog; if exists within 1 arcsec."""
        return alert["xm"]["gaiadr3_DR3Name"]

    def get_xm_gaiadr3_e_Plx(self, alert):
        """Standard error of the stellar parallax (in milli-arcsecond) of the closest source from Gaia
        catalog; if exists within 1 arcsec.
        """
        return alert["xm"]["gaiadr3_e_Plx"]

    def get_xm_gaiadr3_Plx(self, alert):
        """Absolute stellar parallax (in milli-arcsecond) of the closest source from Gaia catalog; if
        exists within 1 arcsec.
        """
        return alert["xm"]["gaiadr3_Plx"]

    def get_xm_gaiadr3_VarFlag(self, alert):
        """Photometric variability flag from Gaia DR3."""
        return alert["xm"]["gaiadr3_VarFlag"]

    def get_xm_gcvs_type(self, alert):
        """Object type of the closest source from GCVS catalog; if exists within 1 arcsec."""
        return alert["xm"]["gcvs_type"]

    def get_xm_legacydr8_e_zphot(self, alert):
        """Uncertainty on zphot from Legacy Surveys DR8 South Photometric Redshifts catalog - standard
        deviation of the normally distributed photo-z posterior.
        """
        return alert["xm"]["legacydr8_e_zphot"]

    def get_xm_legacydr8_fqual(self, alert):
        """Photo-z reliability flag from Legacy Surveys DR8 South Photometric Redshifts catalog."""
        return alert["xm"]["legacydr8_fqual"]

    def get_xm_legacydr8_pstar(self, alert):
        """Star likelihood based on colours from GMM star-QSO classification (Legacy Surveys DR8 South
        Photometric Redshifts catalog)
        """
        return alert["xm"]["legacydr8_pstar"]

    def get_xm_legacydr8_zphot(self, alert):
        """Photo-z estimate from Legacy Surveys DR8 South Photometric Redshifts catalog - mean of the
        normally distributed photo-z posterior
        """
        return alert["xm"]["legacydr8_zphot"]

    def get_xm_mangrove_2MASS_name(self, alert):
        """2MASS source designation of closest source from Mangrove catalog; if exists within 1 arcmin."""
        return alert["xm"]["mangrove_2MASS_name"]

    def get_xm_mangrove_ang_dist(self, alert):
        """Angular distance of closest source from Mangrove catalog; if exists within 1 arcmin."""
        return alert["xm"]["mangrove_ang_dist"]

    def get_xm_mangrove_HyperLEDA_name(self, alert):
        """HyperLEDA source designation of closest source from Mangrove catalog; if exists within 1 arcmin."""
        return alert["xm"]["mangrove_HyperLEDA_name"]

    def get_xm_mangrove_lum_dist(self, alert):
        """Luminosity distance of closest source from Mangrove catalog; if exists within 1 arcmin."""
        return alert["xm"]["mangrove_lum_dist"]

    def get_xm_simbad_otype(self, alert):
        """Object type of the closest source from SIMBAD database; if exists within 1 arcsec."""
        return alert["xm"]["simbad_otype"]

    def get_xm_spicy_class(self, alert):
        """Class name of closest source from SPICY catalog; if exists within 1.2 arcsec."""
        return alert["xm"]["spicy_class"]

    def get_xm_tns_fullname(self, alert):
        """TNS name, if it exists."""
        return alert["xm"]["tns_fullname"]

    def get_xm_tns_redshift(self, alert):
        """Redshift from TNS, if it exists."""
        return alert["xm"]["tns_redshift"]

    def get_xm_tns_type(self, alert):
        """TNS label, if it exists."""
        return alert["xm"]["tns_type"]

    def get_xm_vsx_Type(self, alert):
        """Object type of the closest source from VSX catalog; if exists within 1 arcsec."""
        return alert["xm"]["vsx_Type"]

    def get_xm_x3hsp_type(self, alert):
        """Counterpart (cross-match) to the 3HSP catalog if exists within 1 arcminute."""
        return alert["xm"]["x3hsp_type"]

    def get_xm_x4lac_type(self, alert):
        """Counterpart (cross-match) to the 4LAC DR3 catalog if exists within 1 arcminute."""
        return alert["xm"]["x4lac_type"]

    # ==================== Collation methods ====================

    # --- collate: prvDiaForcedSources ---
    def collate_prvDiaForcedSources_band(self, samples):
        """Collate the prvDiaForcedSources band light curves."""
        return self._collate_padded(samples, "prvDiaForcedSources_band", np.str_)

    def collate_prvDiaForcedSources_dec(self, samples):
        """Collate the prvDiaForcedSources dec light curves."""
        return self._collate_padded(samples, "prvDiaForcedSources_dec", np.float64)

    def collate_prvDiaForcedSources_detector(self, samples):
        """Collate the prvDiaForcedSources detector light curves."""
        return self._collate_padded(samples, "prvDiaForcedSources_detector", np.int64)

    def collate_prvDiaForcedSources_diaForcedSourceId(self, samples):
        """Collate the prvDiaForcedSources diaForcedSourceId light curves."""
        return self._collate_padded(samples, "prvDiaForcedSources_diaForcedSourceId", np.int64)

    def collate_prvDiaForcedSources_diaObjectId(self, samples):
        """Collate the prvDiaForcedSources diaObjectId light curves."""
        return self._collate_padded(samples, "prvDiaForcedSources_diaObjectId", np.int64)

    def collate_prvDiaForcedSources_midpointMjdTai(self, samples):
        """Collate the prvDiaForcedSources midpointMjdTai light curves."""
        return self._collate_padded(samples, "prvDiaForcedSources_midpointMjdTai", np.float64)

    def collate_prvDiaForcedSources_psfFlux(self, samples):
        """Collate the prvDiaForcedSources psfFlux light curves."""
        return collate_as_1d_light_curve(samples, "prvDiaForcedSources_psfFlux")

    def collate_prvDiaForcedSources_psfFluxErr(self, samples):
        """Collate the prvDiaForcedSources psfFluxErr light curves."""
        return collate_as_1d_light_curve(samples, "prvDiaForcedSources_psfFluxErr")

    def collate_prvDiaForcedSources_ra(self, samples):
        """Collate the prvDiaForcedSources ra light curves."""
        return self._collate_padded(samples, "prvDiaForcedSources_ra", np.float64)

    def collate_prvDiaForcedSources_scienceFlux(self, samples):
        """Collate the prvDiaForcedSources scienceFlux light curves."""
        return collate_as_1d_light_curve(samples, "prvDiaForcedSources_scienceFlux")

    def collate_prvDiaForcedSources_scienceFluxErr(self, samples):
        """Collate the prvDiaForcedSources scienceFluxErr light curves."""
        return collate_as_1d_light_curve(samples, "prvDiaForcedSources_scienceFluxErr")

    def collate_prvDiaForcedSources_timeProcessedMjdTai(self, samples):
        """Collate the prvDiaForcedSources timeProcessedMjdTai light curves."""
        return self._collate_padded(samples, "prvDiaForcedSources_timeProcessedMjdTai", np.float64)

    def collate_prvDiaForcedSources_timeWithdrawnMjdTai(self, samples):
        """Collate the prvDiaForcedSources timeWithdrawnMjdTai light curves."""
        return self._collate_padded(samples, "prvDiaForcedSources_timeWithdrawnMjdTai", np.float64)

    def collate_prvDiaForcedSources_visit(self, samples):
        """Collate the prvDiaForcedSources visit light curves."""
        return self._collate_padded(samples, "prvDiaForcedSources_visit", np.int64)

    # --- collate: prvDiaSources ---
    def collate_prvDiaSources_apFlux(self, samples):
        """Collate the prvDiaSources apFlux light curves."""
        return collate_as_1d_light_curve(samples, "prvDiaSources_apFlux")

    def collate_prvDiaSources_apFlux_flag(self, samples):
        """Collate the prvDiaSources apFlux_flag light curves."""
        return self._collate_padded(samples, "prvDiaSources_apFlux_flag", np.bool_)

    def collate_prvDiaSources_apFlux_flag_apertureTruncated(self, samples):
        """Collate the prvDiaSources apFlux_flag_apertureTruncated light curves."""
        return self._collate_padded(samples, "prvDiaSources_apFlux_flag_apertureTruncated", np.bool_)

    def collate_prvDiaSources_apFluxErr(self, samples):
        """Collate the prvDiaSources apFluxErr light curves."""
        return collate_as_1d_light_curve(samples, "prvDiaSources_apFluxErr")

    def collate_prvDiaSources_band(self, samples):
        """Collate the prvDiaSources band light curves."""
        return self._collate_padded(samples, "prvDiaSources_band", np.str_)

    def collate_prvDiaSources_bboxSize(self, samples):
        """Collate the prvDiaSources bboxSize light curves."""
        return self._collate_padded(samples, "prvDiaSources_bboxSize", np.int64)

    def collate_prvDiaSources_centroid_flag(self, samples):
        """Collate the prvDiaSources centroid_flag light curves."""
        return self._collate_padded(samples, "prvDiaSources_centroid_flag", np.bool_)

    def collate_prvDiaSources_dec(self, samples):
        """Collate the prvDiaSources dec light curves."""
        return self._collate_padded(samples, "prvDiaSources_dec", np.float64)

    def collate_prvDiaSources_decErr(self, samples):
        """Collate the prvDiaSources decErr light curves."""
        return collate_as_1d_light_curve(samples, "prvDiaSources_decErr")

    def collate_prvDiaSources_detector(self, samples):
        """Collate the prvDiaSources detector light curves."""
        return self._collate_padded(samples, "prvDiaSources_detector", np.int64)

    def collate_prvDiaSources_diaObjectId(self, samples):
        """Collate the prvDiaSources diaObjectId light curves."""
        return self._collate_padded(samples, "prvDiaSources_diaObjectId", np.int64)

    def collate_prvDiaSources_diaSourceId(self, samples):
        """Collate the prvDiaSources diaSourceId light curves."""
        return self._collate_padded(samples, "prvDiaSources_diaSourceId", np.int64)

    def collate_prvDiaSources_dipoleAngle(self, samples):
        """Collate the prvDiaSources dipoleAngle light curves."""
        return collate_as_1d_light_curve(samples, "prvDiaSources_dipoleAngle")

    def collate_prvDiaSources_dipoleChi2(self, samples):
        """Collate the prvDiaSources dipoleChi2 light curves."""
        return collate_as_1d_light_curve(samples, "prvDiaSources_dipoleChi2")

    def collate_prvDiaSources_dipoleFitAttempted(self, samples):
        """Collate the prvDiaSources dipoleFitAttempted light curves."""
        return self._collate_padded(samples, "prvDiaSources_dipoleFitAttempted", np.bool_)

    def collate_prvDiaSources_dipoleFluxDiff(self, samples):
        """Collate the prvDiaSources dipoleFluxDiff light curves."""
        return collate_as_1d_light_curve(samples, "prvDiaSources_dipoleFluxDiff")

    def collate_prvDiaSources_dipoleFluxDiffErr(self, samples):
        """Collate the prvDiaSources dipoleFluxDiffErr light curves."""
        return collate_as_1d_light_curve(samples, "prvDiaSources_dipoleFluxDiffErr")

    def collate_prvDiaSources_dipoleLength(self, samples):
        """Collate the prvDiaSources dipoleLength light curves."""
        return collate_as_1d_light_curve(samples, "prvDiaSources_dipoleLength")

    def collate_prvDiaSources_dipoleMeanFlux(self, samples):
        """Collate the prvDiaSources dipoleMeanFlux light curves."""
        return collate_as_1d_light_curve(samples, "prvDiaSources_dipoleMeanFlux")

    def collate_prvDiaSources_dipoleMeanFluxErr(self, samples):
        """Collate the prvDiaSources dipoleMeanFluxErr light curves."""
        return collate_as_1d_light_curve(samples, "prvDiaSources_dipoleMeanFluxErr")

    def collate_prvDiaSources_dipoleNdata(self, samples):
        """Collate the prvDiaSources dipoleNdata light curves."""
        return self._collate_padded(samples, "prvDiaSources_dipoleNdata", np.int64)

    def collate_prvDiaSources_exposureTime(self, samples):
        """Collate the prvDiaSources exposureTime light curves."""
        return collate_as_1d_light_curve(samples, "prvDiaSources_exposureTime")

    def collate_prvDiaSources_extendedness(self, samples):
        """Collate the prvDiaSources extendedness light curves."""
        return collate_as_1d_light_curve(samples, "prvDiaSources_extendedness")

    def collate_prvDiaSources_forced_PsfFlux_flag(self, samples):
        """Collate the prvDiaSources forced_PsfFlux_flag light curves."""
        return self._collate_padded(samples, "prvDiaSources_forced_PsfFlux_flag", np.bool_)

    def collate_prvDiaSources_forced_PsfFlux_flag_edge(self, samples):
        """Collate the prvDiaSources forced_PsfFlux_flag_edge light curves."""
        return self._collate_padded(samples, "prvDiaSources_forced_PsfFlux_flag_edge", np.bool_)

    def collate_prvDiaSources_forced_PsfFlux_flag_noGoodPixels(self, samples):
        """Collate the prvDiaSources forced_PsfFlux_flag_noGoodPixels light curves."""
        return self._collate_padded(samples, "prvDiaSources_forced_PsfFlux_flag_noGoodPixels", np.bool_)

    def collate_prvDiaSources_glint_trail(self, samples):
        """Collate the prvDiaSources glint_trail light curves."""
        return self._collate_padded(samples, "prvDiaSources_glint_trail", np.bool_)

    def collate_prvDiaSources_isDipole(self, samples):
        """Collate the prvDiaSources isDipole light curves."""
        return self._collate_padded(samples, "prvDiaSources_isDipole", np.bool_)

    def collate_prvDiaSources_isNegative(self, samples):
        """Collate the prvDiaSources isNegative light curves."""
        return self._collate_padded(samples, "prvDiaSources_isNegative", np.bool_)

    def collate_prvDiaSources_ixx(self, samples):
        """Collate the prvDiaSources ixx light curves."""
        return collate_as_1d_light_curve(samples, "prvDiaSources_ixx")

    def collate_prvDiaSources_ixxPSF(self, samples):
        """Collate the prvDiaSources ixxPSF light curves."""
        return collate_as_1d_light_curve(samples, "prvDiaSources_ixxPSF")

    def collate_prvDiaSources_ixy(self, samples):
        """Collate the prvDiaSources ixy light curves."""
        return collate_as_1d_light_curve(samples, "prvDiaSources_ixy")

    def collate_prvDiaSources_ixyPSF(self, samples):
        """Collate the prvDiaSources ixyPSF light curves."""
        return collate_as_1d_light_curve(samples, "prvDiaSources_ixyPSF")

    def collate_prvDiaSources_iyy(self, samples):
        """Collate the prvDiaSources iyy light curves."""
        return collate_as_1d_light_curve(samples, "prvDiaSources_iyy")

    def collate_prvDiaSources_iyyPSF(self, samples):
        """Collate the prvDiaSources iyyPSF light curves."""
        return collate_as_1d_light_curve(samples, "prvDiaSources_iyyPSF")

    def collate_prvDiaSources_midpointMjdTai(self, samples):
        """Collate the prvDiaSources midpointMjdTai light curves."""
        return self._collate_padded(samples, "prvDiaSources_midpointMjdTai", np.float64)

    def collate_prvDiaSources_mixedInstruments(self, samples):
        """Collate the prvDiaSources mixedInstruments light curves."""
        return self._collate_padded(samples, "prvDiaSources_mixedInstruments", np.bool_)

    def collate_prvDiaSources_observation_reason(self, samples):
        """Collate the prvDiaSources observation_reason light curves."""
        return self._collate_padded(samples, "prvDiaSources_observation_reason", np.str_)

    def collate_prvDiaSources_parentDiaSourceId(self, samples):
        """Collate the prvDiaSources parentDiaSourceId light curves."""
        return self._collate_padded(samples, "prvDiaSources_parentDiaSourceId", np.int64)

    def collate_prvDiaSources_pixelFlags(self, samples):
        """Collate the prvDiaSources pixelFlags light curves."""
        return self._collate_padded(samples, "prvDiaSources_pixelFlags", np.bool_)

    def collate_prvDiaSources_pixelFlags_bad(self, samples):
        """Collate the prvDiaSources pixelFlags_bad light curves."""
        return self._collate_padded(samples, "prvDiaSources_pixelFlags_bad", np.bool_)

    def collate_prvDiaSources_pixelFlags_cr(self, samples):
        """Collate the prvDiaSources pixelFlags_cr light curves."""
        return self._collate_padded(samples, "prvDiaSources_pixelFlags_cr", np.bool_)

    def collate_prvDiaSources_pixelFlags_crCenter(self, samples):
        """Collate the prvDiaSources pixelFlags_crCenter light curves."""
        return self._collate_padded(samples, "prvDiaSources_pixelFlags_crCenter", np.bool_)

    def collate_prvDiaSources_pixelFlags_edge(self, samples):
        """Collate the prvDiaSources pixelFlags_edge light curves."""
        return self._collate_padded(samples, "prvDiaSources_pixelFlags_edge", np.bool_)

    def collate_prvDiaSources_pixelFlags_injected(self, samples):
        """Collate the prvDiaSources pixelFlags_injected light curves."""
        return self._collate_padded(samples, "prvDiaSources_pixelFlags_injected", np.bool_)

    def collate_prvDiaSources_pixelFlags_injected_template(self, samples):
        """Collate the prvDiaSources pixelFlags_injected_template light curves."""
        return self._collate_padded(samples, "prvDiaSources_pixelFlags_injected_template", np.bool_)

    def collate_prvDiaSources_pixelFlags_injected_templateCenter(self, samples):
        """Collate the prvDiaSources pixelFlags_injected_templateCenter light curves."""
        return self._collate_padded(samples, "prvDiaSources_pixelFlags_injected_templateCenter", np.bool_)

    def collate_prvDiaSources_pixelFlags_injectedCenter(self, samples):
        """Collate the prvDiaSources pixelFlags_injectedCenter light curves."""
        return self._collate_padded(samples, "prvDiaSources_pixelFlags_injectedCenter", np.bool_)

    def collate_prvDiaSources_pixelFlags_interpolated(self, samples):
        """Collate the prvDiaSources pixelFlags_interpolated light curves."""
        return self._collate_padded(samples, "prvDiaSources_pixelFlags_interpolated", np.bool_)

    def collate_prvDiaSources_pixelFlags_interpolatedCenter(self, samples):
        """Collate the prvDiaSources pixelFlags_interpolatedCenter light curves."""
        return self._collate_padded(samples, "prvDiaSources_pixelFlags_interpolatedCenter", np.bool_)

    def collate_prvDiaSources_pixelFlags_nodata(self, samples):
        """Collate the prvDiaSources pixelFlags_nodata light curves."""
        return self._collate_padded(samples, "prvDiaSources_pixelFlags_nodata", np.bool_)

    def collate_prvDiaSources_pixelFlags_nodataCenter(self, samples):
        """Collate the prvDiaSources pixelFlags_nodataCenter light curves."""
        return self._collate_padded(samples, "prvDiaSources_pixelFlags_nodataCenter", np.bool_)

    def collate_prvDiaSources_pixelFlags_offimage(self, samples):
        """Collate the prvDiaSources pixelFlags_offimage light curves."""
        return self._collate_padded(samples, "prvDiaSources_pixelFlags_offimage", np.bool_)

    def collate_prvDiaSources_pixelFlags_saturated(self, samples):
        """Collate the prvDiaSources pixelFlags_saturated light curves."""
        return self._collate_padded(samples, "prvDiaSources_pixelFlags_saturated", np.bool_)

    def collate_prvDiaSources_pixelFlags_saturatedCenter(self, samples):
        """Collate the prvDiaSources pixelFlags_saturatedCenter light curves."""
        return self._collate_padded(samples, "prvDiaSources_pixelFlags_saturatedCenter", np.bool_)

    def collate_prvDiaSources_pixelFlags_spike(self, samples):
        """Collate the prvDiaSources pixelFlags_spike light curves."""
        return self._collate_padded(samples, "prvDiaSources_pixelFlags_spike", np.bool_)

    def collate_prvDiaSources_pixelFlags_streak(self, samples):
        """Collate the prvDiaSources pixelFlags_streak light curves."""
        return self._collate_padded(samples, "prvDiaSources_pixelFlags_streak", np.bool_)

    def collate_prvDiaSources_pixelFlags_streakCenter(self, samples):
        """Collate the prvDiaSources pixelFlags_streakCenter light curves."""
        return self._collate_padded(samples, "prvDiaSources_pixelFlags_streakCenter", np.bool_)

    def collate_prvDiaSources_pixelFlags_suspect(self, samples):
        """Collate the prvDiaSources pixelFlags_suspect light curves."""
        return self._collate_padded(samples, "prvDiaSources_pixelFlags_suspect", np.bool_)

    def collate_prvDiaSources_pixelFlags_suspectCenter(self, samples):
        """Collate the prvDiaSources pixelFlags_suspectCenter light curves."""
        return self._collate_padded(samples, "prvDiaSources_pixelFlags_suspectCenter", np.bool_)

    def collate_prvDiaSources_psfChi2(self, samples):
        """Collate the prvDiaSources psfChi2 light curves."""
        return collate_as_1d_light_curve(samples, "prvDiaSources_psfChi2")

    def collate_prvDiaSources_psfFlux(self, samples):
        """Collate the prvDiaSources psfFlux light curves."""
        return collate_as_1d_light_curve(samples, "prvDiaSources_psfFlux")

    def collate_prvDiaSources_psfFlux_flag(self, samples):
        """Collate the prvDiaSources psfFlux_flag light curves."""
        return self._collate_padded(samples, "prvDiaSources_psfFlux_flag", np.bool_)

    def collate_prvDiaSources_psfFlux_flag_edge(self, samples):
        """Collate the prvDiaSources psfFlux_flag_edge light curves."""
        return self._collate_padded(samples, "prvDiaSources_psfFlux_flag_edge", np.bool_)

    def collate_prvDiaSources_psfFlux_flag_noGoodPixels(self, samples):
        """Collate the prvDiaSources psfFlux_flag_noGoodPixels light curves."""
        return self._collate_padded(samples, "prvDiaSources_psfFlux_flag_noGoodPixels", np.bool_)

    def collate_prvDiaSources_psfFluxErr(self, samples):
        """Collate the prvDiaSources psfFluxErr light curves."""
        return collate_as_1d_light_curve(samples, "prvDiaSources_psfFluxErr")

    def collate_prvDiaSources_psfLnL(self, samples):
        """Collate the prvDiaSources psfLnL light curves."""
        return collate_as_1d_light_curve(samples, "prvDiaSources_psfLnL")

    def collate_prvDiaSources_psfNdata(self, samples):
        """Collate the prvDiaSources psfNdata light curves."""
        return self._collate_padded(samples, "prvDiaSources_psfNdata", np.int64)

    def collate_prvDiaSources_ra(self, samples):
        """Collate the prvDiaSources ra light curves."""
        return self._collate_padded(samples, "prvDiaSources_ra", np.float64)

    def collate_prvDiaSources_ra_dec_Cov(self, samples):
        """Collate the prvDiaSources ra_dec_Cov light curves."""
        return collate_as_1d_light_curve(samples, "prvDiaSources_ra_dec_Cov")

    def collate_prvDiaSources_raErr(self, samples):
        """Collate the prvDiaSources raErr light curves."""
        return collate_as_1d_light_curve(samples, "prvDiaSources_raErr")

    def collate_prvDiaSources_reliability(self, samples):
        """Collate the prvDiaSources reliability light curves."""
        return collate_as_1d_light_curve(samples, "prvDiaSources_reliability")

    def collate_prvDiaSources_reliabilityVersion(self, samples):
        """Collate the prvDiaSources reliabilityVersion light curves."""
        return self._collate_padded(samples, "prvDiaSources_reliabilityVersion", np.str_)

    def collate_prvDiaSources_scienceFlux(self, samples):
        """Collate the prvDiaSources scienceFlux light curves."""
        return collate_as_1d_light_curve(samples, "prvDiaSources_scienceFlux")

    def collate_prvDiaSources_scienceFluxErr(self, samples):
        """Collate the prvDiaSources scienceFluxErr light curves."""
        return collate_as_1d_light_curve(samples, "prvDiaSources_scienceFluxErr")

    def collate_prvDiaSources_shape_flag(self, samples):
        """Collate the prvDiaSources shape_flag light curves."""
        return self._collate_padded(samples, "prvDiaSources_shape_flag", np.bool_)

    def collate_prvDiaSources_shape_flag_no_pixels(self, samples):
        """Collate the prvDiaSources shape_flag_no_pixels light curves."""
        return self._collate_padded(samples, "prvDiaSources_shape_flag_no_pixels", np.bool_)

    def collate_prvDiaSources_shape_flag_not_contained(self, samples):
        """Collate the prvDiaSources shape_flag_not_contained light curves."""
        return self._collate_padded(samples, "prvDiaSources_shape_flag_not_contained", np.bool_)

    def collate_prvDiaSources_shape_flag_parent_source(self, samples):
        """Collate the prvDiaSources shape_flag_parent_source light curves."""
        return self._collate_padded(samples, "prvDiaSources_shape_flag_parent_source", np.bool_)

    def collate_prvDiaSources_snr(self, samples):
        """Collate the prvDiaSources snr light curves."""
        return collate_as_1d_light_curve(samples, "prvDiaSources_snr")

    def collate_prvDiaSources_ssObjectId(self, samples):
        """Collate the prvDiaSources ssObjectId light curves."""
        return self._collate_padded(samples, "prvDiaSources_ssObjectId", np.int64)

    def collate_prvDiaSources_target_name(self, samples):
        """Collate the prvDiaSources target_name light curves."""
        return self._collate_padded(samples, "prvDiaSources_target_name", np.str_)

    def collate_prvDiaSources_templateFlux(self, samples):
        """Collate the prvDiaSources templateFlux light curves."""
        return collate_as_1d_light_curve(samples, "prvDiaSources_templateFlux")

    def collate_prvDiaSources_templateFluxErr(self, samples):
        """Collate the prvDiaSources templateFluxErr light curves."""
        return collate_as_1d_light_curve(samples, "prvDiaSources_templateFluxErr")

    def collate_prvDiaSources_templateInstrument(self, samples):
        """Collate the prvDiaSources templateInstrument light curves."""
        return self._collate_padded(samples, "prvDiaSources_templateInstrument", np.str_)

    def collate_prvDiaSources_templateMjdMax(self, samples):
        """Collate the prvDiaSources templateMjdMax light curves."""
        return self._collate_padded(samples, "prvDiaSources_templateMjdMax", np.float64)

    def collate_prvDiaSources_templateMjdMean(self, samples):
        """Collate the prvDiaSources templateMjdMean light curves."""
        return self._collate_padded(samples, "prvDiaSources_templateMjdMean", np.float64)

    def collate_prvDiaSources_templateMjdMin(self, samples):
        """Collate the prvDiaSources templateMjdMin light curves."""
        return self._collate_padded(samples, "prvDiaSources_templateMjdMin", np.float64)

    def collate_prvDiaSources_templateVersion(self, samples):
        """Collate the prvDiaSources templateVersion light curves."""
        return self._collate_padded(samples, "prvDiaSources_templateVersion", np.int64)

    def collate_prvDiaSources_timeProcessedMjdTai(self, samples):
        """Collate the prvDiaSources timeProcessedMjdTai light curves."""
        return self._collate_padded(samples, "prvDiaSources_timeProcessedMjdTai", np.float64)

    def collate_prvDiaSources_timeWithdrawnMjdTai(self, samples):
        """Collate the prvDiaSources timeWithdrawnMjdTai light curves."""
        return self._collate_padded(samples, "prvDiaSources_timeWithdrawnMjdTai", np.float64)

    def collate_prvDiaSources_trail_flag(self, samples):
        """Collate the prvDiaSources trail_flag light curves."""
        return self._collate_padded(samples, "prvDiaSources_trail_flag", np.bool_)

    def collate_prvDiaSources_trail_flag_edge(self, samples):
        """Collate the prvDiaSources trail_flag_edge light curves."""
        return self._collate_padded(samples, "prvDiaSources_trail_flag_edge", np.bool_)

    def collate_prvDiaSources_trailAlgorithm(self, samples):
        """Collate the prvDiaSources trailAlgorithm light curves."""
        return self._collate_padded(samples, "prvDiaSources_trailAlgorithm", np.int64)

    def collate_prvDiaSources_trailAngle(self, samples):
        """Collate the prvDiaSources trailAngle light curves."""
        return collate_as_1d_light_curve(samples, "prvDiaSources_trailAngle")

    def collate_prvDiaSources_trailAngleErr(self, samples):
        """Collate the prvDiaSources trailAngleErr light curves."""
        return collate_as_1d_light_curve(samples, "prvDiaSources_trailAngleErr")

    def collate_prvDiaSources_trailChi2(self, samples):
        """Collate the prvDiaSources trailChi2 light curves."""
        return collate_as_1d_light_curve(samples, "prvDiaSources_trailChi2")

    def collate_prvDiaSources_trailDec(self, samples):
        """Collate the prvDiaSources trailDec light curves."""
        return self._collate_padded(samples, "prvDiaSources_trailDec", np.float64)

    def collate_prvDiaSources_trailDecErr(self, samples):
        """Collate the prvDiaSources trailDecErr light curves."""
        return collate_as_1d_light_curve(samples, "prvDiaSources_trailDecErr")

    def collate_prvDiaSources_trailFlux(self, samples):
        """Collate the prvDiaSources trailFlux light curves."""
        return collate_as_1d_light_curve(samples, "prvDiaSources_trailFlux")

    def collate_prvDiaSources_trailFluxErr(self, samples):
        """Collate the prvDiaSources trailFluxErr light curves."""
        return collate_as_1d_light_curve(samples, "prvDiaSources_trailFluxErr")

    def collate_prvDiaSources_trailLength(self, samples):
        """Collate the prvDiaSources trailLength light curves."""
        return collate_as_1d_light_curve(samples, "prvDiaSources_trailLength")

    def collate_prvDiaSources_trailLengthErr(self, samples):
        """Collate the prvDiaSources trailLengthErr light curves."""
        return collate_as_1d_light_curve(samples, "prvDiaSources_trailLengthErr")

    def collate_prvDiaSources_trailNdata(self, samples):
        """Collate the prvDiaSources trailNdata light curves."""
        return self._collate_padded(samples, "prvDiaSources_trailNdata", np.int64)

    def collate_prvDiaSources_trailRa(self, samples):
        """Collate the prvDiaSources trailRa light curves."""
        return self._collate_padded(samples, "prvDiaSources_trailRa", np.float64)

    def collate_prvDiaSources_trailRaErr(self, samples):
        """Collate the prvDiaSources trailRaErr light curves."""
        return collate_as_1d_light_curve(samples, "prvDiaSources_trailRaErr")

    def collate_prvDiaSources_visit(self, samples):
        """Collate the prvDiaSources visit light curves."""
        return self._collate_padded(samples, "prvDiaSources_visit", np.int64)

    def collate_prvDiaSources_x(self, samples):
        """Collate the prvDiaSources x light curves."""
        return collate_as_1d_light_curve(samples, "prvDiaSources_x")

    def collate_prvDiaSources_xErr(self, samples):
        """Collate the prvDiaSources xErr light curves."""
        return collate_as_1d_light_curve(samples, "prvDiaSources_xErr")

    def collate_prvDiaSources_y(self, samples):
        """Collate the prvDiaSources y light curves."""
        return collate_as_1d_light_curve(samples, "prvDiaSources_y")

    def collate_prvDiaSources_yErr(self, samples):
        """Collate the prvDiaSources yErr light curves."""
        return collate_as_1d_light_curve(samples, "prvDiaSources_yErr")
