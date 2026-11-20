# ruff: noqa: N802, N806

import numpy as np
from hyrax.datasets.collate_utils import collate_as_1d_light_curve

from hyrax_alerts.logging_utils import get_logger

from .kafka_consumer import HyraxKafkaConsumer

logger = get_logger(__name__)


class LasairLiteConsumer(HyraxKafkaConsumer):
    """A consumer for working with Lasair messages, specifically supporting those
    messages that include lite light curve data as `diaSourcesList` and `diaForcedSourcesList`.

    No guarantee is made that the getters in this consumer will continue to match
    the schema defined by Lasair. If schema keys are missing from this consumer,
    you should inherit from this consumer and implement the missing getters as needed.

    The schema as defined by Lasair can be seen here:
    https://lasair.lsst.ac.uk/schema/

    The primary ID field for Lasair messages is typically 'diaObjectId'.
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

    # Getters for the `objects` table, in the order listed at https://lasair.lsst.ac.uk/schema/

    # --- Basic information ---
    def get_diaObjectId(self, sample):
        """ID for this object."""
        return sample.get("diaObjectId")

    def get_ra(self, sample):
        """Mean RA of this object."""
        return sample.get("ra")

    def get_decl(self, sample):
        """Mean Dec of this object."""
        return sample.get("decl")

    # --- Lightcurve interval ---
    def get_lastDiaSourceMjdTai(self, sample):
        """Latest MJD of a diaSource."""
        return sample.get("lastDiaSourceMjdTai")

    def get_firstDiaSourceMjdTai(self, sample):
        """Earliest MJD of a diaSource."""
        return sample.get("firstDiaSourceMjdTai")

    # --- Latest flux (nJy), overall then per band ---
    def get_latest_psfFlux(self, sample):
        """Latest flux of any band (nJy)."""
        return sample.get("latest_psfFlux")

    def get_u_psfFlux(self, sample):
        """Latest u flux (nJy)."""
        return sample.get("u_psfFlux")

    def get_u_latestMJD(self, sample):
        """MJD of latest u flux."""
        return sample.get("u_latestMJD")

    def get_u_psfFluxMean(self, sample):
        """Weighted mean point-source model flux for u filter (nJy)."""
        return sample.get("u_psfFluxMean")

    def get_u_psfFluxMeanErr(self, sample):
        """Standard error of u_psfFluxMean (nJy)."""
        return sample.get("u_psfFluxMeanErr")

    def get_g_psfFlux(self, sample):
        """Latest g flux (nJy)."""
        return sample.get("g_psfFlux")

    def get_g_latestMJD(self, sample):
        """MJD of latest g flux."""
        return sample.get("g_latestMJD")

    def get_g_psfFluxMean(self, sample):
        """Weighted mean point-source model flux for g filter (nJy)."""
        return sample.get("g_psfFluxMean")

    def get_g_psfFluxMeanErr(self, sample):
        """Standard error of g_psfFluxMean (nJy)."""
        return sample.get("g_psfFluxMeanErr")

    def get_r_psfFlux(self, sample):
        """Latest r flux (nJy)."""
        return sample.get("r_psfFlux")

    def get_r_latestMJD(self, sample):
        """MJD of latest r flux."""
        return sample.get("r_latestMJD")

    def get_r_psfFluxMean(self, sample):
        """Weighted mean point-source model flux for r filter (nJy)."""
        return sample.get("r_psfFluxMean")

    def get_r_psfFluxMeanErr(self, sample):
        """Standard error of r_psfFluxMean (nJy)."""
        return sample.get("r_psfFluxMeanErr")

    def get_i_psfFlux(self, sample):
        """Latest i flux (nJy)."""
        return sample.get("i_psfFlux")

    def get_i_latestMJD(self, sample):
        """MJD of latest i flux."""
        return sample.get("i_latestMJD")

    def get_i_psfFluxMean(self, sample):
        """Weighted mean point-source model flux for i filter (nJy)."""
        return sample.get("i_psfFluxMean")

    def get_i_psfFluxMeanErr(self, sample):
        """Standard error of i_psfFluxMean (nJy)."""
        return sample.get("i_psfFluxMeanErr")

    def get_z_psfFlux(self, sample):
        """Latest z flux (nJy)."""
        return sample.get("z_psfFlux")

    def get_z_latestMJD(self, sample):
        """MJD of latest z flux."""
        return sample.get("z_latestMJD")

    def get_z_psfFluxMean(self, sample):
        """Weighted mean point-source model flux for z filter (nJy)."""
        return sample.get("z_psfFluxMean")

    def get_z_psfFluxMeanErr(self, sample):
        """Standard error of z_psfFluxMean (nJy)."""
        return sample.get("z_psfFluxMeanErr")

    def get_y_psfFlux(self, sample):
        """Latest y flux (nJy)."""
        return sample.get("y_psfFlux")

    def get_y_latestMJD(self, sample):
        """MJD of latest y flux."""
        return sample.get("y_latestMJD")

    def get_y_psfFluxMean(self, sample):
        """Weighted mean point-source model flux for y filter (nJy)."""
        return sample.get("y_psfFluxMean")

    def get_y_psfFluxMeanErr(self, sample):
        """Standard error of y_psfFluxMean (nJy)."""
        return sample.get("y_psfFluxMeanErr")

    # --- Counting and reliability ---
    def get_medianR(self, sample):
        """Median reliability of the diaSources in this diaObject."""
        return sample.get("medianR")

    def get_latestR(self, sample):
        """Most recent reliability of the diaSources in this diaObject."""
        return sample.get("latestR")

    def get_nDiaSources(self, sample):
        """Number of diaSources associated with this diaObject."""
        return sample.get("nDiaSources")

    def get_nPosDiaSources(self, sample):
        """Number of positive-flux diaSources associated with this diaObject."""
        return sample.get("nPosDiaSources")

    def get_nPosDiaSourcesNights(self, sample):
        """Number of nights this object was detected with positive flux."""
        return sample.get("nPosDiaSourcesNights")

    def get_nSourcesGood(self, sample):
        """Number of sources with reliability > 0.5."""
        return sample.get("nSourcesGood")

    def get_nuSources(self, sample):
        """Number of u sources."""
        return sample.get("nuSources")

    def get_ngSources(self, sample):
        """Number of g sources."""
        return sample.get("ngSources")

    def get_nrSources(self, sample):
        """Number of r sources."""
        return sample.get("nrSources")

    def get_niSources(self, sample):
        """Number of i sources."""
        return sample.get("niSources")

    def get_nzSources(self, sample):
        """Number of z sources."""
        return sample.get("nzSources")

    def get_nySources(self, sample):
        """Number of y sources."""
        return sample.get("nySources")

    # --- Other/nearest objects ---
    def get_tns_name(self, sample):
        """TNS name of this object, if it exists."""
        return sample.get("tns_name")

    # --- Absolute magnitude ---
    def get_absMag(self, sample):
        """Peak absolute magnitude (extinction corrected), if host distance available."""
        return sample.get("absMag")

    def get_absMagMJD(self, sample):
        """Time of peak absolute magnitude, if host distance available."""
        return sample.get("absMagMJD")

    # --- BazinBlackBody (BBB) fit ---
    def get_BBBRiseRate(self, sample):
        """Fitted Bazin or Exp rise rate."""
        return sample.get("BBBRiseRate")

    def get_BBBFallRate(self, sample):
        """Fitted Bazin fall rate, or None if Exp."""
        return sample.get("BBBFallRate")

    def get_BBBTemp(self, sample):
        """Fitted Bazin temperature (kK)."""
        return sample.get("BBBTemp")

    def get_BBBPeakFlux(self, sample):
        """Peak flux (nJy) if Bazin fit, else None."""
        return sample.get("BBBPeakFlux")

    def get_BBBPeakMJD(self, sample):
        """Time of peak brightness if Bazin fit, else None."""
        return sample.get("BBBPeakMJD")

    def get_BBBPeakAbsMag(self, sample):
        """Peak absolute magnitude if Bazin fit and Sherlock host distance, else None."""
        return sample.get("BBBPeakAbsMag")

    # --- Milky Way ---
    def get_glat(self, sample):
        """Galactic latitude."""
        return sample.get("glat")

    def get_ebv(self, sample):
        """Extinction E(B-V), Schlegel, Finkbeiner & Davis (1998)."""
        return sample.get("ebv")

    # --- Jump detector ---
    def get_jump1(self, sample):
        """Largest sigma jump of recent flux from previous -70 to -10 days."""
        return sample.get("jump1")

    def get_jump2(self, sample):
        """Largest sigma jump of recent flux in a different band from previous -70 to -10 days."""
        return sample.get("jump2")

    # --- Pair colours ---
    def get_latestPairMJD(self, sample):
        """Latest pair MJD."""
        return sample.get("latestPairMJD")

    def get_latestPairColourMag(self, sample):
        """Magnitude difference from latest pair."""
        return sample.get("latestPairColourMag")

    def get_latestPairColourBands(self, sample):
        """Bands used for latest pair colour, e.g. g-r, u-r."""
        return sample.get("latestPairColourBands")

    def get_latestPairColourTemp(self, sample):
        """Extinction-corrected blackbody temperature from latest pair (kK)."""
        return sample.get("latestPairColourTemp")

    def get_penultimatePairMJD(self, sample):
        """Penultimate pair MJD."""
        return sample.get("penultimatePairMJD")

    def get_penultimatePairColourMag(self, sample):
        """Magnitude difference from penultimate pair."""
        return sample.get("penultimatePairColourMag")

    def get_penultimatePairColourBands(self, sample):
        """Bands used for penultimate pair colour, e.g. g-r, u-r."""
        return sample.get("penultimatePairColourBands")

    def get_penultimatePairColourTemp(self, sample):
        """Extinction-corrected blackbody temperature from penultimate pair (kK)."""
        return sample.get("penultimatePairColourTemp")

    # --- Utility ---
    def get_htm16(self, sample):
        """Hierarchical Triangular Mesh level 16 index."""
        return sample.get("htm16")

    def get_timestamp(self, sample):
        """Time at which this object was last modified."""
        return sample.get("timestamp")

    # Light curve getters. Each returns a 1D numpy array with one entry per source in
    # the named list of the alert, or an empty array if the list is missing or empty.
    # Note: These are the "lite" light curves fields.

    @staticmethod
    def _get_light_curve_values(sample, list_name, key, dtype):
        """Pull `key` from every entry of `sample["alert"][list_name]` as a numpy array."""
        alert = sample.get("alert") or {}
        sources = alert.get(list_name) or []
        return np.array([s[key] for s in sources], dtype=dtype)

    # --- diaForcedSourcesList ---
    def get_diaForcedSource_psfFlux(self, sample):
        """PSF flux (nJy) of each diaForcedSource."""
        return self._get_light_curve_values(sample, "diaForcedSourcesList", "psfFlux", np.float32)

    def get_diaForcedSource_psfFluxErr(self, sample):
        """PSF flux error (nJy) of each diaForcedSource."""
        return self._get_light_curve_values(sample, "diaForcedSourcesList", "psfFluxErr", np.float32)

    def get_diaForcedSource_midpointMjdTai(self, sample):
        """Midpoint MJD (TAI) of each diaForcedSource."""
        return self._get_light_curve_values(sample, "diaForcedSourcesList", "midpointMjdTai", np.float32)

    def get_diaForcedSource_band(self, sample):
        """Band of each diaForcedSource."""
        return self._get_light_curve_values(sample, "diaForcedSourcesList", "band", np.str_)

    # --- diaSourcesList ---
    def get_diaSource_psfFlux(self, sample):
        """PSF flux (nJy) of each diaSource."""
        return self._get_light_curve_values(sample, "diaSourcesList", "psfFlux", np.float32)

    def get_diaSource_psfFluxErr(self, sample):
        """PSF flux error (nJy) of each diaSource."""
        return self._get_light_curve_values(sample, "diaSourcesList", "psfFluxErr", np.float32)

    def get_diaSource_band(self, sample):
        """Band of each diaSource."""
        return self._get_light_curve_values(sample, "diaSourcesList", "band", np.str_)

    def get_diaSource_midpointMjdTai(self, sample):
        """Midpoint MJD (TAI) of each diaSource."""
        return self._get_light_curve_values(sample, "diaSourcesList", "midpointMjdTai", np.float32)

    def get_diaSource_reliability(self, sample):
        """Reliability score of each diaSource."""
        return self._get_light_curve_values(sample, "diaSourcesList", "reliability", np.float32)

    # Collation methods for each light curve field. Each returns `<field>`,
    # `<field>_lengths` and `<field>_mask`. Note that `samples` here is a list of
    # dictionaries assembled by Hyrax from the getters above, NOT a list of raw alerts.

    @staticmethod
    def _collate_band(samples, field):
        """Modeled on Hyrax's `collate_as_1d_light_curve`, but for single-character
        band strings, padded with empty strings.

        Returns
        -------
        dict
            `field` - "U1" array (batch, max_len) of padded band values
            `field_lengths` - int64 array (batch) of true light curve lengths
            `field_mask` - int64 array (batch, max_len) denoting data vs. padding
        """
        vals = [s[field] for s in samples]
        lengths = np.array([len(v) for v in vals], dtype=np.int64)
        max_len = int(lengths.max())

        padded = np.full((len(vals), max_len), "", dtype="U1")
        mask = np.zeros((len(vals), max_len), dtype=np.int64)
        for i, v in enumerate(vals):
            padded[i, : lengths[i]] = v
            mask[i, : lengths[i]] = 1

        return {
            field: padded,
            field + "_lengths": lengths,
            field + "_mask": mask,
        }

    # --- diaForcedSourcesList ---
    def collate_diaForcedSource_psfFlux(self, samples):
        """Collate diaForcedSource PSF flux light curves."""
        return collate_as_1d_light_curve(samples, "diaForcedSource_psfFlux")

    def collate_diaForcedSource_psfFluxErr(self, samples):
        """Collate diaForcedSource PSF flux error light curves."""
        return collate_as_1d_light_curve(samples, "diaForcedSource_psfFluxErr")

    def collate_diaForcedSource_midpointMjdTai(self, samples):
        """Collate diaForcedSource midpoint MJD (TAI) light curves."""
        return collate_as_1d_light_curve(samples, "diaForcedSource_midpointMjdTai")

    def collate_diaForcedSource_band(self, samples):
        """Collate diaForcedSource band light curves."""
        return self._collate_band(samples, "diaForcedSource_band")

    # --- diaSourcesList ---
    def collate_diaSource_psfFlux(self, samples):
        """Collate diaSource PSF flux light curves."""
        return collate_as_1d_light_curve(samples, "diaSource_psfFlux")

    def collate_diaSource_psfFluxErr(self, samples):
        """Collate diaSource PSF flux error light curves."""
        return collate_as_1d_light_curve(samples, "diaSource_psfFluxErr")

    def collate_diaSource_band(self, samples):
        """Collate diaSource band light curves."""
        return self._collate_band(samples, "diaSource_band")

    def collate_diaSource_midpointMjdTai(self, samples):
        """Collate diaSource midpoint MJD (TAI) light curves."""
        return collate_as_1d_light_curve(samples, "diaSource_midpointMjdTai")

    def collate_diaSource_reliability(self, samples):
        """Collate diaSource reliability light curves."""
        return collate_as_1d_light_curve(samples, "diaSource_reliability")
