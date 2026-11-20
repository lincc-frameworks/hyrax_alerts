# ruff: noqa: N802, N806

import numpy as np
from hyrax.datasets.collate_utils import collate_as_1d_light_curve

from hyrax_alerts.logging_utils import get_logger

from .kafka_consumer import HyraxKafkaConsumer

logger = get_logger(__name__)


class LasairLiteConsumer(HyraxKafkaConsumer):
    """A consumer for working with Lasair messages, specifically supporting those
    messages that include lite light curve data as `diaSourcesList`.

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

    # Getters for a PORTION of the `object` table
    def get_diaObjectId(self, sample):
        """Get the ID from a sample."""
        return sample.get("diaObjectId")

    def get_ra(self, sample):
        """Get the right ascension (RA) from a sample."""
        return sample.get("ra")

    def get_decl(self, sample):
        """Get the declination (DECL) from a sample."""
        return sample.get("decl")

    def get_firstDiaSourceMjdTai(self, sample):
        """Get the first diaSource MJD TAI from a sample."""
        return sample.get("firstDiaSourceMjdTai")

    def get_lastDiaSourceMjdTai(self, sample):
        """Get the last diaSource MJD TAI from a sample."""
        return sample.get("lastDiaSourceMjdTai")

    def get_latest_psfFlux(self, sample):
        """Get the latest PSF flux from a sample."""
        return sample.get("latest_psfFlux")

    # Getters specifically for light curve handling - each returns a numpy array
    def get_psfFlux(self, sample):
        """Get the PSF flux from a sample."""
        psfFlux = np.array([], dtype=np.float32)
        alert = sample.get("alert")
        if alert is not None:
            diaForcedSourcesList = alert.get("diaForcedSourcesList")
            if diaForcedSourcesList and len(diaForcedSourcesList) > 0:
                psfFlux = np.array([s["psfFlux"] for s in diaForcedSourcesList], dtype=np.float32)
        return psfFlux

    def get_psfFluxErr(self, sample):
        """Get the PSF flux error from a sample."""
        psfFluxErr = np.array([], dtype=np.float32)
        alert = sample.get("alert")
        if alert is not None:
            diaForcedSourcesList = alert.get("diaForcedSourcesList")
            if diaForcedSourcesList and len(diaForcedSourcesList) > 0:
                psfFluxErr = np.array([s["psfFluxErr"] for s in diaForcedSourcesList], dtype=np.float32)
        return psfFluxErr

    def get_midpointMjdTai(self, sample):
        """Get the midpoint MJD TAI from a sample."""
        midpointMjdTai = np.array([], dtype=np.float32)
        alert = sample.get("alert")
        if alert is not None:
            diaForcedSourcesList = alert.get("diaForcedSourcesList")
            if diaForcedSourcesList and len(diaForcedSourcesList) > 0:
                midpointMjdTai = np.array(
                    [s["midpointMjdTai"] for s in diaForcedSourcesList], dtype=np.float32
                )
        return midpointMjdTai

    def get_band(self, sample):
        """Get the band from a sample."""
        band = np.array([], dtype=np.float32)
        alert = sample.get("alert")
        if alert is not None:
            diaForcedSourcesList = alert.get("diaForcedSourcesList")
            if diaForcedSourcesList and len(diaForcedSourcesList) > 0:
                band = np.array([s["band"] for s in diaForcedSourcesList], dtype=np.str_)
        return band

    # Collation methods for each of the light curve fields. Note that `samples` here
    # is a list of dictionaries assembled by Hyrax from the getters above, NOT a list
    # of raw alerts.

    def collate_psfFlux(self, samples):
        """Collate PSF flux light curves. Returns `psfFlux`, `psfFlux_lengths`
        and `psfFlux_mask`."""
        return collate_as_1d_light_curve(samples, "psfFlux")

    def collate_psfFluxErr(self, samples):
        """Collate PSF flux error light curves. Returns `psfFluxErr`,
        `psfFluxErr_lengths` and `psfFluxErr_mask`."""
        return collate_as_1d_light_curve(samples, "psfFluxErr")

    def collate_midpointMjdTai(self, samples):
        """Collate midpoint MJD TAI light curves. Returns `midpointMjdTai`,
        `midpointMjdTai_lengths` and `midpointMjdTai_mask`."""
        return collate_as_1d_light_curve(samples, "midpointMjdTai")

    def collate_band(self, samples):
        """Collate band light curves. Modeled on Hyrax's `collate_as_1d_light_curve`,
        but for single-character band strings, padded with empty strings.

        Returns
        -------
        dict
            `band` - "U1" array (batch, max_len) of padded band values
            `band_lengths` - int64 array (batch) of true light curve lengths
            `band_mask` - int64 array (batch, max_len) denoting data vs. padding
        """
        field = "band"
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
