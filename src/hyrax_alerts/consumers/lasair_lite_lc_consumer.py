# ruff: noqa: N802, N806

import numpy as np

from hyrax_alerts.logging_utils import get_logger

from .kafka_consumer import HyraxKafkaConsumer

logger = get_logger(__name__)

LSST_BANDS = ["u", "g", "r", "i", "z", "y"]
BAND_MAP = {k: v for k, v in zip(LSST_BANDS, range(len(LSST_BANDS)), strict=False)}


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

    # ^ Getters for a PORTION of the `object` table

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
    # ^ These are only for the diaSourceList fields!
    # ^ The diaForcedSourceList implementation is deferred until later

    def get_psfFlux(self, sample):
        """Get the PSF flux from a sample."""
        psfFlux = np.array([], dtype=np.float32)
        alert = sample.get("alert")
        if alert is not None:
            diaSourcesList = alert.get("diaSourcesList")
            if diaSourcesList and len(diaSourcesList) > 0:
                psfFlux = np.array([s["psfFlux"] for s in diaSourcesList], dtype=np.float32)
        return psfFlux

    def get_psfFluxErr(self, sample):
        """Get the PSF flux error from a sample."""
        psfFluxErr = np.array([], dtype=np.float32)
        alert = sample.get("alert")
        if alert is not None:
            diaSourcesList = alert.get("diaSourcesList")
            if diaSourcesList and len(diaSourcesList) > 0:
                psfFluxErr = np.array([s["psfFluxErr"] for s in diaSourcesList], dtype=np.float32)
        return psfFluxErr

    def get_midpointMjdTai(self, sample):
        """Get the midpoint MJD TAI from a sample."""
        midpointMjdTai = np.array([], dtype=np.float32)
        alert = sample.get("alert")
        if alert is not None:
            diaSourcesList = alert.get("diaSourcesList")
            if diaSourcesList and len(diaSourcesList) > 0:
                midpointMjdTai = np.array([s["midpointMjdTai"] for s in diaSourcesList], dtype=np.float32)
        return midpointMjdTai

    def get_band(self, sample):
        """Get the band from a sample."""
        band = np.array([], dtype=np.int8)
        alert = sample.get("alert")
        if alert is not None:
            diaSourcesList = alert.get("diaSourcesList")
            if diaSourcesList and len(diaSourcesList) > 0:
                band = np.array(
                    [BAND_MAP.get(s["band"].lower(), np.nan) for s in diaSourcesList], dtype=np.int8
                )
        return band

    def get_reliability(self, sample):
        """Get the reliability from a sample."""
        reliability = np.array([], dtype=np.float32)
        alert = sample.get("alert")
        if alert is not None:
            diaSourcesList = alert.get("diaSourcesList")
            if diaSourcesList and len(diaSourcesList) > 0:
                reliability = np.array([s["reliability"] for s in diaSourcesList], dtype=np.float32)
        return reliability

    def _num_dia_sources(self, sample):
        """Get the number of dia sources from a sample."""
        alert = sample.get("alert")
        if alert is not None:
            diaSourcesList = alert.get("diaSourcesList")
            if diaSourcesList:
                return len(diaSourcesList)
        return 0

    # Collation methods for each of the light curve fields

    def collate_psfFlux(self, samples):
        """Collate PSF flux data from a list of samples. Returns a dictionary with
        the collated psfFlux and a corresponding psfFlux_mask indicating valid values."""
        collated_data = {}

        max_dia_sources = max(self._num_dia_sources(s) for s in samples)
        collated_data["psfFlux"] = np.array(
            [
                np.pad(
                    self.get_psfFlux(sample),
                    (0, max(0, max_dia_sources - self._num_dia_sources(sample))),
                    mode="constant",
                    constant_values=np.nan,
                )[:max_dia_sources]
                for sample in samples
            ]
        )

        collated_data["psfFlux_mask"] = np.array(
            [[not np.isnan(value) for value in series] for series in collated_data["psfFlux"]]
        )

        return collated_data

    def collate_psfFluxErr(self, samples):
        """Collate PSF flux error data from a list of samples. Returns a dictionary with
        the collated psfFluxErr and a corresponding psfFluxErr_mask indicating valid values."""
        collated_data = {}

        max_dia_sources = max(self._num_dia_sources(s) for s in samples)
        collated_data["psfFluxErr"] = np.array(
            [
                np.pad(
                    self.get_psfFluxErr(sample),
                    (0, max(0, max_dia_sources - self._num_dia_sources(sample))),
                    mode="constant",
                    constant_values=np.nan,
                )[:max_dia_sources]
                for sample in samples
            ]
        )

        collated_data["psfFluxErr_mask"] = np.array(
            [[not np.isnan(value) for value in series] for series in collated_data["psfFluxErr"]]
        )

        return collated_data

    def collate_midpointMjdTai(self, samples):
        """Collate midpoint MJD TAI data from a list of samples. Returns a dictionary with
        the collated midpointMjdTai and a corresponding midpointMjdTai_mask indicating valid values."""
        collated_data = {}

        max_dia_sources = max(self._num_dia_sources(s) for s in samples)
        collated_data["midpointMjdTai"] = np.array(
            [
                np.pad(
                    self.get_midpointMjdTai(sample),
                    (0, max(0, max_dia_sources - self._num_dia_sources(sample))),
                    mode="constant",
                    constant_values=np.nan,
                )[:max_dia_sources]
                for sample in samples
            ]
        )

        collated_data["midpointMjdTai_mask"] = np.array(
            [[not np.isnan(value) for value in series] for series in collated_data["midpointMjdTai"]]
        )

        return collated_data

    def collate_band(self, samples):
        """Collate band data from a list of samples. Returns a dictionary with
        the collated band and a corresponding band_mask indicating valid values."""
        collated_data = {}

        max_dia_sources = max(self._num_dia_sources(s) for s in samples)
        collated_data["band"] = np.array(
            [
                np.pad(
                    self.get_band(sample),
                    (0, max(0, max_dia_sources - self._num_dia_sources(sample))),
                    mode="constant",
                    constant_values=np.nan,
                )[:max_dia_sources]
                for sample in samples
            ]
        )

        collated_data["band_mask"] = np.array(
            [[not np.isnan(value) for value in series] for series in collated_data["band"]]
        )

        return collated_data

    def collate_reliability(self, samples):
        """Collate reliability data from a list of samples. Returns a dictionary with
        the collated reliability and a corresponding reliability_mask indicating valid values."""
        collated_data = {}

        max_dia_sources = max(self._num_dia_sources(s) for s in samples)
        collated_data["reliability"] = np.array(
            [
                np.pad(
                    self.get_reliability(sample),
                    (0, max(0, max_dia_sources - self._num_dia_sources(sample))),
                    mode="constant",
                    constant_values=np.nan,
                )[:max_dia_sources]
                for sample in samples
            ]
        )

        collated_data["reliability_mask"] = np.array(
            [[not np.isnan(value) for value in series] for series in collated_data["reliability"]]
        )

        return collated_data
