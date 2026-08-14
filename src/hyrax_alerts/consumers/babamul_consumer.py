import io
from typing import Any, cast

import fastavro
import numpy as np

from .kafka_consumer import HyraxKafkaConsumer

# Photometry Constants
LOG_CONST = 1.0 / np.log(10)
NUM_BANDS = 3
COLLATION_LENGTH = 257

BAND_TO_IDX = {
    "g": 0,
    "r": 1,
    "i": 2,
}


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
        reader = fastavro.reader(io.BytesIO(msg.value()))
        result = cast(dict[str, Any], next(reader))
        return result

    def get_candid(self, msg):
        """Extract the Candid from the incoming message.

        This method retrieves the Candid from the decoded message.

        Parameters
        ----------
        msg : bytes
            The incoming message containing the Candid.

        Returns
        -------
        int
            The extracted Candid.
        """
        return msg["candid"]


class BabamulPhotometryConsumer(BabamulConsumer):
    """A consumer for the Babamul photometry data stream.

    This class is a specialized consumer that inherits from `BabamulConsumer`.
    It is designed to handle the specific requirements of the Babamul photometry data stream.
    """

    def __init__(self, config, data_location=None):
        """Initialize the Babamul photometry consumer.
        Designed to work with the applecider HyraxBaselineCLS model.

        Parameters
        ----------
        config : dict
            The configuration dictionary for the consumer.
        data_location : str, optional
            The location of the data stream. Defaults to None.
        """
        super().__init__(config=config, data_location=data_location)
        stats_path = config["hyrax_alerts"]["consumer"]["BabamulPhotometryConsumer"]["stats_path"]
        self.stats = np.load(stats_path)

    def get_photometry(self, msg):
        """Extract photometry data from the incoming message.

        This method retrieves the photometry data from the decoded message.

        Parameters
        ----------
        msg : JSON
            The incoming message containing photometry data.

        Returns
        -------
        Numpy array
            an array of the photometry data with shape (N, 7) where N is the number of observations.
            The columns are:
            - dt: time since first observation
            - dt_prev: time since previous observation
            - log_flux: log10 of the flux
            - log_flux_error: log10 of the flux error
            - one-hot encoding of the band (g, r, i)
        """
        photometry = msg["fp_hists"]

        obstimes = []
        fluxes = []
        flux_errors = []
        bands = []
        for obs in photometry:
            obstimes.append(obs["jd"])
            fluxes.append(obs["psfFlux"])
            flux_errors.append(obs["psfFluxErr"])
            bands.append(obs["band"])

        t0 = obstimes[0]
        dt = np.array(obstimes) - t0
        dt_prev = np.diff(np.r_[t0, np.array(obstimes)])
        f = np.clip(np.array(fluxes), 1e-6, None)
        log_fluxes = np.log10(f)
        log_flux_errors = np.array(flux_errors) * LOG_CONST / f

        band_id = np.array([BAND_TO_IDX[b] for b in bands], dtype=np.int64)

        vec4 = np.stack([dt, dt_prev, log_fluxes, log_flux_errors], axis=1)

        one_hot_encoding = np.eye(NUM_BANDS, dtype=np.float32)
        one_hot_band = one_hot_encoding[band_id]

        photometry_vec = np.concatenate([vec4, one_hot_band], axis=1)
        return photometry_vec

    def get_mean(self, _):
        """Get the mean of all the photometry features computed from the training set.
        This is used for normalization of the photometry features.

        Stats grabbed from prev-provided stats file, through 'stats_path' in the config file.

        Returns
        -------
        Numpy array
            an array of the mean of the photometry features with shape (1, 4)
            The Columns are:
            - mean of dt
            - mean of dt_prev
            - mean of log_flux
            - mean of log_flux_error
        """
        return self.stats["mean"].reshape(1, 4)

    def get_std(self, _):
        """Get the standard deviation of all the photometry features computed from the training set.
        This is used for normalization of the photometry features.

        Stats grabbed from prev-provided stats file, through 'stats_path' in the config file.

        Returns
        -------
        Numpy array
            an array of the standard deviation of the photometry features with shape (1, 4)
            The Columns are:
            - std of dt
            - std of dt_prev
            - std of log_flux
            - std of log_flux_error
        """
        return self.stats["std"].reshape(1, 4)

    @staticmethod
    def collate_photometry(batch):
        """custom collate function for photometry data"""
        seqs = [i["photometry"] for i in batch]

        lengths = [s.shape[0] for s in seqs]
        max_len = max([COLLATION_LENGTH, max(lengths)])

        # Create padding arrays: False where there is data, True where there is padding
        padded = []
        for s in seqs:
            pad_width = ((0, max_len - s.shape[0]), (0, 0))
            padded.append(np.pad(s, pad_width, mode="constant", constant_values=0.0))
        pad = np.stack(padded, axis=0)
        pad_mask = np.stack(
            [np.concatenate([np.zeros(ln), np.ones(pad.shape[1] - ln)]) for ln in lengths]
        ).astype(bool)

        # Truncate to a consistent sequence length
        pad = pad[:, :COLLATION_LENGTH, :]
        pad_mask = pad_mask[:, :COLLATION_LENGTH]

        return {
            "photometry": pad,
            "pad_mask": pad_mask,
        }
