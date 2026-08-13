import io
import numpy as np
from typing import Any, cast

import fastavro

from .kafka_consumer import HyraxKafkaConsumer

# Photometry Constants
LOG_CONST = 1.0 / np.log(10)
NUM_BANDS = 3

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

        Parameters
        ----------
        config : dict
            The configuration dictionary for the consumer.
        data_location : str, optional
            The location of the data stream. Defaults to None.
        """
        super().__init__(config=config, data_location=data_location)

    def get_photometry(self, msg):
        """Extract photometry data from the incoming message.

        This method retrieves the photometry data from the decoded message.

        Parameters
        ----------
        msg : bytes
            The incoming message containing photometry data.

        Returns
        -------
        dict
            The extracted photometry data as a dictionary.
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

    def get_mean(self, msg):
        # this is almost definitely not the right stat,
        # but I don't have the stats files handy
        # so I just want something I can shove through there
        mean_flux = np.ones((1,4), dtype=np.float32)
        return mean_flux

    def get_std(self, msg):
        # this is almost definitely not the right stat,
        # but I don't have the stats files handy
        # so I just want something I can shove through there
        std_flux = np.ones((1,4), dtype=np.float32)
        return std_flux

    @staticmethod
    def collate_photometry(batch):
        """custom collate function for photometry data"""
        seqs = [i["photometry"] for i in batch]

        lengths = [s.shape[0] for s in seqs]
        max_len = max([257, max(lengths)])

        # Create padding arrays: False where there is data, True where there is padding
        padded = []
        for s in seqs:
            pad_width = ((0, max_len - s.shape[0]), (0, 0))
            padded.append(np.pad(s, pad_width, mode="constant", constant_values=0.0))
        pad = np.stack(padded, axis=0)
        pad_mask = np.stack(
            [np.concatenate([np.zeros(l), np.ones(pad.shape[1] - l)]) for l in lengths]
        ).astype(bool)

        # Truncate to a consistent sequence length
        pad = pad[:, :257, :]
        pad_mask = pad_mask[:, :257]

        return {
            "photometry": pad,
            "pad_mask": pad_mask,
        }