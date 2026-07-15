import base64
import datetime
import os
import random
from pathlib import Path

import numpy as np

from hyrax_alerts.writers.base_writer import HyraxAlertsBaseWriter

# NOTE: This is the most naive implementation of a disk writer possible. We need
# more input from our users to determine specifically what would be the most usable
# form of the results. For now, simply saving object id and model output in .npy
# files.

# NOTE: Hyrax already has a mode where it will output results to disk from the
# infer_stream into a timestamped results directory in lance format. However, it
# doesn't save the source data. This writer _might_ ultimately do that.


def create_results_dir(results_root: str | Path, postfix: str) -> Path:
    """Creates a results directory for this run.

    Postfix is the verb name of the run e.g. (infer, train, etc)

    The directory is created within the results dir and follows the pattern
    <timestamp>-<postfix>-<random_str>

    The resulting directory is returned.

    Parameters
    ----------
    results_root : str | Path
        The root, or parent, directory where result directories will be stored.
    postfix : str
        The verb name of the run.

    Returns
    -------
    Path
        The path created by this function
    """
    results_root = Path(results_root).expanduser().resolve()
    # This date format is chosen specifically to create a lexical search order
    # which matches the date order.
    timestamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    # Generate 4 random ascii characters to avoid collisions from multiple processes
    # started in a shared filesystem environment.
    random_str = base64.urlsafe_b64encode(random.randbytes(3)).decode("ascii")
    directory = results_root / f"{timestamp}-{postfix}-{random_str}"
    directory.mkdir(parents=True, exist_ok=False)
    return directory


class HyraxAlertsDiskWriter(HyraxAlertsBaseWriter):
    """Writer class for writing model output to disk."""

    def __init__(self, config):
        super().__init__(config)
        self.output_location = config.get("output_location", None)
        if self.output_location is None:
            raise ValueError("HyraxAlertsDiskWriter requires an 'output_location' path.")

        # create timestamped output directory within the specified output location
        self.output_location = create_results_dir(self.output_location, "hyrax_alerts")

        self.file_counter = 0

    def write(self, data_batch: dict, result_batch: list | dict[str, list]):
        """Write a batch of results and their object IDs to disk."""
        if isinstance(result_batch, dict):
            result_batch["_object_id"] = data_batch["object_id"]
        else:
            result_batch = {"_object_id": data_batch["object_id"], "result": result_batch}

        np.save(os.path.join(self.output_location, f"batch_{self.file_counter:08d}.npy"), result_batch)

        self.file_counter += 1

    def __enter__(self):
        """Enter the context manager for the writer."""
        return self

    def close(self):
        """Close the writer and release any resources."""
        pass
