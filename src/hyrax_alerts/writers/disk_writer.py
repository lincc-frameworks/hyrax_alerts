import datetime
import os

import numpy as np

from hyrax_alerts.writers.base_writer import HyraxAlertsBaseWriter

# NOTE: This is the most naive implementation of a disk writer possible. We need
# more input from our users to determine specifically what would be the most usable
# form of the results. For now, simply saving object id and model output in .npy
# files.

# NOTE: Hyrax already has a mode where it will output results to disk from the
# infer_stream into a timestamped results directory in lance format. However, it
# doesn't save the source data. This writer _might_ ultimately do that.


class HyraxAlertsDiskWriter(HyraxAlertsBaseWriter):
    """Writer class for writing model output to disk."""

    def __init__(self, config):
        super().__init__(config)
        self.output_location = config.get("output_location", None)
        if self.output_location is None:
            raise ValueError("HyraxAlertsDiskWriter requires an 'output_location' path.")

        # create timestamped output directory within the specified output location
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        self.output_location = os.path.join(self.output_location, f"results_{timestamp}")

        # create self.output_location directory if it doesn't exist, log a warning
        # if we create one for the user
        if not os.path.exists(self.output_location):
            os.makedirs(self.output_location, exist_ok=True)
            print(f"Warning: Created directory '{self.output_location}' for output.")

        self.file_counter = 0

    def write(self, data_batch: dict, result_batch: list | dict[str, list]):
        """Write a batch of results and their source data to disk."""
        if isinstance(result_batch, dict):
            result_batch["_object_id"] = data_batch["object_id"]
        else:
            result_batch = {"_object_id": data_batch["object_id"], "result": result_batch}

        np.save(f"{self.output_location}/batch_{self.file_counter:08d}.npy", result_batch)
        self.file_counter += 1

    def __enter__(self):
        """Enter the context manager for the writer."""
        return self

    def close(self):
        """Close the writer and release any resources."""
        pass
