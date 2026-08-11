from pathlib import Path

import numpy as np

from hyrax_alerts.run_context import RUN_DIR_POSTFIX, reserve_results_dir
from hyrax_alerts.writers.base_writer import HyraxAlertsBaseWriter

# NOTE: This is the most naive implementation of a disk writer possible. We need
# more input from our users to determine specifically what would be the most usable
# form of the results. For now, simply saving object id and model output in .npy
# files.

# NOTE: Hyrax already has a mode where it will output results to disk from the
# infer_stream into a timestamped results directory in lance format. However, it
# doesn't save the source data. This writer _might_ ultimately do that.


class HyraxAlertsDiskWriter(HyraxAlertsBaseWriter):
    """Writer class for writing model output to disk.

    Configuration keys:

    ``output_location``
        Where to write. Defaults to a directory inside this run's output
        directory, named after the writer's config name. See
        :mod:`hyrax_alerts.run_context`.
    ``timestamped_subdirectory``
        When ``true`` (the default), ``output_location`` is treated as a root and
        batches are written to a ``<timestamp>-hyrax_alerts-<random_str>``
        subdirectory of it, so repeated runs never overwrite each other. When
        ``false``, ``output_location`` is the exact target directory.

    The directory is created when the first batch is written, so a writer that
    never receives records leaves nothing behind.
    """

    def __init__(self, config):
        super().__init__(config)
        output_location = config.get("output_location", None)
        if output_location is None:
            raise ValueError("HyraxAlertsDiskWriter requires an 'output_location' path.")

        if config.get("timestamped_subdirectory", True):
            self.output_location = reserve_results_dir(output_location, RUN_DIR_POSTFIX)
        else:
            self.output_location = Path(output_location).expanduser().resolve()

        self.file_counter = 0
        self._directory_ready = False

    def write(self, result_batch: list[dict]):
        """Write a batch of result dictionaries to disk."""
        if not self._directory_ready:
            self.output_location.mkdir(parents=True, exist_ok=True)
            self._directory_ready = True

        # Skip past any batch files already present so a directory shared with
        # another writer instance is appended to rather than overwritten.
        output_path = self.output_location / f"batch_{self.file_counter:08d}.npy"
        while output_path.exists():
            self.file_counter += 1
            output_path = self.output_location / f"batch_{self.file_counter:08d}.npy"

        np.save(output_path, np.array(result_batch, dtype=object))

        self.file_counter += 1

    def __enter__(self):
        """Enter the context manager for the writer."""
        return self

    def close(self):
        """Close the writer and release any resources."""
        pass
