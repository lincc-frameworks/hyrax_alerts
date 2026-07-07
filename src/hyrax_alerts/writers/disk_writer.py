from hyrax_alerts.writers.base_writer import HyraxAlertsBaseWriter

# NOTE: Hyrax already has a mode where it will output results to disk from the
# infer_stream into a timestamped results directory. However, it doesn't save
# the source data. This writer _might_ ultimately do that.


class HyraxAlertsDiskWriter(HyraxAlertsBaseWriter):
    """Writer class for writing model output to disk."""

    def __init__(self, config):
        super().__init__(config)

        # TODO: Think about result_location instead of data_location.
        self.data_location = config.get("data_location", None)
        if self.data_location:
            self.directory = self.data_location
        # TODO: We should just instantiate a ResultDataset here

    def write(self, data_batch: list, result_batch: list):
        """Write a batch of results and their source data to disk."""
        # TODO: Output using ResultDataset class here
        pass

    def __enter__(self):
        """Enter the context manager for the writer."""
        return self

    def close(self):
        """Close the writer and release any resources."""
        pass
