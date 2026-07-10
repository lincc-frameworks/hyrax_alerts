from hyrax_alerts.writers.base_writer import HyraxAlertsBaseWriter

# NOTE: Hyrax already has a mode where it will output results to disk from the
# infer_stream into a timestamped results directory. However, it doesn't save
# the source data. This writer _might_ ultimately do that.


class HyraxAlertsDiskWriter(HyraxAlertsBaseWriter):
    """Writer class for writing model output to disk."""

    def __init__(self, config):
        super().__init__(config)
        self.output_location = config.get("output_location", None)
        if self.output_location is None:
            raise ValueError("HyraxAlertsDiskWriter requires an 'output_location' path.")

        # create self.output_location directory if it doesn't exist, log a warning
        # if we create one for the user
        import os

        if not os.path.exists(self.output_location):
            os.makedirs(self.output_location, exist_ok=True)
            print(f"Warning: Created directory '{self.output_location}' for output.")

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
