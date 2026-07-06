from hyrax_alerts.writers.base_writer import HyraxAlertsBaseWriter


class HyraxAlertsDiskWriter(HyraxAlertsBaseWriter):
    """Writer class for writing model output to disk."""

    def __init__(self, config):
        super().__init__(config)
        self.data_location = config.get("data_location", None)
        if self.data_location:
            self.directory = self.data_location
        # TODO: We should just instantiate a ResultDataset here

    def write(self, result_batch: list):
        """Write a batch of results to disk."""
        # TODO: Output using ResultDataset class here
        pass
