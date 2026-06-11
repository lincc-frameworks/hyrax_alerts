from hyrax.plugin_utils import update_registry

WRITER_REGISTRY = {}


class HyraxAlertsBaseWriter:
    """Base writer class for HyraxAlerts output writers."""

    def __init__(self, config):
        self.config = config

    def __init_subclass__(cls):
        """Automatically register subclasses in the WRITER_REGISTRY."""
        update_registry(WRITER_REGISTRY, cls.__name__, cls)

    def _register_post_process(self, function):
        """Used to register a post-processing function that will be applied to
        results output from a model"""
        pass

    def _register_post_filter(self, function):
        """Used to register a post-filtering function that will be applied to
        results output from a model, after post-processing."""
        pass

    def post_process(self, result_batch: list) -> list:
        """Default implementation that simply returns the input batch. Subclasses
        can register a custom post-processing function using the
        _register_post_process method."""
        return result_batch

    def post_filter(self, result_batch: list) -> list:
        """Default implementation that simply returns the input batch. Subclasses
        can register a custom post-filtering function using the
        _register_post_filter method."""
        return result_batch

    def write(self, result_batch: list):
        """Abstract method to write a batch of results.
        Subclasses must implement this"""
        raise NotImplementedError("Subclasses must implement the write method")
