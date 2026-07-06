from importlib import import_module
from types import MethodType

from hyrax.plugin_utils import update_registry

WRITER_REGISTRY = {}


def get_writers(config):
    """Return a list of writer instances based on the provided configuration.
    Args:
    config (dict): Configuration dictionary containing writer settings."""
    writers = []
    for _, writer_config in config["hyrax_alerts"]["writers"].items():
        writer_class = WRITER_REGISTRY.get(writer_config["writer_class"])
        if writer_class:
            writers.append(writer_class(writer_config))
    return writers


class HyraxAlertsBaseWriter:
    """Base writer class for HyraxAlerts output writers."""

    def __init__(self, config):
        """Read config then load and attach post processing and filtering methods."""
        self.config = config
        if self.config.get("post_process"):
            self._register_post_process(self.config["post_process"])
        if self.config.get("post_filter"):
            self._register_post_filter(self.config["post_filter"])

    def __init_subclass__(cls):
        """Automatically register subclasses in the WRITER_REGISTRY."""
        update_registry(WRITER_REGISTRY, cls.__name__, cls)

    @staticmethod
    def _load_callable(dotted_path):
        """Load a callable from a dotted path like package.module.function."""
        module_path, func_name = dotted_path.rsplit(".", 1)
        module = import_module(module_path)
        function = getattr(module, func_name)
        if not callable(function):
            raise TypeError(f"{dotted_path} is not callable")
        return function

    def _register_post_process(self, function):
        """Used to register a post-processing function that will be applied to
        results output from a model"""
        if isinstance(function, str):
            function = self._load_callable(function)
        self.post_process = MethodType(function, self)

    def _register_post_filter(self, function):
        """Used to register a post-filtering function that will be applied to
        results output from a model, after post-processing."""
        if isinstance(function, str):
            function = self._load_callable(function)
        self.post_filter = MethodType(function, self)

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
