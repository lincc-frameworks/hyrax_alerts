from collections.abc import Callable
from types import MethodType

from hyrax.plugin_utils import update_registry

from hyrax_alerts.callable_loader import load_callable

WRITER_REGISTRY = {}


def get_writers(config):
    """Return a list of writer instances based on the provided configuration.
    Parameters
    ----------
    config : dict
        Configuration dictionary containing writer settings.

    Returns
    -------
    list
        A list of instantiated writer objects.
    """

    writers = []
    for _, writer_config in config["hyrax_alerts"]["writers"].items():
        writer_class = WRITER_REGISTRY.get(writer_config["writer_class"])
        if writer_class:
            writers.append(writer_class(writer_config))
    # TODO: Log the writers that were instantiated for debugging purposes
    return writers


class HyraxAlertsBaseWriter:
    """Base writer class for HyraxAlerts output writers.

    Each subclass will be registered in the WRITER_REGISTRY, allowing for dynamic
    instantiation based on configuration.

    The post_process and post_filter methods can be specified in the configuration
    file as dotted paths to callable functions. These methods will be bound to the
    writer instance, allowing for custom processing and filtering of results.

    A subclass should implement the write method to define how results are written
    (e.g., to disk, to a database, etc.). See the docstring for the write method
    for more details.

    Each writer is also a context manager, allowing for resource management
    using the `with` statement. Subclasses can override the __enter__ and close
    methods to manage resources as needed.
    """

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

    def _register_post_process(self, function: str | Callable):
        """Used to register a post-processing function that will be applied to
        results output from a model"""
        if isinstance(function, str):
            function = load_callable(function)
        self.post_process = MethodType(function, self)

    def _register_post_filter(self, function: str | Callable):
        """Used to register a post-filtering function that will be applied to
        results output from a model, after post-processing."""
        if isinstance(function, str):
            function = load_callable(function)
        self.post_filter = MethodType(function, self)

    def post_process(self, result_batch: list) -> list:
        """Default implementation that simply returns the input batch. Users can
        provide their own implementations by specifying the dotted path to a callable
        function in the configuration file.

        For example:
        .. code-block:: toml

            [hyrax_alerts.writers.to_disk]
            writer_class = "HyraxAlertsDiskWriter"
            location = "./results"
            post_process = "hyrax_alerts.example_functions.example_post_process"

        Parameters
        ----------
        result_batch : list
            A batch of results to be post-processed.

        Returns
        -------
        list
            The post-processed batch of results.
        """
        return result_batch

    def post_filter(self, result_batch: list) -> list[bool]:
        """Return a boolean selector aligned to ``result_batch``. Users can
        provide their own implementations by specifying the dotted path to a callable
        function in the configuration file.

        For example:
        .. code-block:: toml

            [hyrax_alerts.writers.to_disk]
            writer_class = "HyraxAlertsDiskWriter"
            location = "./results"
            post_filter = "hyrax_alerts.example_functions.example_post_filter"

        Parameters
        ----------
        result_batch : list
            A batch of results to be filtered.

        Returns
        -------
        list[bool]
            A list of booleans indicating which results to keep.
        """
        return [True] * len(result_batch)

    def _post_filter_batches(self, data_batch: list, result_batch: list) -> tuple[list, list]:
        """Filter result and data batches while preserving their alignment."""
        if len(data_batch) != len(result_batch):
            raise ValueError("data_batch and result_batch must be the same length to preserve alignment")

        selection = self.post_filter(result_batch)

        if len(selection) != len(result_batch):
            raise ValueError("post_filter must return a boolean selector with one entry per result")

        if not all(isinstance(keep_result, bool) for keep_result in selection):
            raise TypeError("post_filter must return booleans so results stay aligned with input data")

        filtered_data_batch = [
            data for data, keep_result in zip(data_batch, selection, strict=True) if keep_result
        ]
        filtered_result_batch = [
            result for result, keep_result in zip(result_batch, selection, strict=True) if keep_result
        ]
        return filtered_data_batch, filtered_result_batch

    def write(self, data_batch: list, result_batch: list):
        """Abstract method to write a batch of results. Subclasses must implement
        this method.

        Parameters
        ----------
        data_batch : list
            A batch of input data corresponding to the results.
        result_batch : list
            A batch of model results that have been post-processed and filtered.

        Returns
        -------
        None
        """
        raise NotImplementedError("Subclasses must implement the write method")

    def __enter__(self):
        """Enter the runtime context related to this object."""
        return self

    def __exit__(self, *_):
        """Exit the runtime context related to this object."""
        self.close()
        return False

    def close(self):
        """Close any resources held by the writer. Subclasses can override this if needed."""
        pass
