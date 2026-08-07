from collections.abc import Callable
from pathlib import Path
from types import MethodType

from hyrax.plugin_utils import update_registry

from hyrax_alerts.callable_loader import load_callable
from hyrax_alerts.logging_utils import get_logger

WRITER_REGISTRY = {}

logger = get_logger(__name__)


def instantiate_writer(writer_config: dict) -> "HyraxAlertsBaseWriter | None":
    """Instantiate a writer from a config dict shaped like a `[hyrax_alerts.writers.<name>]`
    table (i.e. it must contain a ``writer_class`` key naming a class registered in
    ``WRITER_REGISTRY``).

    Returns ``None`` if ``writer_class`` is missing or not registered.
    """
    writer_class = WRITER_REGISTRY.get(writer_config.get("writer_class"))
    if writer_class is None:
        return None
    return writer_class(writer_config)


def get_reject_writer(
    owner_config: dict, owner_name: str, reject_output_root: str | None
) -> "HyraxAlertsBaseWriter | None":
    """Build the reject-writer for one owner (the consumer, or one configured writer).

    If ``owner_config`` defines its own ``reject_writer`` table, that config is used
    as-is. Otherwise, if a pipeline-wide ``reject_output_root`` is configured, default
    to a ``HyraxAlertsDiskWriter`` nested under ``<reject_output_root>/<owner_name>``,
    so every owner's rejected records land under one common parent directory without
    requiring per-owner configuration. Returns ``None`` if neither is configured.
    """
    explicit_config = owner_config.get("reject_writer")
    if explicit_config:
        return instantiate_writer(explicit_config)
    if not reject_output_root:
        return None
    return instantiate_writer(
        {
            "writer_class": "HyraxAlertsDiskWriter",
            "output_location": str(Path(reject_output_root) / owner_name),
        }
    )


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
        # Set by get_writers()/get_reject_writer() when a reject_output_root or
        # per-writer reject_writer config is configured; None otherwise.
        self.reject_writer = None

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

    def post_process(self, result_batch: list[dict]) -> list[dict]:
        """Default implementation that simply returns the input batch. Users can
        provide their own implementations by specifying the dotted path to a callable
        function in the configuration file.

        For example:
        .. code-block:: toml

            [hyrax_alerts.writers.to_disk]
            writer_class = "HyraxAlertsDiskWriter"
            output_location = "./results"
            post_process = "hyrax_alerts.example_functions.example_post_process"

        Parameters
        ----------
        result_batch : list[dict]
            A batch of results to be post-processed.

        Returns
        -------
        list[dict]
            The post-processed batch of results as a list of dictionaries. Each
            dictionary corresponds to a single result in the batch.
        """
        return result_batch

    def post_filter(self, result_batch: list[dict]) -> list[dict]:
        """Return a filtered list of results. Users can provide their own
        implementations by specifying the dotted path to a callable function in
        the configuration file.

        For example:
        .. code-block:: toml

            [hyrax_alerts.writers.to_disk]
            writer_class = "HyraxAlertsDiskWriter"
            output_location = "./results"
            post_filter = "hyrax_alerts.example_functions.example_post_filter"

        Parameters
        ----------
        result_batch : list[dict]
            A batch of results to be filtered.

        Returns
        -------
        list[dict]
            The filtered list of results as a list of dictionaries. Each
            dictionary corresponds to a single result in the batch.
        """
        return result_batch

    def write(self, result_batch: list[dict]):
        """Abstract method to write a batch of results. Subclasses must implement
        this method.

        Parameters
        ----------
        result_batch : list[dict]
            A batch of results that have been post-processed and filtered. Each
            dictionary holds the input data for one alert plus its model output
            under the ``__hyrax_result`` key, which is always a dict keyed by
            result field name. If the model returns only a single result field,
            it will be normalized under the ``data`` key. If the model returns
            multiple result fields, they will be preserved under their original
            field names.

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
