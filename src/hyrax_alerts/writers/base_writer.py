from collections.abc import Callable
from types import MethodType

import numpy as np
from hyrax.plugin_utils import update_registry

from hyrax_alerts.callable_loader import load_callable
from hyrax_alerts.logging_utils import get_logger

WRITER_REGISTRY = {}

logger = get_logger(__name__)


def _aligned_batch_length(values):
    """Return aligned batch length for list/array or dict-of-aligned-fields. Initial
    return values from Hyrax models were expected to be a single numpy object. However
    in future releases we will support more complex batch structures that include
    returning a dictionary of numpy values from the model for each batch element."""
    if isinstance(values, dict):
        if not values:
            raise ValueError("result_batch dictionary must contain at least one aligned field")
        # set comprehension to collect the lengths of all fields in the dictionary
        lengths = {len(field_values) for field_values in values.values()}
        if len(lengths) != 1:
            raise ValueError("result_batch dictionary fields must share the same length")
        return lengths.pop()
    return len(values)


def _filter_aligned_values(values, selection_mask):
    """Filter an aligned collection while preserving array-backed types."""

    # NOTE: if `values` is a dictionary, we recursively filter each field. An example
    # is when `values` represents a nested data structure within the batch, as in
    # the "label" portion of {"data": {"label": [4, 5, 6]}}.
    if isinstance(values, dict):
        return {
            field: _filter_aligned_values(field_values, selection_mask)
            for field, field_values in values.items()
        }

    # NOTE: We assume that it will always be the case that `values` is an np.ndarray
    # but we include a list comprehension fallback just in case.
    if isinstance(values, np.ndarray):
        return values[selection_mask]
    return [value for value, keep_result in zip(values, selection_mask, strict=True) if keep_result]


def _filter_data_batch(data_batch, selection_mask):
    """Filter a structured batch while preserving aligned top-level fields."""
    # Filter the "object_id" key/value
    filtered_data_batch = {"object_id": _filter_aligned_values(data_batch["object_id"], selection_mask)}

    # Filter the remaining top-level friendly name datasets. We assume that only
    # one top-level friendly name exists because we are operating on streaming data
    # but this is built to handle multiple top-level friendly name datasets to handle
    # any future scenarios where multiple top-level friendly name datasets might exist.
    filtered_data_batch.update(
        {
            field: _filter_aligned_values(values, selection_mask)
            for field, values in data_batch.items()
            if field != "object_id"
        }
    )
    return filtered_data_batch


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
    writer_config = config.get("hyrax_alerts", {}).get("writers", {})
    for writer_friendly_name, writer in writer_config.items():
        writer_class = WRITER_REGISTRY.get(writer["writer_class"])
        if writer_class:
            writers.append(writer_class(writer))
            logger.info(f"Created writer '{writer_friendly_name}' of class '{writer['writer_class']}'")
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
            output_location = "./results"
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

    def post_filter(self, result_batch: list | dict[str, list]) -> list[bool]:
        """Return a boolean selector aligned to ``result_batch``. Users can
        provide their own implementations by specifying the dotted path to a callable
        function in the configuration file.

        For example:
        .. code-block:: toml

            [hyrax_alerts.writers.to_disk]
            writer_class = "HyraxAlertsDiskWriter"
            output_location = "./results"
            post_filter = "hyrax_alerts.example_functions.example_post_filter"

        Parameters
        ----------
        result_batch : list | dict[str, list]
            A batch of results to be filtered.

        Returns
        -------
        list[bool]
            A list of booleans indicating which results to keep.
        """
        return [True] * _aligned_batch_length(result_batch)

    def _post_filter_batches(
        self, data_batch: dict, result_batch: list | dict[str, list]
    ) -> tuple[dict, list | dict[str, list]]:
        """Filter result and data batches while preserving their alignment."""
        batch_length = len(data_batch["object_id"])
        result_batch_length = _aligned_batch_length(result_batch)

        if batch_length != result_batch_length:
            raise ValueError("data_batch and result_batch must be the same length to preserve alignment")

        selection = self.post_filter(result_batch)
        # convert the returned selection to a numpy bool array for faster filtering
        selection_mask = np.asarray(selection, dtype=bool)

        if len(selection_mask) != result_batch_length:
            raise ValueError("post_filter must return a boolean selector with one entry per result")

        # filter the data batch using the selection mask
        filtered_data_batch = _filter_data_batch(data_batch, selection_mask)

        # filter the result batch using the same helper to preserve input types
        filtered_result_batch = _filter_aligned_values(result_batch, selection_mask)
        return filtered_data_batch, filtered_result_batch

    def write(self, data_batch: dict, result_batch: list | dict[str, list]):
        """Abstract method to write a batch of results. Subclasses must implement
        this method.

        Parameters
        ----------
        data_batch : dict
            A batch of input data corresponding to the results.
        result_batch : list | dict[str, list]
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
