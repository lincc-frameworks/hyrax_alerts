from types import MethodType

from hyrax.plugin_utils import update_registry

from hyrax_alerts.callable_loader import load_callable

CONSUMER_REGISTRY = {}


class HyraxAlertsBaseConsumer:
    """Base class for HyraxAlerts consumers.

    Each subclass will be registered in the CONSUMER_REGISTRY, allowing for dynamic
    instantiation based on configuration.

    The pre_process and pre_filter methods can be specified in the configuration
    file as dotted paths to callable functions. These methods will be bound to the
    consumer instance, allowing for custom processing and filtering of results.
    """

    def __init__(self, config, data_location=None):
        """
        Initialize the base consumer with any necessary parameters.
        """
        self._config = config
        consumer_config = self.config.get("hyrax_alerts", {}).get("consumer", {})
        if consumer_config.get("pre_process"):
            self._register_pre_process(consumer_config["pre_process"])
        if consumer_config.get("pre_filter"):
            self._register_pre_filter(consumer_config["pre_filter"])

    def __init_subclass__(cls):
        """Automatically register subclasses in the CONSUMER_REGISTRY."""
        update_registry(CONSUMER_REGISTRY, cls.__name__, cls)

    @property
    def config(self):
        """Config property to access the consumer's configuration."""
        return self._config

    def _register_pre_process(self, function: str):
        if isinstance(function, str):
            function = load_callable(function)
        self.pre_process = MethodType(function, self)

    def _register_pre_filter(self, function: str):
        if isinstance(function, str):
            function = load_callable(function)
        self.pre_filter = MethodType(function, self)

    def pre_process(self, input_batch):
        """Default implementation that simply returns the input batch. Users can
        provide their own implementations by specifying the dotted path to a callable
        function in the configuration file.

        Parameters
        ----------
        input_batch : list
            A batch of data to be pre-processed.

        Returns
        -------
        list
            The pre-processed batch of data.
        """
        return input_batch

    def pre_filter(self, input_batch: list) -> list[bool]:
        """Return a boolean selector aligned to ``input_batch``. Users can
        provide their own implementations by specifying the dotted path to a callable
        function in the configuration file.

        Parameters
        ----------
        input_batch : list
            A batch of data to be filtered.

        Returns
        -------
        list
            A list of boolean values indicating which items in the batch should be kept.
        """
        return [True] * len(input_batch)
