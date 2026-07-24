"""Logging helpers for hyrax-alerts.

Loggers returned here are parented under Hyrax's ``"hyrax"`` logger so they
automatically inherit the handler, formatter, level, and destination that
Hyrax configures (see ``Hyrax._initialize_log_handlers``). No handler or
formatter setup is required in hyrax-alerts.
"""

import logging

# Must be a child of "hyrax" so records propagate to the handler Hyrax
# attaches to the "hyrax" logger.
BASE_LOGGER_NAME = "hyrax.alerts"
_PACKAGE_PREFIX = "hyrax_alerts"


def get_logger(name: str) -> logging.Logger:
    """Return a logger under the ``hyrax.alerts.*`` namespace.

    Pass ``__name__`` from the calling module; the ``hyrax_alerts`` package
    prefix is remapped to ``hyrax.alerts`` (e.g.
    ``hyrax_alerts.writers.base_writer`` -> ``hyrax.alerts.writers.base_writer``).
    """
    if name == _PACKAGE_PREFIX:
        suffix = ""
    elif name.startswith(_PACKAGE_PREFIX + "."):
        suffix = name[len(_PACKAGE_PREFIX) + 1 :]
    else:
        suffix = name
    return logging.getLogger(f"{BASE_LOGGER_NAME}.{suffix}" if suffix else BASE_LOGGER_NAME)
