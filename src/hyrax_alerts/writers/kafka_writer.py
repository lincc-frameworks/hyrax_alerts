from hyrax_alerts.logging_utils import get_logger
from hyrax_alerts.writers.base_writer import HyraxAlertsBaseWriter
from hyrax_alerts.writers.serialization_utils import json_dumps

# NOTE: This writer publishes one JSON message per alert, keyed by object id, so
# that every alert for a given object lands on the same partition and downstream
# consumers see them in order. The whole record is published - the input fields
# plus the model output under `__hyrax_result`. Trimming a record down before it
# goes on the wire is a job for post_process, not for this writer.

# NOTE: confluent_kafka's Producer is thread-safe, and each writer instance owns
# exactly one Producer. write_batch also submits only one future per writer per
# batch and waits on all of them before starting the next batch, so at most one
# worker thread is inside write() for a given instance at a time.

# NOTE: The confluent_kafka import is deferred into _load_producer_class so that
# hyrax_alerts.writers can import this module - and therefore register this
# writer - without the `kafka` extra installed.

logger = get_logger(__name__)

# Which record field supplies the Kafka message key.
DEFAULT_KEY_FIELD = "object_id"

# How long close() waits for outstanding messages to be delivered.
DEFAULT_FLUSH_TIMEOUT_SECONDS = 30.0

# How long to block serving delivery callbacks when the local produce queue is
# full, before retrying the produce once.
DEFAULT_BUFFER_FULL_POLL_SECONDS = 1.0

KAFKA_IMPORT_ERROR = (
    "HyraxAlertsKafkaWriter requires confluent-kafka. Install it with: pip install 'hyrax_alerts[kafka]'"
)


def _load_producer_class():
    """Import and return ``confluent_kafka.Producer``.

    Returns
    -------
    type
        The ``confluent_kafka.Producer`` class.

    Raises
    ------
    ImportError
        If confluent-kafka is not installed, with a message naming the extra
        that provides it.
    """
    try:
        from confluent_kafka import Producer
    except ImportError as error:
        raise ImportError(KAFKA_IMPORT_ERROR) from error
    return Producer


def _delivery_report(error, message):
    """Log Kafka delivery failures.

    Passed to ``Producer.produce`` as ``on_delivery`` and invoked from ``poll``
    and ``flush``, on whichever thread makes that call.

    Parameters
    ----------
    error : object | None
        The delivery error, or ``None`` when the message was delivered.
    message : object
        The message the report is about.

    Returns
    -------
    None
    """
    if error is not None:
        logger.error(f"Kafka delivery failed for key {message.key()!r}: {error}")


def _message_key(record: dict, key_field: str = DEFAULT_KEY_FIELD) -> bytes | None:
    """Build the Kafka message key for one record.

    Parameters
    ----------
    record : dict
        One alert record from a result batch.
    key_field : str, optional
        The record field supplying the key, by default ``DEFAULT_KEY_FIELD``.

    Returns
    -------
    bytes | None
        The utf-8 encoded key, or ``None`` when the record has no such field.
        A ``None`` key lets Kafka assign the partition rather than dropping the
        alert.
    """
    value = record.get(key_field)
    return None if value is None else str(value).encode("utf-8")


def _message_value(record: dict) -> bytes:
    """Build the utf-8 encoded JSON body for one record.

    Parameters
    ----------
    record : dict
        One alert record from a result batch.

    Returns
    -------
    bytes
        The record serialized as JSON and encoded utf-8.
    """
    return json_dumps(record).encode("utf-8")


class HyraxAlertsKafkaWriter(HyraxAlertsBaseWriter):
    """Writer class for producing one JSON message per alert to a Kafka topic.

    Requires the ``kafka`` extra: ``pip install 'hyrax_alerts[kafka]'``.

    Each record is published as a single JSON message whose key is the record's
    ``object_id`` encoded as utf-8, so every alert for one object lands on the
    same partition. numpy arrays and scalars are converted to plain JSON types
    and byte fields are base64 encoded - see
    :mod:`hyrax_alerts.writers.serialization_utils`.

    Failures are logged rather than raised: a broker problem should not abort a
    long-running processing run.

    Required configuration keys:

    - ``bootstrap_servers``: a comma separated ``host:port`` list.
    - ``topic``: the topic to produce to.

    Optional configuration keys:

    - ``producer_config``: a table merged into the ``confluent_kafka.Producer``
      configuration, for keys such as ``security.protocol``, ``sasl.mechanism``,
      ``sasl.username``, ``sasl.password``, ``ssl.ca.location``, and
      ``compression.type``. A ``bootstrap.servers`` set here is overridden by
      the top level ``bootstrap_servers`` key.
    - ``key_field``: which record field supplies the message key
      (default ``"object_id"``). Records without that field are produced with
      no key.
    - ``flush_timeout``: seconds to wait for outstanding messages when the
      writer is closed (default ``30``).

    Example configuration:

    .. code-block:: toml

        [hyrax_alerts.writers.to_kafka_0]
        writer_class = "HyraxAlertsKafkaWriter"
        bootstrap_servers = "localhost:9092"
        topic = "hyrax-alerts-out"

        [hyrax_alerts.writers.to_kafka_0.producer_config]
        "security.protocol" = "SASL_SSL"
        "sasl.mechanism" = "SCRAM-SHA-512"
    """

    def __init__(self, config):
        super().__init__(config)

        self.bootstrap_servers = config.get("bootstrap_servers")
        if not isinstance(self.bootstrap_servers, str) or not self.bootstrap_servers.strip():
            raise ValueError("HyraxAlertsKafkaWriter requires a non-empty string 'bootstrap_servers'.")

        self.topic = config.get("topic")
        if not isinstance(self.topic, str) or not self.topic.strip():
            raise ValueError("HyraxAlertsKafkaWriter requires a non-empty string 'topic'.")

        self.key_field = config.get("key_field", DEFAULT_KEY_FIELD)
        self.flush_timeout = config.get("flush_timeout", DEFAULT_FLUSH_TIMEOUT_SECONDS)

        # Passthrough first, explicit key second: 'bootstrap_servers' is a
        # required, documented key, so it wins over a 'bootstrap.servers' buried
        # in producer_config.
        producer_config = {
            **config.get("producer_config", {}),
            "bootstrap.servers": self.bootstrap_servers,
        }

        producer_class = _load_producer_class()
        self.producer = producer_class(producer_config)

    def write(self, result_batch: list[dict]):
        """Produce one JSON message per record to the configured topic.

        Parameters
        ----------
        result_batch : list[dict]
            A batch of post-processed, post-filtered alert records.

        Returns
        -------
        None
        """
        for record in result_batch:
            try:
                value = _message_value(record)
            except Exception as error:
                logger.error(f"Skipping an alert record that could not be serialized to JSON: {error}")
                continue

            self._produce(_message_key(record, self.key_field), value)

            # Serve delivery callbacks as we go, so the queue drains and
            # failures are reported promptly rather than only at flush time.
            self.producer.poll(0)

    def _produce(self, key, value):
        """Produce one message, retrying once if the local queue is full.

        Parameters
        ----------
        key : bytes | None
            The Kafka message key.
        value : bytes
            The Kafka message body.

        Returns
        -------
        None
        """
        try:
            self.producer.produce(self.topic, key=key, value=value, on_delivery=_delivery_report)
            return
        except BufferError:
            logger.warning("Kafka produce queue is full; draining delivery callbacks and retrying.")
        except Exception as error:
            logger.error(f"Failed to produce alert {key!r} to topic '{self.topic}': {error}")
            return

        self.producer.poll(DEFAULT_BUFFER_FULL_POLL_SECONDS)
        try:
            self.producer.produce(self.topic, key=key, value=value, on_delivery=_delivery_report)
        except Exception as error:
            logger.error(f"Dropping alert {key!r}; Kafka produce queue is still full: {error}")

    def __enter__(self):
        """Enter the context manager for the writer."""
        return self

    def close(self):
        """Flush buffered messages and report anything left undelivered."""
        remaining = self.producer.flush(self.flush_timeout)
        if remaining:
            logger.error(
                f"{remaining} Kafka message(s) were still undelivered after "
                f"{self.flush_timeout}s and have been dropped."
            )
