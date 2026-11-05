import json
import logging

import numpy as np
import pytest
from hyrax_alerts.writers import kafka_writer
from hyrax_alerts.writers.base_writer import WRITER_REGISTRY
from hyrax_alerts.writers.kafka_writer import (
    KAFKA_IMPORT_ERROR,
    HyraxAlertsKafkaWriter,
    _delivery_report,
    _message_key,
)

LOGGER_NAME = "hyrax.alerts.writers.kafka_writer"


class FakeProducer:
    """Stands in for ``confluent_kafka.Producer``, recording what it is asked to do."""

    def __init__(self, config=None):
        self.config = config or {}
        self.produced = []
        self.poll_timeouts = []
        self.flush_timeouts = []
        self.flush_result = 0
        # Exceptions to raise from successive produce() calls, oldest first.
        self.produce_errors = []

    def produce(self, topic, key=None, value=None, on_delivery=None):
        """Record a message, or raise the next queued error."""
        if self.produce_errors:
            raise self.produce_errors.pop(0)
        self.produced.append({"topic": topic, "key": key, "value": value, "on_delivery": on_delivery})

    def poll(self, timeout):
        """Record a poll."""
        self.poll_timeouts.append(timeout)
        return 0

    def flush(self, timeout):
        """Record a flush and report the configured number of stragglers."""
        self.flush_timeouts.append(timeout)
        return self.flush_result


@pytest.fixture(autouse=True)
def fake_producer_class(monkeypatch):
    """Build writers against FakeProducer, so confluent-kafka is never needed."""
    monkeypatch.setattr(kafka_writer, "_load_producer_class", lambda: FakeProducer)


def _valid_config(**overrides):
    """Return a minimal valid Kafka writer config, with optional overrides."""
    config = {"bootstrap_servers": "localhost:9092", "topic": "test-topic"}
    config.update(overrides)
    return config


def _records(object_ids):
    """Return a batch of records in the shape writers receive them."""
    return [
        {"object_id": object_id, "__hyrax_result": {"data": index}}
        for index, object_id in enumerate(object_ids)
    ]


def test_init_requires_bootstrap_servers():
    """The writer raises when no bootstrap_servers is provided."""
    with pytest.raises(ValueError, match="bootstrap_servers"):
        HyraxAlertsKafkaWriter(config={"topic": "test-topic"})


def test_init_requires_topic():
    """The writer raises when no topic is provided."""
    with pytest.raises(ValueError, match="topic"):
        HyraxAlertsKafkaWriter(config={"bootstrap_servers": "localhost:9092"})


def test_writer_registered_in_registry():
    """Defining the subclass registers it in the shared writer registry."""
    assert WRITER_REGISTRY.get("HyraxAlertsKafkaWriter") is HyraxAlertsKafkaWriter


def test_producer_receives_bootstrap_servers():
    """The configured brokers reach the producer under confluent-kafka's key."""
    writer = HyraxAlertsKafkaWriter(config=_valid_config())

    assert writer.producer.config["bootstrap.servers"] == "localhost:9092"


def test_explicit_bootstrap_servers_overrides_passthrough():
    """The documented top level key wins over one buried in producer_config."""
    writer = HyraxAlertsKafkaWriter(
        config=_valid_config(producer_config={"bootstrap.servers": "ignored:9092"})
    )

    assert writer.producer.config["bootstrap.servers"] == "localhost:9092"


def test_producer_config_passthrough_keys_are_preserved():
    """Other producer_config keys are handed to the producer untouched."""
    writer = HyraxAlertsKafkaWriter(
        config=_valid_config(
            producer_config={"security.protocol": "SASL_SSL", "sasl.mechanism": "SCRAM-SHA-512"}
        )
    )

    assert writer.producer.config["security.protocol"] == "SASL_SSL"
    assert writer.producer.config["sasl.mechanism"] == "SCRAM-SHA-512"


def test_credentials_file_keys_reach_producer(tmp_path):
    """Keys from a credentials file are merged into the producer configuration."""
    creds = tmp_path / "kafka_credentials.toml"
    creds.write_bytes(b'"sasl.username" = "alice"\n"sasl.password" = "secret"\n')

    writer = HyraxAlertsKafkaWriter(config=_valid_config(credentials_file=str(creds)))

    assert writer.producer.config["sasl.username"] == "alice"
    assert writer.producer.config["sasl.password"] == "secret"


def test_credentials_file_overridden_by_producer_config(tmp_path):
    """Explicit producer_config keys take precedence over those in credentials_file."""
    creds = tmp_path / "kafka_credentials.toml"
    creds.write_bytes(b'"sasl.username" = "from_file"\n')

    writer = HyraxAlertsKafkaWriter(
        config=_valid_config(
            credentials_file=str(creds),
            producer_config={"sasl.username": "from_config"},
        )
    )

    assert writer.producer.config["sasl.username"] == "from_config"


def test_credentials_file_not_found_raises(tmp_path):
    """The writer raises a clear error when the credentials_file does not exist."""
    with pytest.raises(ValueError, match="does not exist"):
        HyraxAlertsKafkaWriter(
            config=_valid_config(credentials_file=str(tmp_path / "missing.toml"))
        )


def test_credentials_file_must_be_a_regular_file(tmp_path):
    """The writer rejects directories and other non-file paths to credentials_file."""
    bad_path = tmp_path / "credentials_dir"
    bad_path.mkdir()

    with pytest.raises(ValueError, match="not a regular file"):
        HyraxAlertsKafkaWriter(config=_valid_config(credentials_file=str(bad_path)))


def test_write_produces_one_message_per_record():
    """Each record in the batch becomes its own message on the configured topic."""
    writer = HyraxAlertsKafkaWriter(config=_valid_config())

    writer.write(_records([101, 102, 103]))

    assert len(writer.producer.produced) == 3
    assert {message["topic"] for message in writer.producer.produced} == {"test-topic"}


def test_message_key_is_the_utf8_object_id():
    """The message key is the object id, so an object's alerts share a partition."""
    writer = HyraxAlertsKafkaWriter(config=_valid_config())

    writer.write(_records([101]))

    assert writer.producer.produced[0]["key"] == b"101"


def test_message_key_is_none_when_the_key_field_is_missing():
    """A record with no key field is still produced, with the key left unset."""
    assert _message_key({"__hyrax_result": {}}) is None


def test_key_field_is_configurable():
    """A different record field can supply the message key."""
    writer = HyraxAlertsKafkaWriter(config=_valid_config(key_field="survey_id"))

    writer.write([{"object_id": 101, "survey_id": "ZTF123"}])

    assert writer.producer.produced[0]["key"] == b"ZTF123"


def test_message_value_is_json_with_numpy_coerced():
    """numpy values in the record survive as plain JSON."""
    writer = HyraxAlertsKafkaWriter(config=_valid_config())

    writer.write([{"object_id": 101, "__hyrax_result": {"data": np.array([0.25, 0.75])}}])

    payload = json.loads(writer.producer.produced[0]["value"].decode("utf-8"))
    assert payload == {"object_id": 101, "__hyrax_result": {"data": [0.25, 0.75]}}


def test_write_polls_the_producer():
    """The producer is polled as messages are produced, so callbacks fire promptly."""
    writer = HyraxAlertsKafkaWriter(config=_valid_config())

    writer.write(_records([101, 102]))

    assert writer.producer.poll_timeouts == [0, 0]


def test_write_swallows_produce_errors_and_continues():
    """A failed produce is logged, and the rest of the batch is still attempted."""
    writer = HyraxAlertsKafkaWriter(config=_valid_config())
    writer.producer.produce_errors = [RuntimeError("boom")]

    # Should not raise.
    writer.write(_records([101, 102]))

    assert len(writer.producer.produced) == 1
    assert writer.producer.produced[0]["key"] == b"102"


def test_buffer_error_triggers_a_blocking_poll_and_one_retry():
    """A full queue is drained and the message is produced on the second attempt."""
    writer = HyraxAlertsKafkaWriter(config=_valid_config())
    writer.producer.produce_errors = [BufferError()]

    writer.write(_records([101]))

    assert len(writer.producer.produced) == 1
    assert kafka_writer.DEFAULT_BUFFER_FULL_POLL_SECONDS in writer.producer.poll_timeouts


def test_persistent_buffer_error_drops_the_message():
    """A queue that stays full drops the alert rather than aborting the run."""
    writer = HyraxAlertsKafkaWriter(config=_valid_config())
    writer.producer.produce_errors = [BufferError(), BufferError()]

    # Should not raise.
    writer.write(_records([101]))

    assert writer.producer.produced == []


def test_unserializable_record_is_skipped_not_fatal(monkeypatch):
    """A record that cannot be serialized is skipped; the batch keeps going."""
    writer = HyraxAlertsKafkaWriter(config=_valid_config())

    def _fail_on_first(record):
        if record["object_id"] == 101:
            raise TypeError("nope")
        return b"{}"

    monkeypatch.setattr(kafka_writer, "_message_value", _fail_on_first)

    writer.write(_records([101, 102]))

    assert len(writer.producer.produced) == 1
    assert writer.producer.produced[0]["key"] == b"102"


def test_close_flushes_the_producer():
    """Closing the writer flushes with the configured timeout."""
    writer = HyraxAlertsKafkaWriter(config=_valid_config(flush_timeout=5))

    writer.close()

    assert writer.producer.flush_timeouts == [5]


def test_close_logs_undelivered_messages(caplog):
    """Messages still outstanding after the flush timeout are reported."""
    writer = HyraxAlertsKafkaWriter(config=_valid_config())
    writer.producer.flush_result = 5

    with caplog.at_level(logging.ERROR, logger=LOGGER_NAME):
        writer.close()

    assert "5 Kafka message(s)" in caplog.text


def test_delivery_report_logs_failures(caplog):
    """A failed delivery is reported, and a successful one is silent."""

    class FakeMessage:
        def key(self):
            return b"101"

    with caplog.at_level(logging.ERROR, logger=LOGGER_NAME):
        _delivery_report(None, FakeMessage())
        assert caplog.text == ""

        _delivery_report("broker down", FakeMessage())
        assert "broker down" in caplog.text


def test_import_error_names_the_extra():
    """The missing-dependency message tells the user which extra to install."""
    assert "hyrax_alerts[kafka]" in KAFKA_IMPORT_ERROR
