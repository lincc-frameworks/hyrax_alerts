"""Tests for BabamulConsumer's ability to decode Avro-encoded Babamul alerts.

Mirrors the approach in tests/hyrax_alerts/test_kafka_consumer.py: a FakeConsumer
stands in for confluent_kafka.Consumer so these tests never touch a real broker.
Unlike HyraxKafkaConsumer (which decodes JSON), BabamulConsumer decodes messages via
babamul.avro.deserialize_alert, so the messages here are real Avro-encoded bytes
(built with fastavro) rather than JSON strings.
"""

import io

import fastavro
import hyrax
from hyrax_alerts.consumers.babamul_consumer import BabamulConsumer

ALERT_SCHEMA = {
    "type": "record",
    "name": "Alert",
    "fields": [
        {"name": "object_id", "type": "string"},
        {"name": "image", "type": {"type": "array", "items": {"type": "array", "items": "double"}}},
    ],
}


class FakeMessage:
    """Minimal stand-in for a confluent_kafka Message."""

    def __init__(self, value, error=None):
        self._value = value
        self._error = error

    def value(self):
        """Return the message payload."""
        return self._value

    def error(self):
        """Return the message error, or None for a normal message."""
        return self._error


class FakeConsumer:
    """Returns queued messages, then None (empty poll) on every subsequent call."""

    def __init__(self, messages, on_exhausted=None):
        self._messages = list(messages)
        self._on_exhausted = on_exhausted
        self.closed = False

    def poll(self, timeout):
        """Pop the next queued message, or None once the queue is empty."""
        if self._messages:
            return self._messages.pop(0)
        if self._on_exhausted is not None:
            self._on_exhausted()
        return None

    def consume(self, num_messages, timeout):
        """Drain up to num_messages queued messages; signal exhaustion when empty."""
        drained = []
        while self._messages and len(drained) < num_messages:
            drained.append(self._messages.pop(0))
        if not drained and self._on_exhausted is not None:
            self._on_exhausted()
        return drained

    def subscribe(self, topics):
        """No-op subscribe to match the confluent_kafka Consumer interface."""
        pass

    def close(self):
        """Record that the consumer was closed."""
        self.closed = True


def _encode_avro_alert(object_id, image):
    """Avro-encode a single alert record into an Avro object container file's bytes."""
    buffer = io.BytesIO()
    fastavro.writer(buffer, ALERT_SCHEMA, [{"object_id": object_id, "image": image}])
    return buffer.getvalue()


def _make_message(object_id, image):
    return FakeMessage(_encode_avro_alert(object_id, image))


def _build_consumer(batch_size=5, batch_flush_timeout=100.0, consumer_config=None, raw_alert=False):
    """Construct a BabamulConsumer with a configured topic and batch settings."""
    h = hyrax.Hyrax()
    h.config["data_loader"]["batch_size"] = batch_size
    ds_config = h.config["data_set"]["KafkaStreamDataset"]
    ds_config["topics"] = "test-topic"
    ds_config["batch_flush_timeout"] = batch_flush_timeout
    consumer_config = dict(consumer_config or {})
    consumer_config["BabamulConsumer"] = {"raw_alert": raw_alert}
    h.config["hyrax_alerts"] = {"consumer": consumer_config}
    return BabamulConsumer(h.config)


def _patch_consumer(monkeypatch, consumer, messages, stop_when_exhausted=False):
    """Make the consumer use a single FakeConsumer over ``messages``."""
    on_exhausted = consumer.stop if stop_when_exhausted else None
    fake_consumer = FakeConsumer(messages, on_exhausted=on_exhausted)
    monkeypatch.setattr(consumer, "_make_consumer", lambda: fake_consumer)
    return fake_consumer


def test_decodes_single_avro_encoded_alert():
    """BabamulConsumer._decode should deserialize an Avro-encoded message into the
    original alert dict via babamul.avro.deserialize_alert."""
    consumer = _build_consumer()
    msg = _make_message("alert-0", [[1.0, 2.0]])

    decoded = consumer._decode(msg)

    assert decoded == {"object_id": "alert-0", "image": [[1.0, 2.0]]}


def test_ingests_avro_encoded_babamul_alerts(monkeypatch):
    """BabamulConsumer should decode Avro-encoded Kafka alerts into batches, the same
    way HyraxKafkaConsumer decodes JSON ones, just with Avro on the wire."""
    consumer = _build_consumer(
        batch_size=3,
        consumer_config={
            "pre_process": "hyrax_alerts.example_functions.example_pre_process",
            "pre_filter": "hyrax_alerts.example_functions.example_pre_filter",
        },
    )
    messages = [_make_message(f"alert-{i}", [[float(i), 0.0]]) for i in range(3)]
    _patch_consumer(monkeypatch, consumer, messages, stop_when_exhausted=True)

    batches = list(consumer)
    assert len(batches) == 1

    batch = batches[0]
    batch = consumer.pre_filter(batch)
    batch = consumer.pre_process(batch)

    assert [sample["object_id"] for sample in batch] == ["alert-0", "alert-1", "alert-2"]
    assert batch[1]["image"] == [[1.0, 0.0]]


def test_consumer_closed_after_ingesting_avro_encoded_alerts(monkeypatch):
    """The underlying Kafka consumer is closed once the mocked Avro alert stream ends."""
    consumer = _build_consumer(batch_size=2)
    messages = [_make_message("a", [[1.0]]), _make_message("b", [[2.0]])]
    fake_consumer = _patch_consumer(monkeypatch, consumer, messages, stop_when_exhausted=True)

    list(consumer)

    assert fake_consumer.closed
