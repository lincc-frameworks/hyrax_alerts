"""Tests for HyraxKafkaConsumer's ability to ingest mocked Kafka alerts.

A FakeConsumer stands in for confluent_kafka.Consumer (mirroring the approach in
hyrax's tests/hyrax/test_kafka_stream_dataset.py) so these tests never touch a real
broker. The ingestion loop below mirrors ``process_alerts`` in
src/hyrax_alerts/process_alerts.py: pull a batch from the stream, then run it through
``pre_filter`` and ``pre_process``.
"""

import json

import hyrax
from hyrax_alerts.consumers.kafka_consumer import HyraxKafkaConsumer


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


def _make_message(object_id, image):
    return FakeMessage(json.dumps({"object_id": object_id, "image": image}))


def _build_consumer(batch_size=5, batch_flush_timeout=100.0, consumer_config=None):
    """Construct a HyraxKafkaConsumer with a configured topic and batch settings."""
    h = hyrax.Hyrax()
    h.config["data_loader"]["batch_size"] = batch_size
    ds_config = h.config["data_set"]["KafkaStreamDataset"]
    ds_config["topics"] = "test-topic"
    ds_config["batch_flush_timeout"] = batch_flush_timeout
    h.config["hyrax_alerts"] = {"consumer": consumer_config or {}}
    return HyraxKafkaConsumer(h.config)


def _patch_consumer(monkeypatch, consumer, messages, stop_when_exhausted=False):
    """Make the consumer use a single FakeConsumer over ``messages``."""
    on_exhausted = consumer.stop if stop_when_exhausted else None
    fake_consumer = FakeConsumer(messages, on_exhausted=on_exhausted)
    monkeypatch.setattr(consumer, "_make_consumer", lambda: fake_consumer)
    return fake_consumer


def test_ingests_mocked_kafka_alerts_like_process_alerts(monkeypatch):
    """HyraxKafkaConsumer should decode mocked Kafka alerts into batches, and those
    batches should flow through pre_filter/pre_process the same way process_alerts
    drives a real consumer."""
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


def test_pre_filter_and_pre_process_default_to_noop_on_ingested_batch(monkeypatch):
    """Without any consumer config, pre_process is an identity function and pre_filter
    returns an all-True selector aligned to the ingested batch."""
    consumer = _build_consumer(batch_size=2)
    messages = [_make_message("a", [[1.0]]), _make_message("b", [[2.0]])]
    _patch_consumer(monkeypatch, consumer, messages, stop_when_exhausted=True)

    batches = list(consumer)
    assert len(batches) == 1

    batch = batches[0]
    assert consumer.pre_process(batch) is batch
    assert consumer.pre_filter(batch) == batch


def test_consumer_closed_after_ingesting_mocked_alerts(monkeypatch):
    """The underlying Kafka consumer is closed once the mocked alert stream ends."""
    consumer = _build_consumer(batch_size=2)
    messages = [_make_message("a", [[1.0]]), _make_message("b", [[2.0]])]
    fake_consumer = _patch_consumer(monkeypatch, consumer, messages, stop_when_exhausted=True)

    list(consumer)

    assert fake_consumer.closed


def test_peek_sample_pre_filter(monkeypatch):
    """peek_sample respects a pre_filter hook, skipping messages that return None."""
    dataset = _build_consumer(batch_size=5, batch_flush_timeout=0.0)
    messages = [_make_message("first", [[1.0]]), _make_message("second", [[2.0]])]
    _patch_consumer(monkeypatch, dataset, messages, stop_when_exhausted=True)

    def _test_pre_filter(self, batch):
        # Only allow the second message to be processed
        good_batch = []
        for s in batch:
            if s["object_id"] == "second":
                good_batch.append(s)
        return good_batch

    dataset.pre_filter = lambda sample: _test_pre_filter(dataset, sample)

    peeked = dataset.peek_sample()
    assert peeked["object_id"] == "second"
    # also ensure that buffer was wiped
    assert len(dataset._buffered) == 1
    assert dataset._buffered[0]["object_id"] == "second"


def test_peek_sample_pre_process(monkeypatch):
    """peek_sample respects a pre_process hook, transforming messages before returning."""
    dataset = _build_consumer(batch_size=5, batch_flush_timeout=0.0)
    messages = [_make_message("first", [[1.0]])]
    _patch_consumer(monkeypatch, dataset, messages, stop_when_exhausted=True)

    def _test_pre_process(self, batch):
        # Transform the message by adding a new field
        for s in batch:
            print(s)
            s["image"] = [[2.0]]
        return batch

    dataset.pre_process = lambda sample: _test_pre_process(dataset, sample)

    peeked = dataset.peek_sample()
    assert peeked["object_id"] == "first"
    assert peeked["image"] == [[2.0]]
