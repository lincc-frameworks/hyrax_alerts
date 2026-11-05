"""Tests for AlerceConsumer's ability to decode ALeRCE's classifier output streams.

Mirrors the approach in tests/hyrax_alerts/test_babamul_consumer.py: a FakeConsumer
stands in for confluent_kafka.Consumer so these tests never touch a real broker.

ALeRCE publishes Avro object container files, so -- unlike Fink, whose schema arrives in
the Kafka message key -- the writer schema travels inside the message value and the
fixtures below are built with plain ``fastavro.writer``.
"""

import io

import fastavro
import hyrax
import numpy as np
import pytest
from hyrax.datasets.data_provider import CollationMixin
from hyrax_alerts.consumers.alerce_consumer import (
    AlerceConsumer,
    AlerceLightCurveClassifierConsumer,
    AlerceStampClassifierConsumer,
)

STAMP_CLASSIFIER_SCHEMA = {
    "doc": "Early Classification",
    "name": "stamp_probabilities",
    "type": "record",
    "fields": [
        {"name": "objectId", "type": "string"},
        {"name": "candid", "type": "long"},
        {
            "name": "probabilities",
            "type": {
                "name": "probabilities",
                "type": "record",
                "fields": [
                    {"name": "SN", "type": "float"},
                    {"name": "AGN", "type": "float"},
                    {"name": "VS", "type": "float"},
                    {"name": "asteroid", "type": "float"},
                    {"name": "bogus", "type": "float"},
                ],
            },
        },
    ],
}

# The real lc_classifier payload also carries a ~180-field `features` record; the
# consumer deliberately does not reach into it, so the fixture keeps a token map.
LC_CLASSIFIER_SCHEMA = {
    "doc": "Late Classification",
    "name": "probabilities_and_features",
    "type": "record",
    "fields": [
        {"name": "oid", "type": "string"},
        {"name": "candid", "type": "long"},
        {
            "name": "lc_classification",
            "type": {
                "name": "late_record",
                "type": "record",
                "fields": [
                    {"name": "probabilities", "type": {"type": "map", "values": "float"}},
                    {"name": "class", "type": "string"},
                ],
            },
        },
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


def _stamp_record(object_id="ZTF20aaelulu", candid=1234, sn=0.8):
    """Build a single stamp classifier record matching STAMP_CLASSIFIER_SCHEMA."""
    return {
        "objectId": object_id,
        "candid": candid,
        "probabilities": {"SN": sn, "AGN": 0.1, "VS": 0.05, "asteroid": 0.03, "bogus": 0.02},
    }


def _lc_record(oid="ZTF20aaelulu", candid=1234, winning_class="SNIa"):
    """Build a single light curve classifier record matching LC_CLASSIFIER_SCHEMA."""
    return {
        "oid": oid,
        "candid": candid,
        "lc_classification": {
            "probabilities": {"SNIa": 0.7, "SNII": 0.2, "AGN": 0.1},
            "class": winning_class,
        },
    }


def _encode_avro_alert(record, schema):
    """Avro-encode a single record into an Avro object container file's bytes."""
    buffer = io.BytesIO()
    fastavro.writer(buffer, schema, [record])
    return buffer.getvalue()


def _make_message(record, schema):
    return FakeMessage(_encode_avro_alert(record, schema))


def _build_consumer(
    consumer_class=AlerceConsumer,
    batch_size=5,
    batch_flush_timeout=100.0,
    consumer_config=None,
    id_field="candid",
    topics="test-topic",
):
    """Construct an ALeRCE consumer with a configured topic and batch settings."""
    h = hyrax.Hyrax()
    h.config["data_loader"]["batch_size"] = batch_size
    ds_config = h.config["data_set"]["KafkaStreamDataset"]
    ds_config["topics"] = topics
    ds_config["batch_flush_timeout"] = batch_flush_timeout
    consumer_config = dict(consumer_config or {})
    consumer_config["AlerceConsumer"] = {"id_field": id_field}
    h.config["hyrax_alerts"] = {"consumer": consumer_config}
    return consumer_class(h.config)


def _patch_consumer(monkeypatch, consumer, messages, stop_when_exhausted=False):
    """Make the consumer use a single FakeConsumer over ``messages``."""
    on_exhausted = consumer.stop if stop_when_exhausted else None
    fake_consumer = FakeConsumer(messages, on_exhausted=on_exhausted)
    monkeypatch.setattr(consumer, "_make_consumer", lambda: fake_consumer)
    return fake_consumer


# --- decoding ------------------------------------------------------------------------


def test_decodes_an_avro_object_container_message():
    """ALeRCE embeds the writer schema in the value, so decoding needs nothing else."""
    consumer = _build_consumer()

    decoded = consumer._decode(_make_message(_stamp_record(candid=99), STAMP_CLASSIFIER_SCHEMA))

    assert decoded["candid"] == 99
    assert decoded["objectId"] == "ZTF20aaelulu"
    assert decoded["probabilities"]["SN"] == pytest.approx(0.8)


def test_ingests_alerce_alerts_into_batches(monkeypatch):
    """ALeRCE-framed Kafka messages should batch like any other stream."""
    consumer = _build_consumer(batch_size=3)
    messages = [_make_message(_stamp_record(candid=i), STAMP_CLASSIFIER_SCHEMA) for i in range(3)]
    fake_consumer = _patch_consumer(monkeypatch, consumer, messages, stop_when_exhausted=True)

    batches = list(consumer)

    assert len(batches) == 1
    assert [sample["candid"] for sample in batches[0]] == [0, 1, 2]
    assert fake_consumer.closed


def test_a_regex_topic_is_passed_through_untouched():
    """ALeRCE's topics rotate nightly, and the intended answer is a librdkafka regex
    subscription rather than any topic-rolling code here. Nothing in the consumer may
    mangle a "^"-prefixed topic on its way to subscribe()."""
    consumer = _build_consumer(topics="^lc_classifier_[0-9]{8}")

    assert consumer.topics == ["^lc_classifier_[0-9]{8}"]


# --- getters -------------------------------------------------------------------------


def test_get_candid_reads_the_configured_id_field():
    """ALeRCE's ZTF-era topics key on candid, which is the default."""
    consumer = _build_consumer()

    assert consumer.get_candid(_stamp_record(candid=5150)) == 5150


def test_get_candid_honors_an_overridden_id_field():
    """id_field is what lets the same class serve a payload keyed on something else,
    which is how an ALeRCE LSST stream keyed on diaSourceId would be handled."""
    consumer = _build_consumer(id_field="diaSourceId")

    assert consumer.get_candid({"diaSourceId": 777, "candid": 1}) == 777


def test_stamp_classifier_getters_read_each_class_probability():
    """The stamp classifier's probabilities are a fixed-field record, so every class
    probability is a plain scalar lookup."""
    consumer = _build_consumer(consumer_class=AlerceStampClassifierConsumer)
    alert = _stamp_record()

    assert consumer.get_object_id(alert) == "ZTF20aaelulu"
    assert consumer.get_sn_prob(alert) == pytest.approx(0.8)
    assert consumer.get_agn_prob(alert) == pytest.approx(0.1)
    assert consumer.get_vs_prob(alert) == pytest.approx(0.05)
    assert consumer.get_asteroid_prob(alert) == pytest.approx(0.03)
    assert consumer.get_bogus_prob(alert) == pytest.approx(0.02)


def test_stamp_classifier_getters_survive_an_avro_round_trip():
    """The getters must work on a decoded alert, not just a hand-built dict."""
    consumer = _build_consumer(consumer_class=AlerceStampClassifierConsumer)

    decoded = consumer._decode(_make_message(_stamp_record(candid=7), STAMP_CLASSIFIER_SCHEMA))

    assert consumer.get_candid(decoded) == 7
    assert consumer.get_sn_prob(decoded) == pytest.approx(0.8)


def test_light_curve_classifier_getters_read_the_winning_class():
    """Only the scalars are exposed; the probabilities map is left in the alert."""
    consumer = _build_consumer(consumer_class=AlerceLightCurveClassifierConsumer)
    alert = _lc_record(winning_class="SNIa")

    assert consumer.get_oid(alert) == "ZTF20aaelulu"
    assert consumer.get_class(alert) == "SNIa"


def test_light_curve_classifier_getters_survive_an_avro_round_trip():
    """The getters must work on a decoded alert, not just a hand-built dict."""
    consumer = _build_consumer(consumer_class=AlerceLightCurveClassifierConsumer)

    decoded = consumer._decode(_make_message(_lc_record(candid=7), LC_CLASSIFIER_SCHEMA))

    assert consumer.get_candid(decoded) == 7
    assert consumer.get_class(decoded) == "SNIa"
    # The map-valued field is still reachable for a pre_process function or subclass.
    assert decoded["lc_classification"]["probabilities"]["SNIa"] == pytest.approx(0.7)


def test_no_collate_hooks_are_defined():
    """These consumers deliberately ship scalar getters only, so they define no
    collate_* hooks. If someone adds a vectorized probabilities or features getter
    later, this is the reminder that it needs a matching collation function."""
    for consumer_class in (AlerceStampClassifierConsumer, AlerceLightCurveClassifierConsumer):
        hooks = [name for name in dir(consumer_class) if name.startswith("collate_")]
        assert hooks == [], f"{consumer_class.__name__} defines {hooks}"


def test_scalar_getters_collate_with_hyrax_defaults():
    """Because every getter returns a scalar, all fields are fixed-shape and hyrax's
    default_field_collate stacks them without any custom hook."""
    consumer = _build_consumer(consumer_class=AlerceStampClassifierConsumer)
    alerts = [_stamp_record(candid=i, sn=0.1 * i) for i in range(4)]

    # Mirror StreamingDataProvider._structure, which wraps each getter's return value.
    samples = [{"sn_prob": np.asarray(consumer.get_sn_prob(a))} for a in alerts]

    collated = CollationMixin.default_field_collate(samples, "sn_prob", "alerce")

    assert collated["sn_prob"].shape == (4,)
    np.testing.assert_allclose(collated["sn_prob"], [0.0, 0.1, 0.2, 0.3], rtol=1e-6)
