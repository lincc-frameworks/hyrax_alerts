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
import numpy as np
import pytest
from hyrax_alerts.consumers.babamul_consumer import (
    BAND_TO_IDX,
    COLLATION_LENGTH,
    LOG_CONST,
    NUM_BANDS,
    BabamulConsumer,
    BabamulPhotometryConsumer,
)

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


# --- BabamulPhotometryConsumer -------------------------------------------------------
#
# Photometry measurements arrive under the "fp_hists" key of a decoded message as a list
# of dicts shaped like:
# {'jd': 2461243.9301273, 'psfFlux': -2490725446.259132,
#  'psfFluxErr': 3465780718.5287414, 'band': 'r'}

PHOTOMETRY_ALERT_SCHEMA = {
    "type": "record",
    "name": "PhotometryAlert",
    "fields": [
        {"name": "candid", "type": "long"},
        {
            "name": "fp_hists",
            "type": {
                "type": "array",
                "items": {
                    "type": "record",
                    "name": "FpHist",
                    "fields": [
                        {"name": "jd", "type": "double"},
                        {"name": "psfFlux", "type": "double"},
                        {"name": "psfFluxErr", "type": "double"},
                        {"name": "band", "type": "string"},
                    ],
                },
            },
        },
    ],
}

SAMPLE_FP_HISTS = [
    {"jd": 2461243.9301273, "psfFlux": -2490725446.259132, "psfFluxErr": 3465780718.5287414, "band": "r"},
    {"jd": 2461244.0301273, "psfFlux": 1500.0, "psfFluxErr": 20.0, "band": "g"},
    {"jd": 2461245.0301273, "psfFlux": 3000.0, "psfFluxErr": 30.0, "band": "i"},
]


def _encode_photometry_alert(candid, fp_hists):
    """Avro-encode a single photometry alert record, mirroring _encode_avro_alert."""
    buffer = io.BytesIO()
    fastavro.writer(buffer, PHOTOMETRY_ALERT_SCHEMA, [{"candid": candid, "fp_hists": fp_hists}])
    return buffer.getvalue()


@pytest.fixture
def stats_path(tmp_path):
    """Write a small mean/std stats file for BabamulPhotometryConsumer normalization."""
    path = tmp_path / "photometry_stats.npz"
    np.savez(path, mean=np.array([1.0, 2.0, 3.0, 4.0]), std=np.array([5.0, 6.0, 7.0, 8.0]))
    return path


def _build_photometry_consumer(
    stats_path, batch_size=5, batch_flush_timeout=100.0, consumer_config=None, raw_alert=False
):
    """Construct a BabamulPhotometryConsumer with a configured topic, batch settings,
    and stats file."""
    h = hyrax.Hyrax()
    h.config["data_loader"]["batch_size"] = batch_size
    ds_config = h.config["data_set"]["KafkaStreamDataset"]
    ds_config["topics"] = "test-topic"
    ds_config["batch_flush_timeout"] = batch_flush_timeout
    consumer_config = dict(consumer_config or {})
    consumer_config["BabamulConsumer"] = {"raw_alert": raw_alert}
    consumer_config["BabamulPhotometryConsumer"] = {"stats_path": str(stats_path)}
    h.config["hyrax_alerts"] = {"consumer": consumer_config}
    return BabamulPhotometryConsumer(h.config)


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


def test_get_photometry_extracts_expected_feature_vector(stats_path):
    """get_photometry should turn the raw `fp_hists` list into an (N, 7) array of
    dt, dt_prev, log_flux, log_flux_error, and a one-hot band encoding."""
    consumer = _build_photometry_consumer(stats_path)
    msg = {"fp_hists": SAMPLE_FP_HISTS}

    photometry = consumer.get_photometry(msg)

    assert photometry.shape == (len(SAMPLE_FP_HISTS), 7)

    obstimes = np.array([obs["jd"] for obs in SAMPLE_FP_HISTS])
    fluxes = np.array([obs["psfFlux"] for obs in SAMPLE_FP_HISTS])
    flux_errors = np.array([obs["psfFluxErr"] for obs in SAMPLE_FP_HISTS])

    expected_dt = obstimes - obstimes[0]
    np.testing.assert_allclose(photometry[:, 0], expected_dt)

    expected_dt_prev = np.diff(np.r_[obstimes[0], obstimes])
    np.testing.assert_allclose(photometry[:, 1], expected_dt_prev)

    # psfFlux can be negative (e.g. the first sample); it is clipped to 1e-6 before log10.
    clipped_flux = np.clip(fluxes, 1e-6, None)
    np.testing.assert_allclose(photometry[:, 2], np.log10(clipped_flux))
    np.testing.assert_allclose(photometry[:, 3], flux_errors * LOG_CONST / clipped_flux)

    expected_one_hot = np.eye(NUM_BANDS, dtype=np.float32)[
        [BAND_TO_IDX[obs["band"]] for obs in SAMPLE_FP_HISTS]
    ]
    np.testing.assert_allclose(photometry[:, 4:], expected_one_hot)


def test_babamul_errors_without_stats_path():
    """Instantiation fails without a features stats file provided."""
    with pytest.raises(ValueError):
        _build_photometry_consumer(stats_path=None)


def test_get_photometry_first_observation_has_zero_dt_and_dt_prev(stats_path):
    """The first observation is the reference point, so its dt and dt_prev are both 0."""
    consumer = _build_photometry_consumer(stats_path)
    msg = {"fp_hists": SAMPLE_FP_HISTS}

    photometry = consumer.get_photometry(msg)

    assert photometry[0, 0] == 0.0
    assert photometry[0, 1] == 0.0


def test_get_photometry_raises_on_unknown_band(stats_path):
    """A band outside of BAND_TO_IDX (g, r, i) should surface as a KeyError rather than
    silently mis-encoding the observation."""
    consumer = _build_photometry_consumer(stats_path)
    msg = {"fp_hists": [{"jd": 1.0, "psfFlux": 10.0, "psfFluxErr": 1.0, "band": "z"}]}

    with pytest.raises(KeyError):
        consumer.get_photometry(msg)


def test_get_mean_and_get_std_read_from_stats_file(stats_path):
    """get_mean/get_std should load the configured stats_path .npz and reshape its
    mean/std arrays to (1, 4) for broadcasting against the photometry feature columns."""
    consumer = _build_photometry_consumer(stats_path)

    mean = consumer.get_mean(None)
    std = consumer.get_std(None)

    assert mean.shape == (1, 4)
    assert std.shape == (1, 4)
    np.testing.assert_allclose(mean, [[1.0, 2.0, 3.0, 4.0]])
    np.testing.assert_allclose(std, [[5.0, 6.0, 7.0, 8.0]])


def test_collate_photometry_pads_and_masks_variable_length_sequences():
    """collate_photometry should zero-pad each sequence up to the batch's max length
    (at least COLLATION_LENGTH) and mark padding positions True in pad_mask."""
    short_seq = np.ones((2, 7), dtype=np.float32)
    long_seq = np.full((5, 7), 2.0, dtype=np.float32)
    batch = [{"photometry": short_seq}, {"photometry": long_seq}]

    collated = BabamulPhotometryConsumer.collate_photometry(batch)

    assert collated["photometry"].shape == (2, COLLATION_LENGTH, 7)
    assert collated["pad_mask"].shape == (2, COLLATION_LENGTH)

    np.testing.assert_allclose(collated["photometry"][0, :2], short_seq)
    np.testing.assert_allclose(collated["photometry"][1, :5], long_seq)

    assert not collated["pad_mask"][0, :2].any()
    assert not collated["pad_mask"][1, :5].any()
    assert collated["pad_mask"][0, 2:].all()
    assert collated["pad_mask"][1, 5:].all()
    assert np.all(collated["photometry"][0, 2:] == 0.0)


def test_collate_photometry_truncates_sequences_longer_than_collation_length():
    """A sequence longer than COLLATION_LENGTH should be truncated rather than blow up
    the batch's padded width."""
    n = COLLATION_LENGTH + 10
    long_seq = np.arange(n * 7, dtype=np.float32).reshape(n, 7)
    batch = [{"photometry": long_seq}]

    collated = BabamulPhotometryConsumer.collate_photometry(batch)

    assert collated["photometry"].shape == (1, COLLATION_LENGTH, 7)
    assert collated["pad_mask"].shape == (1, COLLATION_LENGTH)
    assert not collated["pad_mask"].any()
    np.testing.assert_allclose(collated["photometry"][0], long_seq[:COLLATION_LENGTH])


def test_ingests_avro_encoded_photometry_alerts_end_to_end(monkeypatch, stats_path):
    """Decode an Avro-encoded Kafka message carrying `fp_hists` (mocked out with the
    FakeConsumer/FakeMessage pair, the same way test_ingests_avro_encoded_babamul_alerts
    mocks plain Babamul alerts) and confirm get_candid/get_photometry extract the
    expected values from the resulting sample."""
    consumer = _build_photometry_consumer(stats_path, batch_size=1)
    message = FakeMessage(_encode_photometry_alert(candid=12345, fp_hists=SAMPLE_FP_HISTS))
    _patch_consumer(monkeypatch, consumer, [message], stop_when_exhausted=True)

    batches = list(consumer)
    assert len(batches) == 1

    sample = batches[0][0]
    assert consumer.get_candid(sample) == 12345

    photometry = consumer.get_photometry(sample)
    assert photometry.shape == (len(SAMPLE_FP_HISTS), 7)
