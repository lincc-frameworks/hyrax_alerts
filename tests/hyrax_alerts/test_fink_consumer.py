"""Tests for FinkConsumer's ability to decode Fink's schema-in-the-key alerts.

Mirrors the approach in tests/hyrax_alerts/test_babamul_consumer.py: a FakeConsumer
stands in for confluent_kafka.Consumer so these tests never touch a real broker.

Fink frames alerts differently from every other broker supported here. Babamul sends an
Avro object container file, so the writer schema rides along inside the message value.
Fink instead puts the writer schema in the Kafka message *key* as JSON and leaves the
value as a bare schemaless Avro datum, so the fixtures below have to set both halves.
The FakeMessage here therefore grows a ``key()`` method that the one in
test_kafka_consumer.py does not have.
"""

import io
import json

import fastavro
import hyrax
import numpy as np
import pytest
from hyrax.datasets.data_provider import CollationMixin
from hyrax.models.hyrax_loopback import HyraxLoopback
from hyrax.models.model_registry import hyrax_model
from hyrax_alerts.consumers.fink_consumer import SCHEMA_CACHE_SIZE, FinkConsumer, FinkLsstConsumer
from hyrax_alerts.process_alerts import process_alerts
from hyrax_alerts.writers.base_writer import HyraxAlertsBaseWriter

# A trimmed-down stand-in for the Fink/LSST alert. The real schema carries ~100 nested
# fields; this keeps the records that FinkLsstConsumer's getters actually read.
FINK_LSST_SCHEMA = {
    "type": "record",
    "name": "FinkLsstAlert",
    "fields": [
        {"name": "diaSourceId", "type": "long"},
        {
            "name": "diaSource",
            "type": {
                "type": "record",
                "name": "DiaSource",
                "fields": [
                    {"name": "ra", "type": "double"},
                    {"name": "dec", "type": "double"},
                    {"name": "midpointMjdTai", "type": "double"},
                    {"name": "psfFlux", "type": "double"},
                    {"name": "psfFluxErr", "type": "double"},
                    {"name": "snr", "type": "float"},
                    {"name": "band", "type": "string"},
                    {"name": "reliability", "type": "float"},
                    {"name": "extendedness", "type": "float"},
                ],
            },
        },
        {
            "name": "diaObject",
            "type": {
                "type": "record",
                "name": "DiaObject",
                "fields": [{"name": "nDiaSources", "type": "int"}],
            },
        },
        {
            "name": "clf",
            "type": {
                "type": "record",
                "name": "Clf",
                "fields": [
                    {"name": "snnSnVsOthers_score", "type": "float"},
                    {"name": "cats_score", "type": "float"},
                    {"name": "earlySNIa_score", "type": "float"},
                ],
            },
        },
    ],
}

# The old-style Fink key: a bare version string rather than a JSON schema.
LEGACY_VERSION_KEY = "2.7_3.10.1"


class FakeMessage:
    """Stand-in for a confluent_kafka Message, including the key Fink decodes from."""

    def __init__(self, value, key=None, error=None):
        self._value = value
        self._key = key
        self._error = error

    def value(self):
        """Return the message payload."""
        return self._value

    def key(self):
        """Return the message key, which for Fink carries the Avro writer schema."""
        return self._key

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


def _alert_record(dia_source_id=1234, band="r", psf_flux=1500.0):
    """Build a single Fink/LSST alert record matching FINK_LSST_SCHEMA."""
    return {
        "diaSourceId": dia_source_id,
        "diaSource": {
            "ra": 34.5,
            "dec": -12.25,
            "midpointMjdTai": 60676.25,
            "psfFlux": psf_flux,
            "psfFluxErr": 42.0,
            "snr": 35.5,
            "band": band,
            "reliability": 0.875,
            "extendedness": 0.0,
        },
        "diaObject": {"nDiaSources": 7},
        "clf": {
            "snnSnVsOthers_score": 0.9,
            "cats_score": 0.75,
            "earlySNIa_score": 0.5,
        },
    }


def _encode_schemaless(record, schema=None):
    """Encode a record as a bare Avro datum, the way Fink writes message values."""
    buffer = io.BytesIO()
    fastavro.schemaless_writer(buffer, schema or FINK_LSST_SCHEMA, record)
    return buffer.getvalue()


def _make_message(record=None, key=None, schema=None):
    """Build a Fink-shaped message: schemaless value, JSON schema in the key."""
    record = _alert_record() if record is None else record
    schema = schema or FINK_LSST_SCHEMA
    if key is None:
        key = json.dumps(schema).encode("utf-8")
    return FakeMessage(_encode_schemaless(record, schema), key=key)


def _build_consumer(
    consumer_class=FinkConsumer,
    batch_size=5,
    batch_flush_timeout=100.0,
    consumer_config=None,
    schema_path=False,
):
    """Construct a Fink consumer with a configured topic and batch settings."""
    h = hyrax.Hyrax()
    h.config["data_loader"]["batch_size"] = batch_size
    ds_config = h.config["data_set"]["KafkaStreamDataset"]
    ds_config["topics"] = "test-topic"
    ds_config["batch_flush_timeout"] = batch_flush_timeout
    consumer_config = dict(consumer_config or {})
    consumer_config["FinkConsumer"] = {"schema_path": schema_path}
    h.config["hyrax_alerts"] = {"consumer": consumer_config}
    return consumer_class(h.config)


def _patch_consumer(monkeypatch, consumer, messages, stop_when_exhausted=False):
    """Make the consumer use a single FakeConsumer over ``messages``."""
    on_exhausted = consumer.stop if stop_when_exhausted else None
    fake_consumer = FakeConsumer(messages, on_exhausted=on_exhausted)
    monkeypatch.setattr(consumer, "_make_consumer", lambda: fake_consumer)
    return fake_consumer


@pytest.fixture
def schema_file(tmp_path):
    """Write FINK_LSST_SCHEMA to disk as an .avsc for the fallback path."""
    path = tmp_path / "fink_lsst.avsc"
    path.write_text(json.dumps(FINK_LSST_SCHEMA))
    return path


# --- decoding ------------------------------------------------------------------------


def test_decodes_alert_using_the_schema_from_the_message_key():
    """The writer schema arrives as JSON in the key; the value is a schemaless datum."""
    consumer = _build_consumer()
    record = _alert_record(dia_source_id=99)

    decoded = consumer._decode(_make_message(record))

    assert decoded["diaSourceId"] == 99
    assert decoded["diaSource"]["band"] == "r"
    assert decoded["diaObject"]["nDiaSources"] == 7


def test_decodes_alert_when_the_key_arrives_as_str_rather_than_bytes():
    """confluent_kafka hands back bytes, but a str key must decode identically."""
    consumer = _build_consumer()

    decoded = consumer._decode(_make_message(key=json.dumps(FINK_LSST_SCHEMA)))

    assert decoded["diaSourceId"] == 1234


def test_parsed_schemas_are_cached_across_messages(monkeypatch):
    """The Fink LSST schema has ~100 nested fields, so reparsing it for every alert
    would be a real per-message cost. Two alerts sharing a key must parse it once."""
    consumer = _build_consumer()
    calls = []
    real_parse_schema = fastavro.schema.parse_schema

    def counting_parse_schema(schema):
        calls.append(schema)
        return real_parse_schema(schema)

    monkeypatch.setattr(fastavro.schema, "parse_schema", counting_parse_schema)

    consumer._decode(_make_message(_alert_record(dia_source_id=1)))
    consumer._decode(_make_message(_alert_record(dia_source_id=2)))

    assert len(calls) == 1


def test_schema_cache_is_bounded(monkeypatch):
    """A broker cycling through schema versions must not grow the cache without limit."""
    consumer = _build_consumer()

    for index in range(SCHEMA_CACHE_SIZE + 2):
        # Vary the schema's name so each message presents a distinct key.
        schema = dict(FINK_LSST_SCHEMA, name=f"FinkLsstAlert{index}")
        consumer._decode(_make_message(schema=schema))

    assert len(consumer._schema_cache) <= SCHEMA_CACHE_SIZE


def test_falls_back_to_configured_schema_for_legacy_version_keys(schema_file):
    """Older Fink streams put a bare version string in the key instead of a schema.
    There is nothing to parse there, so the configured schema_path takes over."""
    consumer = _build_consumer(schema_path=str(schema_file))

    decoded = consumer._decode(_make_message(key=LEGACY_VERSION_KEY.encode("utf-8")))

    assert decoded["diaSourceId"] == 1234


def test_falls_back_to_configured_schema_when_the_key_is_missing(schema_file):
    """A message with no key at all must still decode via the configured schema."""
    consumer = _build_consumer(schema_path=str(schema_file))

    decoded = consumer._decode(_make_message(key=None))

    assert decoded["diaSourceId"] == 1234


def test_raises_a_pointed_error_when_no_schema_is_available():
    """With neither a schema in the key nor a configured schema_path there is no way to
    decode, and the error should name the setting that fixes it."""
    consumer = _build_consumer(schema_path=False)

    with pytest.raises(ValueError, match="schema_path"):
        consumer._decode(_make_message(key=LEGACY_VERSION_KEY.encode("utf-8")))


def test_errors_at_startup_when_the_configured_schema_file_is_missing(tmp_path):
    """A bad schema_path should fail when the consumer is built, not on the first alert."""
    with pytest.raises(ValueError, match="does not exist"):
        _build_consumer(schema_path=str(tmp_path / "nope.avsc"))


def test_ingests_fink_alerts_into_batches(monkeypatch):
    """End of the pipe: Fink-framed Kafka messages should batch like any other stream."""
    consumer = _build_consumer(batch_size=3)
    messages = [_make_message(_alert_record(dia_source_id=i)) for i in range(3)]
    fake_consumer = _patch_consumer(monkeypatch, consumer, messages, stop_when_exhausted=True)

    batches = list(consumer)

    assert len(batches) == 1
    assert [sample["diaSourceId"] for sample in batches[0]] == [0, 1, 2]
    assert fake_consumer.closed


# --- getters -------------------------------------------------------------------------


def test_get_dia_source_id_returns_the_primary_identifier():
    """get_dia_source_id backs primary_id_field for both Fink consumers."""
    consumer = _build_consumer()

    assert consumer.get_dia_source_id(_alert_record(dia_source_id=5150)) == 5150


def test_lsst_getters_read_the_expected_nested_fields():
    """Each FinkLsstConsumer getter reaches into the right nested record."""
    consumer = _build_consumer(consumer_class=FinkLsstConsumer)
    alert = _alert_record()

    assert consumer.get_ra(alert) == 34.5
    assert consumer.get_dec(alert) == -12.25
    assert consumer.get_mjd(alert) == 60676.25
    assert consumer.get_psf_flux(alert) == 1500.0
    assert consumer.get_psf_flux_err(alert) == 42.0
    assert consumer.get_snr(alert) == 35.5
    assert consumer.get_band(alert) == "r"
    assert consumer.get_reliability(alert) == 0.875
    assert consumer.get_extendedness(alert) == 0.0
    assert consumer.get_snn_sn_vs_others(alert) == 0.9
    assert consumer.get_cats_score(alert) == 0.75
    assert consumer.get_early_snia_score(alert) == 0.5
    assert consumer.get_n_dia_sources(alert) == 7


def test_lsst_getters_survive_an_avro_round_trip():
    """The getters must work on a decoded alert, not just a hand-built dict."""
    consumer = _build_consumer(consumer_class=FinkLsstConsumer)

    decoded = consumer._decode(_make_message(_alert_record(dia_source_id=7, band="z")))

    assert consumer.get_dia_source_id(decoded) == 7
    assert consumer.get_band(decoded) == "z"
    assert consumer.get_n_dia_sources(decoded) == 7


def test_no_collate_hooks_are_defined():
    """These consumers deliberately ship scalar getters only, so they define no
    collate_* hooks. If someone adds an array-valued getter later, this is the
    reminder that it needs a matching collation function."""
    hooks = [name for name in dir(FinkLsstConsumer) if name.startswith("collate_")]

    assert hooks == []


def test_scalar_getters_collate_with_hyrax_defaults():
    """Because every getter returns a scalar, all fields are fixed-shape and hyrax's
    default_field_collate stacks them without any custom hook."""
    consumer = _build_consumer(consumer_class=FinkLsstConsumer)
    alerts = [_alert_record(dia_source_id=i, psf_flux=100.0 * i) for i in range(4)]

    # Mirror StreamingDataProvider._structure, which wraps each getter's return value.
    samples = [
        {"psf_flux": np.asarray(consumer.get_psf_flux(a)), "band": np.asarray(consumer.get_band(a))}
        for a in alerts
    ]

    collated_flux = CollationMixin.default_field_collate(samples, "psf_flux", "fink")
    collated_band = CollationMixin.default_field_collate(samples, "band", "fink")

    assert collated_flux["psf_flux"].shape == (4,)
    np.testing.assert_allclose(collated_flux["psf_flux"], [0.0, 100.0, 200.0, 300.0])
    assert collated_band["band"].shape == (4,)


# --- end to end ----------------------------------------------------------------------
#
# The unit tests above check the getters in isolation. This runs the real
# ``process_alerts`` entry point -- real config file, real consumer, real Hyrax
# ``infer_stream`` session, real writer dispatch -- over a mocked Fink stream, which is
# the only way to prove the all-scalar getter contract satisfies StreamingDataProvider
# end to end and that no ``collate_*`` hook is needed to get there.

E2E_ALERT_COUNT = 5
E2E_BATCH_SIZE = 2


def _e2e_psf_flux(index):
    """Return the model input for alert ``index``, unique to that alert."""
    return 100.0 * index + 1.0


class StreamingFinkConsumer(FinkLsstConsumer):
    """A FinkLsstConsumer that feeds a fixed number of alerts and then stops.

    Subclasses the real consumer so that ``process_alerts`` can run end-to-end without a
    real Kafka broker.
    """

    def _make_consumer(self):
        """Return a fake consumer that feeds a fixed number of Fink-framed alerts."""
        messages = [
            _make_message(_alert_record(dia_source_id=i, psf_flux=_e2e_psf_flux(i)))
            for i in range(E2E_ALERT_COUNT)
        ]
        return FakeConsumer(messages, on_exhausted=self.stop)


@hyrax_model
class ScalarLoopbackModel(HyraxLoopback):
    """A loopback model that reads the scalar field this stream produces.

    ``HyraxLoopback.prepare_inputs`` hardcodes the field names "image" and "label",
    which a Fink stream does not produce, so it needs overriding here. That is a
    property of the stand-in model, not of the consumer under test: picking the model
    inputs out of the batch is always the model's job.
    """

    @staticmethod
    def prepare_inputs(data_dict):
        """Pull the psf_flux column out of the collated batch."""
        return (data_dict["data"]["psf_flux"], np.array([], dtype=np.float32))


class FinkRecordingWriter(HyraxAlertsBaseWriter):
    """Writer that accumulates every record it is handed, across all batches.

    ``process_alerts`` instantiates writers itself from the config, so the records are
    collected on the class rather than on an instance the test holds.
    """

    records: list[dict] = []

    def write(self, result_batch: list[dict]):
        """Record the batch for later assertions."""
        FinkRecordingWriter.records.extend(result_batch)


@pytest.fixture
def recorded_records():
    """Provide an empty recording buffer and clean it up after the test."""
    FinkRecordingWriter.records.clear()
    yield FinkRecordingWriter.records
    FinkRecordingWriter.records.clear()


def _write_e2e_config(tmp_path):
    """Write a runtime config that streams mocked Fink alerts into the recording writer."""
    weights_file = tmp_path / "weights.pth"
    weights_file.write_text("")  # HyraxLoopback.load is a no-op; contents are irrelevant

    config_file = tmp_path / "fink_config.toml"
    config_file.write_text(
        f"""
[general]
results_dir = "{tmp_path / "results"}"

[data_loader]
batch_size = {E2E_BATCH_SIZE}

[data_set.KafkaStreamDataset]
topics = "fink-test-topic"
batch_flush_timeout = 0.0

[data_request.infer_stream.data]
dataset_class = "test_fink_consumer.StreamingFinkConsumer"
data_location = "kafka://localhost:9092/fink-test-topic"
primary_id_field = "dia_source_id"
fields = ["psf_flux"]

[model]
name = "test_fink_consumer.ScalarLoopbackModel"

[infer_stream]
model_weights_file = "{weights_file}"
save_model_output = false

[hyrax_alerts.writers.fink_recorder]
writer_class = "FinkRecordingWriter"

[hyrax_alerts.consumer]
alert_limit = false

[hyrax_alerts.consumer.FinkConsumer]
schema_path = false
"""
    )
    return config_file


def test_fink_alerts_flow_end_to_end_without_a_collate_hook(tmp_path, recorded_records):
    """Every mocked Fink alert reaches the writer carrying its own psf_flux.

    Five alerts over a batch size of two deliberately does not divide evenly, so batch
    boundaries fall in different places for different alerts.
    """
    process_alerts(config_filepath=str(_write_e2e_config(tmp_path)))

    assert len(recorded_records) == E2E_ALERT_COUNT

    # object_id comes back stringified: hyrax carries it as text through the pipeline
    # even though get_dia_source_id returns the alert's integer diaSourceId.
    object_ids = [str(record["object_id"]) for record in recorded_records]
    assert object_ids == [str(i) for i in range(E2E_ALERT_COUNT)]

    for record in recorded_records:
        index = int(record["object_id"])
        expected = _e2e_psf_flux(index)
        assert record["data"]["psf_flux"] == pytest.approx(expected)
        assert record["__hyrax_result"]["data"] == pytest.approx(expected)
