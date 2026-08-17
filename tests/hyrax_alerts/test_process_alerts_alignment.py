"""End-to-end proof that model results stay paired with the alerts that produced them.

``_merge_batch`` is the only place where the input batch and the model output are
zipped back together, and a silent off-by-one there would publish a real alert with
another object's score. These tests pin that down two ways:

1. ``test_alerts_keep_their_own_results_end_to_end`` runs the real ``process_alerts``
   entry point over a mocked Kafka stream -- real config file, real consumer, real
   Hyrax ``infer_stream`` session, real writer dispatch -- and checks every record.
2. ``test_alignment_check_fails_on_rotated_results`` feeds deliberately misaligned
   results through the same assertion helper, so a vacuous check cannot pass.

Each alert carries a payload that uniquely encodes its own object id, and the
``HyraxLoopback`` model returns its input unchanged. So the merged record for
``alert-3`` must carry the payload for ``alert-3`` in ``__hyrax_result``; any shift,
rotation, or cross-batch mixup shows up as a mismatch rather than as a passing test.
"""

import numpy as np
import pytest
from hyrax_alerts.consumers.kafka_consumer import HyraxKafkaConsumer
from hyrax_alerts.decollation_utils import merge_batch
from hyrax_alerts.process_alerts import process_alerts
from hyrax_alerts.writers.base_writer import HyraxAlertsBaseWriter
from test_kafka_consumer import FakeConsumer, _make_message  # noqa: I001

# Seven alerts over a batch size of three deliberately does not divide evenly, so
# batch boundaries fall in different places for different alerts.
ALERT_COUNT = 7
BATCH_SIZE = 3


class TestAlertsConsumer(HyraxKafkaConsumer):
    """A consumer that feeds a fixed number of alerts and then stops.

    This is a subclass of the real consumer so that ``process_alerts`` can be run
    end-to-end without a real Kafka broker.
    """

    def __init__(self, config, data_location=None):
        super().__init__(config=config, data_location=data_location)
        self._alerts_sent = 0

    def _make_consumer(self):
        """Return a fake consumer that feeds a fixed number of alerts."""
        messages = [_make_message(_object_id(i), _payload(i)) for i in range(ALERT_COUNT)]
        return FakeConsumer(messages, on_exhausted=self.stop)

    def get_object_id(self, msg):
        """Return the unique object id for the alert, which is unique to that alert."""
        return msg["object_id"]

    def get_image(self, msg):
        """Return the payload for the alert, which is unique to that alert."""
        return msg["image"]

    def get_label(self, msg):
        """Return the label for the alert, which is unique to that alert."""
        return msg["label"]


def _payload(index):
    """Return the model input for alert ``index``, unique to that alert."""
    # NOTE: both entries vary with the index so that neither a shifted result nor a
    # transposed one can accidentally match another alert's payload.
    return [[float(index), 100.0 * index + 1.0]]


def _object_id(index):
    return f"alert-{index}"


def _index_of(object_id):
    """Recover the alert index that an object id was generated from."""
    return int(object_id.split("-")[1])


class AlignmentRecordingWriter(HyraxAlertsBaseWriter):
    """Writer that accumulates every record it is handed, across all batches.

    ``process_alerts`` instantiates writers itself from the config, so the records
    are collected on the class rather than on an instance the test holds.
    """

    records: list[dict] = []

    def write(self, result_batch: list[dict]):
        """Record the batch for later alignment assertions."""
        AlignmentRecordingWriter.records.extend(result_batch)


@pytest.fixture
def recorded_records():
    """Provide an empty recording buffer and clean it up after the test."""
    AlignmentRecordingWriter.records.clear()
    yield AlignmentRecordingWriter.records
    AlignmentRecordingWriter.records.clear()


def _write_config(tmp_path):
    """Write a runtime config that streams mocked alerts into the recording writer."""
    weights_file = tmp_path / "weights.pth"
    weights_file.write_text("")  # HyraxLoopback.load is a no-op; contents are irrelevant

    config_file = tmp_path / "alignment_config.toml"
    config_file.write_text(
        f"""
[general]
results_dir = "{tmp_path / "results"}"

[data_loader]
batch_size = {BATCH_SIZE}

[data_set.KafkaStreamDataset]
topics = "alignment-test-topic"
batch_flush_timeout = 0.0

[data_request.infer_stream.data]
dataset_class = "test_process_alerts_alignment.TestAlertsConsumer"
data_location = "kafka://localhost:9092/alignment-test-topic"
primary_id_field = "object_id"
fields = ["image"]

[model]
name = "HyraxLoopback"

[infer_stream]
model_weights_file = "{weights_file}"
save_model_output = false

[hyrax_alerts.writers.alignment_recorder]
writer_class = "AlignmentRecordingWriter"

[hyrax_alerts.consumer]
alert_limit = false
"""
    )
    return config_file


def _assert_records_aligned(records):
    """Assert every record pairs an alert with the model output for that same alert.

    Each record is checked against the payload derived from its own ``object_id``,
    so alignment is verified per record rather than by comparing two parallel lists
    that could both be shifted the same way.
    """
    assert [record["object_id"] for record in records] == [_object_id(i) for i in range(ALERT_COUNT)]

    for record in records:
        expected = np.asarray(_payload(_index_of(record["object_id"])), dtype=np.float32)
        model_input = np.asarray(record["data"]["image"], dtype=np.float32)
        model_output = np.asarray(record["__hyrax_result"]["data"], dtype=np.float32)

        assert np.array_equal(
            model_input, expected
        ), f"{record['object_id']} carries the input for alert {model_input[0][0]:.0f}"
        assert np.array_equal(
            model_output, expected
        ), f"{record['object_id']} carries the result for alert {model_output[0][0]:.0f}"


def test_alerts_keep_their_own_results_end_to_end(tmp_path, monkeypatch, recorded_records):
    """Every alert reaches the writer paired with the model output it produced."""
    process_alerts(config_filepath=str(_write_config(tmp_path)))

    assert len(recorded_records) == ALERT_COUNT
    _assert_records_aligned(recorded_records)


def test_alignment_check_fails_on_rotated_results():
    """The alignment assertions reject results that are shifted by a single alert.

    Without this, a bug that made every record look self-consistent -- or an
    assertion that silently compared nothing -- would leave the end-to-end test
    passing.
    """
    data_batch = {
        "object_id": [_object_id(i) for i in range(ALERT_COUNT)],
        "data": {"image": [_payload(i) for i in range(ALERT_COUNT)]},
    }
    rotated = [_payload((i + 1) % ALERT_COUNT) for i in range(ALERT_COUNT)]

    records = merge_batch(data_batch, rotated)

    with pytest.raises(AssertionError, match="carries the result for alert"):
        _assert_records_aligned(records)


@pytest.mark.parametrize("batch_length", [1, 2, 5, 16])
def test_merge_batch_pairs_each_input_with_its_own_result(batch_length):
    """Across batch sizes, record ``i`` holds input ``i`` and result ``i``.

    This is the unit-level counterpart to the end-to-end test: it covers list- and
    dict-shaped result batches, both of which the merge normalizes differently.
    """
    data_batch = {
        "object_id": [_object_id(i) for i in range(batch_length)],
        "data": {"image": np.array([_payload(i) for i in range(batch_length)], dtype=np.float32)},
    }
    result_batch = {
        "score": np.array([100.0 * i + 1.0 for i in range(batch_length)], dtype=np.float32),
        "label": [f"label-{i}" for i in range(batch_length)],
    }

    records = merge_batch(data_batch, result_batch)
    list_records = merge_batch(data_batch, result_batch["score"])

    assert len(records) == batch_length
    for index, (record, list_record) in enumerate(zip(records, list_records, strict=True)):
        assert record["object_id"] == _object_id(index)
        assert np.array_equal(record["data"]["image"], np.asarray(_payload(index), dtype=np.float32))
        assert record["__hyrax_result"]["score"] == pytest.approx(100.0 * index + 1.0)
        assert record["__hyrax_result"]["label"] == f"label-{index}"
        # A list-shaped result batch lands under "data" but must pick the same entry.
        assert list_record["__hyrax_result"]["data"] == record["__hyrax_result"]["score"]
