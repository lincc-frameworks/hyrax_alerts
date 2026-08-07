import logging
from copy import deepcopy
from threading import Barrier, Event

import numpy as np
import pytest
from hyrax_alerts.decollation_utils import merge_batch
from hyrax_alerts.process_alerts import process_alerts
from hyrax_alerts.writers.base_writer import HyraxAlertsBaseWriter

MUTATED_SCORE = 99


class _FakeConsumer:
    def __init__(self):
        self.pre_filter_calls = 0
        self.pre_process_calls = 0

    def pre_filter(self, batch):
        self.pre_filter_calls += 1
        return batch

    def pre_process(self, batch):
        self.pre_process_calls += 1
        return batch


class _FakeDataLoader:
    def __init__(self, batches):
        self._batches = batches
        self.consumer = _FakeConsumer()
        self.dataset = type("Dataset", (), {"_stream": self.consumer})()

    def __iter__(self):
        for batch in self._batches:
            batch = self.consumer.pre_filter(batch)
            batch = self.consumer.pre_process(batch)
            yield deepcopy(batch)


class _FakeSession:
    def __init__(self, batches, results):
        self.data_loader = _FakeDataLoader(batches)
        self._results = results
        self.process_calls = 0

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def process(self, batch):
        self.process_calls += 1
        return deepcopy(self._results)


class _FakeHyrax:
    def __init__(self, config, batches, results):
        self.config = config
        self._batches = batches
        self._results = results
        self.last_session = None

    def infer_stream(self):
        self.last_session = _FakeSession(self._batches, self._results)
        return self.last_session


class _BarrierWriter(HyraxAlertsBaseWriter):
    def __init__(self, barrier):
        super().__init__({})
        self.barrier = barrier
        self.calls = 0

    def write(self, result_batch: list[dict]):
        self.calls += 1
        self.barrier.wait(timeout=1)


class _MutatingWriter(HyraxAlertsBaseWriter):
    def __init__(self, ready):
        super().__init__({})
        self.ready = ready

    def post_process(self, result_batch: list[dict]) -> list[dict]:
        result_batch[0]["__hyrax_result"]["score"] = MUTATED_SCORE
        self.ready.set()
        return result_batch

    def write(self, result_batch: list[dict]):
        pass


class _ObservingWriter(HyraxAlertsBaseWriter):
    def __init__(self, ready):
        super().__init__({})
        self.ready = ready
        self.seen_score = None

    def post_process(self, result_batch: list[dict]) -> list[dict]:
        assert self.ready.wait(timeout=1)
        self.seen_score = result_batch[0]["__hyrax_result"]["score"]
        return result_batch

    def write(self, result_batch: list[dict]):
        pass


class _RecordingWriter(HyraxAlertsBaseWriter):
    def __init__(self, post_filter=None):
        config = {"post_filter": post_filter} if post_filter else {}
        super().__init__(config)
        self.written = None

    def write(self, result_batch: list[dict]):
        self.written = result_batch


class _FailingWriter(HyraxAlertsBaseWriter):
    def __init__(self):
        super().__init__({})

    def write(self, result_batch: list[dict]):
        raise ValueError("boom")


class _RecordingRejectWriter(HyraxAlertsBaseWriter):
    def __init__(self):
        super().__init__({})
        self.written = None
        self.closed = False

    def write(self, result_batch: list[dict]):
        self.written = result_batch

    def close(self):
        self.closed = True


def _patch_process_alerts(monkeypatch, writers, results=None):
    batches = [{"object_id": ["alert-1"], "data": {"flux": [1.0]}}]
    config = {"hyrax_alerts": {"consumer": {"alert_limit": False}}}
    fake_hyrax = _FakeHyrax(config=config, batches=batches, results=results or [1])
    monkeypatch.setattr("hyrax_alerts.process_alerts.Hyrax", lambda config_file=None: fake_hyrax)
    monkeypatch.setattr("hyrax_alerts.process_alerts.get_writers", lambda config: writers)
    return fake_hyrax


def test_process_alerts_runs_writers_in_parallel(monkeypatch):
    """Writers for the same batch should be dispatched concurrently."""
    barrier = Barrier(2)
    writers = [_BarrierWriter(barrier), _BarrierWriter(barrier)]
    _patch_process_alerts(monkeypatch, writers)

    process_alerts()

    assert [writer.calls for writer in writers] == [1, 1]


def test_process_alerts_isolates_results_between_parallel_writers(monkeypatch):
    """Each writer should see its own results copy when run in parallel."""
    ready = Event()
    mutating_writer = _MutatingWriter(ready)
    observing_writer = _ObservingWriter(ready)
    _patch_process_alerts(monkeypatch, [mutating_writer, observing_writer], results={"score": [1]})

    process_alerts()

    assert observing_writer.seen_score == 1


def test_process_alerts_warns_without_configured_writers(monkeypatch, caplog):
    """Warn and continue through Hyrax processing when no writers are configured."""
    fake_hyrax = _patch_process_alerts(monkeypatch, [])
    caplog.set_level(logging.WARNING)

    process_alerts()

    assert "No writers configured; continuing without alert writers." in caplog.text
    assert fake_hyrax.last_session is not None
    assert fake_hyrax.last_session.process_calls == len(fake_hyrax._batches)
    assert fake_hyrax.last_session.data_loader.consumer.pre_filter_calls == len(fake_hyrax._batches)
    assert fake_hyrax.last_session.data_loader.consumer.pre_process_calls == len(fake_hyrax._batches)


def test_merge_batch_builds_one_record_per_alert():
    """Data batch fields and model results are zipped into per-alert records."""
    data_batch = {"object_id": ["a", "b"], "data": {"label": [4, 5]}}

    records = merge_batch(data_batch, [10, 20])

    assert records == [
        {"object_id": "a", "data": {"label": 4}, "__hyrax_result": {"data": 10}},
        {"object_id": "b", "data": {"label": 5}, "__hyrax_result": {"data": 20}},
    ]


def test_merge_batch_keeps_every_top_level_data_field():
    """Multiple nested top-level payloads are all carried into each record."""
    data_batch = {
        "object_id": ["a", "b"],
        "data": {"label": [4, 5]},
        "data_2": {"class": [1, 2]},
    }

    records = merge_batch(data_batch, [10, 20])

    assert records == [
        {"object_id": "a", "data": {"label": 4}, "data_2": {"class": 1}, "__hyrax_result": {"data": 10}},
        {"object_id": "b", "data": {"label": 5}, "data_2": {"class": 2}, "__hyrax_result": {"data": 20}},
    ]


def test_merge_batch_splits_dict_result_batches_per_record():
    """Dict-shaped result batches are split into one dict per record."""
    data_batch = {"object_id": ["a", "b"]}
    result_batch = {"result_1": [1, 2], "result_2": np.array([10.0, 20.0], dtype=np.float32)}

    records = merge_batch(data_batch, result_batch)

    assert [record["object_id"] for record in records] == ["a", "b"]
    assert [record["__hyrax_result"]["result_1"] for record in records] == [1, 2]
    assert [record["__hyrax_result"]["result_2"] for record in records] == [10.0, 20.0]


def test_merge_batch_normalizes_list_results_under_data():
    """List-shaped result batches are wrapped so __hyrax_result is always a dict."""
    data_batch = {"object_id": ["a", "b"]}

    records = merge_batch(data_batch, np.array([1.5, 2.5], dtype=np.float32))

    assert [record["__hyrax_result"] for record in records] == [{"data": 1.5}, {"data": 2.5}]


def test_merge_batch_rejects_length_mismatch():
    """A result batch that is not aligned to the data batch raises a ValueError."""
    data_batch = {"object_id": ["a", "b"], "data": {"label": [4, 5]}}

    with pytest.raises(ValueError, match="same length to preserve alignment"):
        merge_batch(data_batch, [1])


def test_merge_batch_rejects_misaligned_dict_result_batch_fields():
    """Dict-shaped result batches must have aligned field lengths."""
    data_batch = {"object_id": ["a", "b", "c"]}
    result_batch = {"result_1": [1, 2, 3], "result_2": [10, 20]}

    with pytest.raises(ValueError, match="share the same length"):
        merge_batch(data_batch, result_batch)


def test_process_alerts_writes_merged_records(monkeypatch):
    """Writers receive the merged records rather than parallel batches."""
    writer = _RecordingWriter()
    _patch_process_alerts(monkeypatch, [writer])

    process_alerts()

    assert writer.written == [{"object_id": "alert-1", "data": {"flux": 1.0}, "__hyrax_result": {"data": 1}}]


def test_process_alerts_writes_only_records_kept_by_post_filter(monkeypatch):
    """Records dropped by post_filter never reach the writer."""

    def drop_everything(self, result_batch):
        return []

    writer = _RecordingWriter(post_filter=drop_everything)
    _patch_process_alerts(monkeypatch, [writer])

    process_alerts()

    assert writer.written == []


def test_process_alerts_persists_records_removed_by_post_filter(monkeypatch):
    """Records dropped by post_filter are written to the writer's reject_writer,
    tagged with the stage that removed them."""

    def drop_everything(self, result_batch):
        return []

    writer = _RecordingWriter(post_filter=drop_everything)
    reject_writer = _RecordingRejectWriter()
    writer.reject_writer = reject_writer
    _patch_process_alerts(monkeypatch, [writer])

    process_alerts()

    assert writer.written == []
    assert reject_writer.written == [
        {
            "object_id": "alert-1",
            "data": {"flux": 1.0},
            "__hyrax_result": {"data": 1},
            "__hyrax_filter_stage": "post_filter",
        }
    ]


def test_process_alerts_closes_writer_reject_writer(monkeypatch):
    """A writer's reject_writer is entered/closed via the same ExitStack as the writer."""
    writer = _RecordingWriter()
    reject_writer = _RecordingRejectWriter()
    writer.reject_writer = reject_writer
    _patch_process_alerts(monkeypatch, [writer])

    process_alerts()

    assert reject_writer.closed is True


def test_process_alerts_reports_which_writer_failed(monkeypatch):
    """Writer failures should include the writer class name for debugging."""
    _patch_process_alerts(monkeypatch, [_FailingWriter()])

    with pytest.raises(
        RuntimeError, match="_FailingWriter failed while processing a batch: ValueError"
    ) as error:
        process_alerts()

    assert isinstance(error.value.__cause__, ValueError)
    assert str(error.value.__cause__) == "boom"
