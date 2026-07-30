import logging
from copy import deepcopy
from threading import Barrier, Event

import pytest
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

    def write(self, data_batch: dict, result_batch: list | dict[str, list]):
        self.calls += 1
        self.barrier.wait(timeout=1)


class _MutatingWriter(HyraxAlertsBaseWriter):
    def __init__(self, ready):
        super().__init__({})
        self.ready = ready

    def post_process(self, result_batch: list | dict[str, list]) -> list | dict[str, list]:
        result_batch["score"][0] = MUTATED_SCORE
        self.ready.set()
        return result_batch

    def write(self, data_batch: dict, result_batch: list | dict[str, list]):
        pass


class _ObservingWriter(HyraxAlertsBaseWriter):
    def __init__(self, ready):
        super().__init__({})
        self.ready = ready
        self.seen_score = None

    def post_process(self, result_batch: list | dict[str, list]) -> list | dict[str, list]:
        assert self.ready.wait(timeout=1)
        self.seen_score = result_batch["score"][0]
        return result_batch

    def write(self, data_batch: dict, result_batch: list | dict[str, list]):
        pass


class _FailingWriter(HyraxAlertsBaseWriter):
    def __init__(self):
        super().__init__({})

    def write(self, data_batch: dict, result_batch: list | dict[str, list]):
        raise ValueError("boom")


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


def test_process_alerts_reports_which_writer_failed(monkeypatch):
    """Writer failures should include the writer class name for debugging."""
    _patch_process_alerts(monkeypatch, [_FailingWriter()])

    with pytest.raises(
        RuntimeError, match="_FailingWriter failed while processing a batch: ValueError"
    ) as error:
        process_alerts()

    assert isinstance(error.value.__cause__, ValueError)
    assert str(error.value.__cause__) == "boom"
