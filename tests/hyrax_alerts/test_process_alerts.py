from copy import deepcopy
from threading import Barrier, Event

from hyrax_alerts.process_alerts import process_alerts
from hyrax_alerts.writers.base_writer import HyraxAlertsBaseWriter


class _FakeConsumer:
    def pre_filter(self, batch):
        return batch

    def pre_process(self, batch):
        return batch


class _FakeDataLoader:
    def __init__(self, batches):
        self._batches = batches
        self.dataset = type("Dataset", (), {"_stream": _FakeConsumer()})()

    def __iter__(self):
        return iter(self._batches)


class _FakeSession:
    def __init__(self, batches, results):
        self.data_loader = _FakeDataLoader(batches)
        self._results = results

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def process(self, batch):
        return deepcopy(self._results)


class _FakeHyrax:
    def __init__(self, config, batches, results):
        self.config = config
        self._batches = batches
        self._results = results

    def infer_stream(self):
        return _FakeSession(self._batches, self._results)


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
        result_batch["score"][0] = 99
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


def _patch_process_alerts(monkeypatch, writers, results=None):
    batches = [{"object_id": ["alert-1"], "data": {"flux": [1.0]}}]
    fake_hyrax = _FakeHyrax(config={}, batches=batches, results=results or [1])
    monkeypatch.setattr("hyrax_alerts.process_alerts.Hyrax", lambda config_file=None: fake_hyrax)
    monkeypatch.setattr("hyrax_alerts.process_alerts.get_writers", lambda config: writers)


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
