from hyrax_alerts.writers.base_writer import HyraxAlertsBaseWriter, get_reject_writer, instantiate_writer
from hyrax_alerts.writers.disk_writer import HyraxAlertsDiskWriter
from hyrax_alerts.writers.writer_utils import get_writers


class DummyWriter(HyraxAlertsBaseWriter):
    """Minimal concrete writer for exercising shared writer behavior."""

    def write(self, result_batch: list[dict]):
        """Dummy write method that just stores the last written batch."""
        self.last_written = result_batch


def _records():
    """Return a batch of records in the shape writers receive them."""
    return [
        {"object_id": "a", "data": {"label": 4}, "__hyrax_result": {"data": 1}},
        {"object_id": "b", "data": {"label": 5}, "__hyrax_result": {"data": 2}},
        {"object_id": "c", "data": {"label": 6}, "__hyrax_result": {"data": 3}},
    ]


def test_post_filter_keeps_all_results_by_default():
    """Test that the default post_filter behavior keeps all results."""
    writer = DummyWriter(config={})
    records = _records()

    assert writer.post_filter(records) == records


def test_post_filter_returns_the_filtered_records():
    """A custom post_filter returns only the records it keeps."""

    def keep_edges(self, result_batch):
        return [record for record in result_batch if record["__hyrax_result"]["data"] != 2]

    writer = DummyWriter(config={"post_filter": keep_edges})

    filtered = writer.post_filter(_records())

    assert filtered == [
        {"object_id": "a", "data": {"label": 4}, "__hyrax_result": {"data": 1}},
        {"object_id": "c", "data": {"label": 6}, "__hyrax_result": {"data": 3}},
    ]


def test_post_filter_can_drop_every_record():
    """A post_filter that keeps nothing returns an empty batch rather than failing."""

    def keep_nothing(self, result_batch):
        return []

    writer = DummyWriter(config={"post_filter": keep_nothing})

    assert writer.post_filter(_records()) == []


def test_post_process_can_reshape_records_before_filtering():
    """post_process output is what post_filter receives."""

    def add_score(self, result_batch):
        for record in result_batch:
            record["score"] = record["__hyrax_result"]["data"] * 10
        return result_batch

    def keep_high_scores(self, result_batch):
        return [record for record in result_batch if record["score"] > 10]

    writer = DummyWriter(config={"post_process": add_score, "post_filter": keep_high_scores})

    filtered = writer.post_filter(writer.post_process(_records()))

    assert [record["object_id"] for record in filtered] == ["b", "c"]
    assert [record["score"] for record in filtered] == [20, 30]


def test_registering_callable_from_dotted_path_works_for_post_functions():
    """Test that registering callables from dotted paths works for post-processing
    and post-filtering functions."""
    writer = DummyWriter(
        config={
            "post_process": "hyrax_alerts.example_functions.example_post_process",
            "post_filter": "hyrax_alerts.example_functions.example_post_filter",
        }
    )
    records = _records()

    processed = writer.post_process(records)
    filtered = writer.post_filter(processed)

    assert processed == records
    assert filtered == records


def test_writer_context_manager_calls_close():
    """Test that the writer context manager calls the close method when exiting
    the context."""

    class ClosableWriter(HyraxAlertsBaseWriter):
        def __init__(self, config):
            super().__init__(config)
            self.closed = False

        def close(self):
            self.closed = True

        def write(self, result_batch: list[dict]):
            pass

    writer = ClosableWriter(config={})

    with writer as managed_writer:
        assert managed_writer is writer
        assert writer.closed is False

    assert writer.closed is True


def test_get_writers_returns_empty_list_when_no_writers_configured():
    """Test that get_writers returns an empty list when no writers are configured."""
    writers = get_writers({"hyrax_alerts": {}})
    assert writers == []


def test_get_writers_returns_writer_instances():
    """Test that get_writers returns instances of the configured writers."""
    config = {
        "hyrax_alerts": {
            "writers": {
                "dummy_writer1": {
                    "writer_class": "DummyWriter",
                },
                "dummy_writer2": {
                    "writer_class": "DummyWriter",
                },
            }
        }
    }

    # Register DummyWriter in the WRITER_REGISTRY for this test
    from hyrax_alerts.writers.base_writer import WRITER_REGISTRY

    WRITER_REGISTRY["DummyWriter"] = DummyWriter

    writers = get_writers(config)
    assert len(writers) == 2
    assert isinstance(writers[0], DummyWriter)
    assert isinstance(writers[1], DummyWriter)


def test_instantiate_writer_returns_none_for_unregistered_class():
    """An unknown writer_class is skipped rather than raising."""
    assert instantiate_writer({"writer_class": "NotARegisteredWriter"}) is None


def test_instantiate_writer_builds_the_named_writer():
    """A registered writer_class is instantiated with the given config."""
    writer = instantiate_writer({"writer_class": "DummyWriter"})
    assert isinstance(writer, DummyWriter)


def test_get_reject_writer_returns_none_when_persistence_is_disabled():
    """A rejected_dir of None (reject_output_root = false) means no reject writer."""
    assert get_reject_writer({}, "consumer", None) is None


def test_get_reject_writer_explicit_config_wins_over_shared_dir(tmp_path):
    """A per-owner reject_writer block overrides the shared rejected dir entirely."""
    tmp_path = tmp_path.resolve()
    override_dir = tmp_path / "explicit_override"
    owner_config = {
        "reject_writer": {
            "writer_class": "HyraxAlertsDiskWriter",
            "output_location": str(override_dir),
        }
    }

    writer = get_reject_writer(owner_config, "consumer", tmp_path / "rejected")

    assert isinstance(writer, HyraxAlertsDiskWriter)
    # An explicitly configured location still gets its own timestamped subdirectory.
    assert writer.output_location.parent == override_dir


def test_get_reject_writer_nests_owner_directly_under_the_rejected_dir(tmp_path):
    """An owner's rejects land in <rejected_dir>/<owner_name>, with no extra timestamp
    level -- the rejected dir already sits inside a timestamped run directory."""
    tmp_path = tmp_path.resolve()
    rejected_dir = tmp_path / "rejected"

    writer = get_reject_writer({}, "to_disk_0", rejected_dir)

    assert isinstance(writer, HyraxAlertsDiskWriter)
    assert writer.output_location == rejected_dir / "to_disk_0"


def test_reject_writers_for_different_owners_share_one_parent(tmp_path):
    """Every owner's rejects land side by side under the same rejected dir, which is
    what lets one run's dropped records be read as a unit."""
    tmp_path = tmp_path.resolve()
    rejected_dir = tmp_path / "rejected"

    consumer_writer = get_reject_writer({}, "consumer", rejected_dir)
    disk_writer = get_reject_writer({}, "to_disk_0", rejected_dir)

    assert consumer_writer.output_location.parent == rejected_dir
    assert disk_writer.output_location.parent == rejected_dir
    assert consumer_writer.output_location != disk_writer.output_location
