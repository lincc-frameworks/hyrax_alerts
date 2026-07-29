import numpy as np
import pytest
from hyrax_alerts.writers.base_writer import HyraxAlertsBaseWriter, get_writers


class DummyWriter(HyraxAlertsBaseWriter):
    """Minimal concrete writer for exercising shared writer behavior."""

    def write(self, data_batch: list, result_batch: list):
        """Dummy write method that just stores the last written batch."""
        self.last_written = (data_batch, result_batch)


def test_filter_batches_keeps_all_results_by_default():
    """Test that the default post_filter behavior keeps all results."""
    writer = DummyWriter(config={})
    data_batch = {"object_id": ["a", "b"], "data": {"label": [4, 5]}}

    filtered_data, filtered_results = writer._post_filter_batches(data_batch, [1, 2])

    assert filtered_data == data_batch
    assert filtered_results == [1, 2]


def test_filter_batches_uses_boolean_selector_to_keep_alignment():
    """Test that a custom post_filter function correctly keeps results based on
    a boolean selector."""

    def keep_edges(self, result_batch):
        return [True, False, True]

    writer = DummyWriter(config={"post_filter": keep_edges})
    data_batch = {"object_id": ["a", "b", "c"], "data": {"label": [4, 5, 6]}}

    filtered_data, filtered_results = writer._post_filter_batches(data_batch, [1, 2, 3])

    assert filtered_data == {"object_id": ["a", "c"], "data": {"label": [4, 6]}}
    assert filtered_results == [1, 3]


def test_filter_batches_keeps_alignment_across_multiple_nested_payloads():
    """Filtering should preserve alignment across multiple nested top-level payloads."""

    def keep_edges(self, result_batch):
        return [True, False, True]

    writer = DummyWriter(config={"post_filter": keep_edges})
    data_batch = {
        "object_id": ["a", "b", "c"],
        "data": {"label": [4, 5, 6]},
        "data_2": {"class": [1, 2, 2]},
    }

    filtered_data, filtered_results = writer._post_filter_batches(data_batch, [1, 2, 3])

    assert filtered_data == {
        "object_id": ["a", "c"],
        "data": {"label": [4, 6]},
        "data_2": {"class": [1, 2]},
    }
    assert filtered_results == [1, 3]


def test_filter_batches_preserves_dict_result_batch_types():
    """Dict-shaped result batches should be filtered with types preserved."""

    def keep_edges(self, result_batch):
        return [True, False, True]

    writer = DummyWriter(config={"post_filter": keep_edges})
    data_batch = {
        "object_id": ["a", "b", "c"],
        "data": {"label": [4, 5, 6]},
    }
    result_batch = {
        "result_1": [1, 2, 3],
        "result_2": np.array([10.0, 20.0, 30.0], dtype=np.float32),
    }

    filtered_data, filtered_results = writer._post_filter_batches(data_batch, result_batch)

    assert filtered_data == {"object_id": ["a", "c"], "data": {"label": [4, 6]}}
    assert isinstance(filtered_results, dict)
    assert filtered_results["result_1"] == [1, 3]
    assert isinstance(filtered_results["result_2"], np.ndarray)
    np.testing.assert_array_equal(filtered_results["result_2"], np.array([10.0, 30.0], dtype=np.float32))


def test_filter_batches_preserves_numpy_backed_data_types():
    """NumPy-backed top-level fields should stay NumPy-backed after filtering."""

    def keep_edges(self, result_batch):
        return [True, False, True]

    writer = DummyWriter(config={"post_filter": keep_edges})
    data_batch = {
        "object_id": np.array(["a", "b", "c"]),
        "data": {
            "image": np.arange(12, dtype=np.float32).reshape(3, 2, 2),
            "label": np.array([1.0, 2.0, 3.0], dtype=np.float32),
        },
    }

    filtered_data, filtered_results = writer._post_filter_batches(data_batch, [10, 20, 30])

    assert isinstance(filtered_data["object_id"], np.ndarray)
    assert isinstance(filtered_data["data"]["image"], np.ndarray)
    assert isinstance(filtered_data["data"]["label"], np.ndarray)
    np.testing.assert_array_equal(filtered_data["object_id"], np.array(["a", "c"]))
    np.testing.assert_array_equal(filtered_data["data"]["label"], np.array([1.0, 3.0], dtype=np.float32))
    np.testing.assert_array_equal(
        filtered_data["data"]["image"], np.arange(12, dtype=np.float32).reshape(3, 2, 2)[[0, 2]]
    )
    assert filtered_results == [10, 30]


def test_filter_batches_rejects_selector_length_mismatch():
    """Test that a post_filter function returning a selector of the wrong length
    raises a ValueError."""

    def invalid_selector(self, result_batch):
        return result_batch[:1]

    writer = DummyWriter(config={"post_filter": invalid_selector})
    data_batch = {"object_id": ["a", "b"], "data": {"label": [4, 5]}}

    with pytest.raises(ValueError, match="one entry per result"):
        writer._post_filter_batches(data_batch, [1, 2])


def test_filter_batches_rejects_misaligned_dict_result_batch_fields():
    """Dict-shaped result batches must have aligned field lengths."""
    writer = DummyWriter(config={})
    data_batch = {"object_id": ["a", "b", "c"], "data": {"label": [4, 5, 6]}}
    result_batch = {"result_1": [1, 2, 3], "result_2": [10, 20]}

    with pytest.raises(ValueError, match="share the same length"):
        writer._post_filter_batches(data_batch, result_batch)


def test_filter_batches_coerces_non_boolean_entries_to_mask():
    """Numeric selectors are coerced to a boolean mask after conversion."""

    def invalid_selector(self, result_batch):
        return [1, 0]

    writer = DummyWriter(config={"post_filter": invalid_selector})
    data_batch = {"object_id": ["a", "b"], "data": {"label": [4, 5]}}

    filtered_data, filtered_results = writer._post_filter_batches(data_batch, [1, 2])

    assert filtered_data == {"object_id": ["a"], "data": {"label": [4]}}
    assert filtered_results == [1]


def test_registering_callable_from_dotted_path_works_for_post_functions():
    """Test that registering callables from dotted paths works for post-processing
    and post-filtering functions."""
    writer = DummyWriter(
        config={
            "post_process": "hyrax_alerts.example_functions.example_post_process",
            "post_filter": "hyrax_alerts.example_functions.example_post_filter",
        }
    )

    processed = writer.post_process([1, 2, 3])
    selection = writer.post_filter([1, 2, 3])

    assert processed == [1, 2, 3]
    assert selection == [True, True, True]


def test_writer_context_manager_calls_close():
    """Test that the writer context manager calls the close method when exiting
    the context."""

    class ClosableWriter(HyraxAlertsBaseWriter):
        def __init__(self, config):
            super().__init__(config)
            self.closed = False

        def close(self):
            self.closed = True

        def write(self, data_batch: list, result_batch: list):
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
