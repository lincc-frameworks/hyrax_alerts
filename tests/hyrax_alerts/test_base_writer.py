import pytest
from hyrax_alerts.writers.base_writer import HyraxAlertsBaseWriter


class DummyWriter(HyraxAlertsBaseWriter):
    """Minimal concrete writer for exercising shared writer behavior."""

    def write(self, data_batch: list, result_batch: list):
        """Dummy write method that just stores the last written batch."""
        self.last_written = (data_batch, result_batch)


def test_filter_batches_keeps_all_results_by_default():
    """Test that the default post_filter behavior keeps all results."""
    writer = DummyWriter(config={})

    filtered_data, filtered_results = writer._post_filter_batches(["a", "b"], [1, 2])

    assert filtered_data == ["a", "b"]
    assert filtered_results == [1, 2]


def test_filter_batches_uses_boolean_selector_to_keep_alignment():
    """Test that a custom post_filter function correctly keeps results based on
    a boolean selector."""

    def keep_edges(self, result_batch):
        return [True, False, True]

    writer = DummyWriter(config={"post_filter": keep_edges})

    filtered_data, filtered_results = writer._post_filter_batches(["a", "b", "c"], [1, 2, 3])

    assert filtered_data == ["a", "c"]
    assert filtered_results == [1, 3]


def test_filter_batches_rejects_selector_length_mismatch():
    """Test that a post_filter function returning a selector of the wrong length
    raises a ValueError."""

    def invalid_selector(self, result_batch):
        return result_batch[:1]

    writer = DummyWriter(config={"post_filter": invalid_selector})

    with pytest.raises(ValueError, match="one entry per result"):
        writer._post_filter_batches(["a", "b"], [1, 2])


def test_filter_batches_rejects_non_boolean_entries():
    """Test that a post_filter function returning non-boolean entries raises a
    TypeError."""

    def invalid_selector(self, result_batch):
        return [1, 0]

    writer = DummyWriter(config={"post_filter": invalid_selector})

    with pytest.raises(TypeError, match="must return booleans"):
        writer._post_filter_batches(["a", "b"], [1, 2])


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
