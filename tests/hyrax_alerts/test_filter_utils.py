from hyrax_alerts.filter_utils import apply_filter


def test_apply_filter_splits_kept_and_removed():
    """Records excluded by the filter come back in `removed`, in original order."""
    batch = [{"id": 1}, {"id": 2}, {"id": 3}]

    def keep_odd(records):
        return [r for r in records if r["id"] % 2 == 1]

    kept, removed = apply_filter(keep_odd, batch)

    assert [r["id"] for r in kept] == [1, 3]
    assert [r["id"] for r in removed] == [2]


def test_apply_filter_does_not_mutate_caller_batch():
    """The filter must operate on a deep copy, not the caller's original list."""
    batch = [{"id": 1, "nested": {"count": 0}}]

    def mutate_and_drop(records):
        for record in records:
            record["nested"]["count"] += 1
        return []

    kept, removed = apply_filter(mutate_and_drop, batch)

    assert batch == [{"id": 1, "nested": {"count": 0}}]
    assert kept == []
    assert removed[0]["nested"]["count"] == 1


def test_apply_filter_tracks_mutated_but_kept_records_by_identity():
    """A record the filter mutates in place but still returns must count as kept,
    not removed -- equality-based diffing would misclassify it."""
    batch = [{"id": 1}, {"id": 2}]

    def mutate_in_place(records):
        records[0]["flagged"] = True
        return records

    kept, removed = apply_filter(mutate_in_place, batch)

    assert kept == [{"id": 1, "flagged": True}, {"id": 2}]
    assert removed == []


def test_apply_filter_no_op_filter_removes_nothing():
    """A filter that keeps everything produces an empty removed list."""
    batch = [{"id": 1}, {"id": 2}]

    def no_op(records):
        return records

    kept, removed = apply_filter(no_op, batch)

    assert kept == batch
    assert removed == []
