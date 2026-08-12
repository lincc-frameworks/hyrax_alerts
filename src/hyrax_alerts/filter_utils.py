from collections.abc import Callable
from copy import deepcopy

from hyrax_alerts.logging_utils import get_logger

logger = get_logger(__name__)


def apply_filter(
    filter_fn: Callable[[list[dict]], list[dict]], batch: list[dict]
) -> tuple[list[dict], list[dict]]:
    """Run a user-supplied filter against a deep copy of ``batch`` and split the
    result into the records that were kept and the records that were removed.

    ``batch`` is deep copied before being handed to ``filter_fn`` so that callers
    retain an unmodified snapshot of the input to diff against the filtered output,
    even if ``filter_fn`` mutates the records it keeps.

    The kept/removed split is computed by object identity (``id()``), not equality.
    This is both an efficiency win -- building a set of ids and checking membership
    is O(n + m), versus the O(n * m) dict-equality scan of a naive
    ``[item for item in batch if item not in filtered]`` -- and a correctness
    requirement, since equality would misclassify a retained-but-mutated record as
    removed. It assumes ``filter_fn`` returns a subset of the same dict objects it
    was given (e.g. ``[r for r in batch if predicate(r)]``) rather than newly
    constructed copies, which is the natural way a filter is written. A filter that
    rebuilds dicts for kept records will cause every record to look removed.

    Parameters
    ----------
    filter_fn : Callable[[list[dict]], list[dict]]
        A user-supplied filter, e.g. a bound ``pre_filter``/``post_filter`` method.
    batch : list[dict]
        The records to filter.

    Returns
    -------
    tuple[list[dict], list[dict]]
        ``(kept, removed)``, where ``kept`` is exactly the list returned by
        ``filter_fn`` and ``removed`` holds the deep-copied records it dropped.
    """
    batch_copy = deepcopy(batch)
    kept = filter_fn(batch_copy)
    kept_ids = {id(record) for record in kept}
    removed = [record for record in batch_copy if id(record) not in kept_ids]
    return kept, removed
