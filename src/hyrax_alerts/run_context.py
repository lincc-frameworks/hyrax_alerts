"""Run-scoped output directories for hyrax-alerts.

Everything a single ``process_alerts`` run produces lives under one directory::

    <general.results_dir>/<timestamp>-hyrax_alerts-<random_str>/
        rejected/
            consumer/     batch_00000000.npy   <- pre_filter drops
            to_disk_0/    batch_00000000.npy   <- post_filter drops
        to_disk_0/        batch_00000000.npy   <- accepted output

The consumer and the writers are constructed at different points in the run --
writers in :func:`~hyrax_alerts.writers.writer_utils.get_writers`, the consumer
later, inside ``h.infer_stream()`` -- so the run directory is cached here at
module scope and every caller resolves the same path.

Holding this as module state is safe because both are built in the same process:
Hyrax forces ``num_workers = 0`` for streaming datasets, and writers are created
on the main thread before the writer thread pool starts. The lock guards the
lazy initialization regardless.

Directory names are *reserved* here but not created; a
:class:`~hyrax_alerts.writers.disk_writer.HyraxAlertsDiskWriter` creates its own
directory when it first writes. A run that rejects nothing therefore leaves no
empty directories behind.
"""

import base64
import datetime
import random
import threading
from pathlib import Path

DEFAULT_RESULTS_DIR = "./results"
RUN_DIR_POSTFIX = "hyrax_alerts"
REJECTED_DIR_NAME = "rejected"

# Number of times to re-roll the random suffix when a reserved name is already taken.
_RESERVE_ATTEMPTS = 10

_lock = threading.Lock()
_run_dir: Path | None = None
_rejected_dir: Path | None = None
_rejected_dir_resolved = False


def reserve_results_dir(results_root: str | Path, postfix: str) -> Path:
    """Reserve a uniquely named results directory under ``results_root``.

    The name follows the ``<timestamp>-<postfix>-<random_str>`` pattern used by
    Hyrax itself, so alerts output sorts alongside Hyrax's own run directories.

    The directory is **not** created -- the caller creates it when it has
    something to write. The random suffix is re-rolled if the name is already
    taken, which is what keeps concurrent runs from colliding.

    Parameters
    ----------
    results_root : str | Path
        The root, or parent, directory where result directories will be stored.
    postfix : str
        The run name embedded in the directory name.

    Returns
    -------
    Path
        The reserved path. It does not exist yet.
    """
    results_root = Path(results_root).expanduser().resolve()
    # This date format is chosen specifically to create a lexical search order
    # which matches the date order.
    timestamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    for _ in range(_RESERVE_ATTEMPTS):
        # Generate 4 random ascii characters to avoid collisions from multiple processes
        # started in a shared filesystem environment.
        random_str = base64.urlsafe_b64encode(random.randbytes(3)).decode("ascii")
        directory = results_root / f"{timestamp}-{postfix}-{random_str}"
        if not directory.exists():
            return directory
    raise RuntimeError(f"Could not reserve a unique results directory under {results_root}")


def get_run_dir(config: dict) -> Path:
    """Return this run's output directory, reserving it on first use.

    Parameters
    ----------
    config : dict
        The runtime configuration. ``[general] results_dir`` is the root the run
        directory is reserved inside of.

    Returns
    -------
    Path
        ``<general.results_dir>/<timestamp>-hyrax_alerts-<random_str>``, the same
        path for every caller until :func:`reset_run` is called.
    """
    global _run_dir
    with _lock:
        if _run_dir is None:
            results_root = config.get("general", {}).get("results_dir") or DEFAULT_RESULTS_DIR
            _run_dir = reserve_results_dir(results_root, RUN_DIR_POSTFIX)
        return _run_dir


def get_rejected_dir(config: dict) -> Path | None:
    """Return the directory holding this run's per-member rejected records.

    Driven by ``[hyrax_alerts] reject_output_root``:

    * ``true`` (the default) -- ``<run_dir>/rejected``, so rejected records sit
      beside the alerts that were kept.
    * ``false`` -- ``None``. Removed records are counted in the logs but not
      persisted.
    * a path -- ``<path>/<timestamp>-hyrax_alerts-<random_str>``, keeping runs
      separated in the user's chosen location.

    Callers append the member name (``consumer``, or a writer's config name) to
    get that member's own directory.

    Parameters
    ----------
    config : dict
        The runtime configuration.

    Returns
    -------
    Path | None
        The shared parent directory for rejected records, or ``None`` when
        persisting them is turned off.
    """
    global _rejected_dir, _rejected_dir_resolved
    # Resolve outside the lock: get_run_dir() takes the same non-reentrant lock.
    reject_output_root = config.get("hyrax_alerts", {}).get("reject_output_root", True)
    run_dir = get_run_dir(config) if reject_output_root is True else None
    with _lock:
        if not _rejected_dir_resolved:
            if not reject_output_root:
                _rejected_dir = None
            elif reject_output_root is True:
                _rejected_dir = run_dir / REJECTED_DIR_NAME  # type: ignore[union-attr]
            else:
                _rejected_dir = reserve_results_dir(reject_output_root, RUN_DIR_POSTFIX)
            _rejected_dir_resolved = True
        return _rejected_dir


def reset_run() -> None:
    """Forget the cached directories so the next run reserves fresh ones.

    Called at the start of :func:`~hyrax_alerts.process_alerts.process_alerts`,
    so repeated runs in one process do not share an output directory.
    """
    global _run_dir, _rejected_dir, _rejected_dir_resolved
    with _lock:
        _run_dir = None
        _rejected_dir = None
        _rejected_dir_resolved = False
