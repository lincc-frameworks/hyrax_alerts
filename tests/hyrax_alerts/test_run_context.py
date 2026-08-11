import re

import pytest
from hyrax_alerts.run_context import get_rejected_dir, get_run_dir, reserve_results_dir, reset_run
from hyrax_alerts.writers.disk_writer import HyraxAlertsDiskWriter
from hyrax_alerts.writers.writer_utils import get_writers


def _config(tmp_path, **hyrax_alerts_config):
    """Return a runtime config rooted at tmp_path."""
    return {
        "general": {"results_dir": str(tmp_path / "results")},
        "hyrax_alerts": hyrax_alerts_config,
    }


def test_reserve_results_dir_does_not_create_the_directory(tmp_path):
    """Reserving a name is not the same as creating it; writers create on first write."""
    directory = reserve_results_dir(tmp_path, "hyrax_alerts")

    assert directory.parent == tmp_path.resolve()
    assert not directory.exists()


def test_reserve_results_dir_skips_a_name_that_is_already_taken(tmp_path):
    """A reserved name that already exists on disk is re-rolled rather than reused."""
    taken = reserve_results_dir(tmp_path, "hyrax_alerts")
    taken.mkdir(parents=True)

    assert reserve_results_dir(tmp_path, "hyrax_alerts") != taken


def test_run_dir_is_shared_by_every_caller(tmp_path):
    """The consumer and the writers are built at different points in the run and must
    still resolve to one directory."""
    config = _config(tmp_path)

    run_dir = get_run_dir(config)

    assert get_run_dir(config) == run_dir
    assert run_dir.parent == (tmp_path / "results").resolve()
    # <timestamp>-hyrax_alerts-<4 random chars>, sorting alongside Hyrax's own run dirs.
    assert re.fullmatch(r"\d{8}-\d{6}-hyrax_alerts-.{4}", run_dir.name)


def test_reset_run_starts_a_fresh_run_dir(tmp_path):
    """Repeated runs in one process must not share an output directory."""
    config = _config(tmp_path)
    first = get_run_dir(config)

    reset_run()

    assert get_run_dir(config) != first


def test_rejected_dir_defaults_to_the_run_dir(tmp_path):
    """By default rejected records sit beside the alerts that were kept."""
    config = _config(tmp_path)

    assert get_rejected_dir(config) == get_run_dir(config) / "rejected"


def test_rejected_dir_defaults_to_the_run_dir_when_key_is_absent(tmp_path):
    """An unconfigured pipeline still persists rejects, in the default location."""
    config = {"general": {"results_dir": str(tmp_path / "results")}}

    assert get_rejected_dir(config) == get_run_dir(config) / "rejected"


def test_rejected_dir_is_none_when_persistence_is_disabled(tmp_path):
    """reject_output_root = false turns off persisting removed records."""
    assert get_rejected_dir(_config(tmp_path, reject_output_root=False)) is None


def test_rejected_dir_honors_a_configured_path(tmp_path):
    """A path redirects rejects, keeping runs separated by a timestamped directory."""
    config = _config(tmp_path, reject_output_root=str(tmp_path / "elsewhere"))

    rejected_dir = get_rejected_dir(config)

    assert rejected_dir.parent == (tmp_path / "elsewhere").resolve()
    assert "-hyrax_alerts-" in rejected_dir.name
    # Cached, so every owner nests under the same timestamped directory.
    assert get_rejected_dir(config) == rejected_dir
    # And it stays out of the run directory.
    assert get_run_dir(config) not in rejected_dir.parents


def test_get_writers_defaults_each_writer_to_its_own_run_subdir(tmp_path):
    """With no output_location a writer writes to <run_dir>/<writer_name>/."""
    config = _config(tmp_path)
    config["hyrax_alerts"]["writers"] = {
        "to_disk_0": {"writer_class": "HyraxAlertsDiskWriter"},
        "to_disk_1": {"writer_class": "HyraxAlertsDiskWriter"},
    }

    writers = get_writers(config)
    run_dir = get_run_dir(config)

    assert [writer.output_location for writer in writers] == [
        run_dir / "to_disk_0",
        run_dir / "to_disk_1",
    ]
    assert [writer.reject_writer.output_location for writer in writers] == [
        run_dir / "rejected" / "to_disk_0",
        run_dir / "rejected" / "to_disk_1",
    ]


def test_get_writers_honors_an_explicit_output_location(tmp_path):
    """An explicit output_location still gets its own timestamped subdirectory, so
    repeated runs writing to a user-chosen location don't overwrite each other."""
    config = _config(tmp_path)
    config["hyrax_alerts"]["writers"] = {
        "to_disk_0": {
            "writer_class": "HyraxAlertsDiskWriter",
            "output_location": str(tmp_path / "custom"),
        }
    }

    writer = get_writers(config)[0]

    assert writer.output_location.parent == (tmp_path / "custom").resolve()
    assert "-hyrax_alerts-" in writer.output_location.name


def test_disk_writer_creates_nothing_until_it_writes(tmp_path):
    """A run that rejects nothing leaves no empty directories behind."""
    writer = HyraxAlertsDiskWriter(
        {"output_location": str(tmp_path / "rejected" / "consumer"), "timestamped_subdirectory": False}
    )

    assert not writer.output_location.exists()

    writer.write([{"object_id": "a"}])

    assert (writer.output_location / "batch_00000000.npy").exists()


def test_disk_writer_does_not_overwrite_existing_batches(tmp_path):
    """Two writer instances pointed at one directory log an error and raise
    an exception rather than overwriting each other's output."""
    location = {"output_location": str(tmp_path / "shared"), "timestamped_subdirectory": False}
    HyraxAlertsDiskWriter(location).write([{"object_id": "a"}])
    with pytest.raises(ValueError) as excinfo:
        HyraxAlertsDiskWriter(location).write([{"object_id": "b"}])

    assert "already exists" in str(excinfo.value)
