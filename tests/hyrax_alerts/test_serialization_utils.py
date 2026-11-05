import base64
import json
from datetime import datetime
from pathlib import Path

import numpy as np
from hyrax_alerts.writers.serialization_utils import json_dumps, to_jsonable


def test_numpy_array_becomes_a_list():
    """A 1-D numpy array is converted to a plain list."""
    assert to_jsonable(np.array([1, 2, 3])) == [1, 2, 3]


def test_nested_numpy_array_becomes_nested_lists():
    """A 2-D numpy array is converted to nested lists."""
    assert to_jsonable(np.array([[1, 2], [3, 4]])) == [[1, 2], [3, 4]]


def test_zero_dimensional_array_becomes_a_scalar():
    """A 0-d array collapses to a scalar rather than a list."""
    assert to_jsonable(np.array(7)) == 7


def test_numpy_scalars_become_python_scalars():
    """numpy scalars are converted to their Python equivalents, not just equal ones."""
    # Equality alone would not catch a numpy scalar leaking through, since
    # np.float32(1.5) == 1.5 is already True.
    coerced = [to_jsonable(np.float32(1.5)), to_jsonable(np.int64(3)), to_jsonable(np.bool_(True))]

    assert coerced == [1.5, 3, True]
    assert not any(isinstance(value, np.generic) for value in coerced)


def test_bytes_are_base64_encoded():
    """Byte fields survive as a base64 string that round-trips."""
    encoded = to_jsonable(b"cutout-bytes")

    assert base64.b64decode(encoded) == b"cutout-bytes"


def test_non_finite_floats_become_null():
    """NaN and infinity, which are not valid JSON, are replaced with None."""
    assert to_jsonable(float("nan")) is None
    assert to_jsonable(float("inf")) is None
    assert to_jsonable(np.float32("nan")) is None


def test_non_string_dict_keys_are_coerced():
    """Keys that are not strings become strings, since JSON has no others."""
    assert to_jsonable({np.int64(3): "value"}) == {"3": "value"}


def test_tuples_and_sets_become_lists():
    """Other sequence types are normalized to lists."""
    assert to_jsonable((1, 2)) == [1, 2]
    assert to_jsonable(frozenset({1})) == [1]


def test_datetimes_and_paths_become_strings():
    """Datetimes are ISO formatted and paths are stringified."""
    assert to_jsonable(datetime(2026, 8, 19, 12, 30)) == "2026-08-19T12:30:00"
    assert to_jsonable(Path("/tmp/alerts")) == "/tmp/alerts"


def test_unknown_objects_fall_back_to_str():
    """A value with no better representation is stringified rather than raising."""

    class Unserializable:
        def __str__(self):
            return "unserializable"

    assert to_jsonable(Unserializable()) == "unserializable"


def test_json_dumps_round_trips_a_full_record():
    """A record in merge_batch's shape survives a JSON round trip."""
    record = {
        "object_id": np.int64(101),
        "data": {"field_1": np.float32(1.5), "field_2": "a"},
        "__hyrax_result": {"data": np.array([0.25, 0.75])},
    }

    assert json.loads(json_dumps(record)) == {
        "object_id": 101,
        "data": {"field_1": 1.5, "field_2": "a"},
        "__hyrax_result": {"data": [0.25, 0.75]},
    }


def test_to_jsonable_does_not_mutate_the_input_record():
    """Coercion leaves the caller's record untouched."""
    record = {"__hyrax_result": {"data": np.array([1, 2])}}

    to_jsonable(record)

    assert isinstance(record["__hyrax_result"]["data"], np.ndarray)
