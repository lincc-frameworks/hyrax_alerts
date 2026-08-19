"""Coercion helpers for turning writer records into JSON-serializable values.

Records handed to writers come straight out of
:func:`hyrax_alerts.decollation_utils.merge_batch`, so they are full of numpy
arrays, numpy scalars, and raw bytes - none of which the standard library's
JSON encoder can serialize. Writers that put records on a wire (Kafka, HTTP)
run them through :func:`to_jsonable` first.
"""

import base64
import json
import math
from datetime import date, datetime
from pathlib import Path

import numpy as np

# NOTE: This is a recursive coercion function rather than a json.JSONEncoder
# subclass for two reasons. First, `requests` calls json.dumps internally for its
# `json=` keyword and gives no way to pass `cls=`, so an encoder subclass would
# only help the Kafka writer. Second, JSONEncoder.default() is never consulted
# for dictionary *keys*, and merge_batch can produce numpy integer keys, which
# json.dumps rejects outright. Coercing up front handles both.

# NOTE: NaN and Infinity are not valid JSON. json.dumps emits them anyway by
# default, but Kafka JSON consumers and SkyPortal's Postgres JSONB columns both
# reject them, so non-finite floats are replaced rather than passed through.
# This is a deliberate loss of fidelity at the edge of the pipeline.
NON_FINITE_REPLACEMENT = None


def _jsonable_key(key):
    """Return a string usable as a JSON object key.

    Parameters
    ----------
    key : object
        A key taken from a dictionary inside a record. Commonly a string, but
        de-collated numpy data can also produce numpy integer keys.

    Returns
    -------
    str
        ``key`` unchanged when it is already a string, otherwise its coerced
        value rendered as a string.
    """
    return key if isinstance(key, str) else str(to_jsonable(key))


def to_jsonable(value):
    """Recursively coerce a value into JSON-serializable plain Python.

    numpy arrays become lists, numpy scalars become their Python equivalents,
    byte strings are base64 encoded, and non-finite floats become ``None``.
    Anything with no better representation falls back to ``str``. The input is
    never modified.

    Parameters
    ----------
    value : object
        Any value found inside a writer record.

    Returns
    -------
    object
        A value composed only of ``dict``, ``list``, ``str``, ``int``,
        ``float``, ``bool``, and ``None``.
    """
    # bool must be checked before int, since bool is a subclass of int.
    if value is None or isinstance(value, bool | str):
        return value

    if isinstance(value, int):
        return value

    if isinstance(value, float):
        return value if math.isfinite(value) else NON_FINITE_REPLACEMENT

    # Recurse on .item(): np.datetime64 yields a datetime, and a numpy NaN
    # yields a non-finite float that still needs replacing.
    if isinstance(value, np.generic):
        return to_jsonable(value.item())

    # 0-d arrays fall out of .tolist() as scalars, which is what we want.
    if isinstance(value, np.ndarray):
        return to_jsonable(value.tolist())

    if isinstance(value, bytes | bytearray | memoryview):
        return base64.b64encode(bytes(value)).decode("ascii")

    if isinstance(value, dict):
        return {_jsonable_key(key): to_jsonable(item) for key, item in value.items()}

    if isinstance(value, list | tuple | set | frozenset):
        return [to_jsonable(item) for item in value]

    if isinstance(value, datetime | date):
        return value.isoformat()

    if isinstance(value, Path):
        return str(value)

    return str(value)


def json_dumps(value) -> str:
    """Serialize a writer record to a JSON string, coercing numpy values first.

    Parameters
    ----------
    value : object
        The value to serialize, typically one record from a result batch.

    Returns
    -------
    str
        The JSON representation of ``value``.
    """
    return json.dumps(to_jsonable(value))
