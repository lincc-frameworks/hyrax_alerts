def _aligned_entry(values, index):
    """Return the entry at ``index`` for an aligned field or dict of aligned fields."""
    # NOTE: if `values` is a dictionary, we recursively pull the entry from each
    # field. An example is when `values` represents a nested data structure within
    # the batch, as in the "data" portion of {"data": {"label": [4, 5, 6]}}.
    if isinstance(values, dict):
        return {field: _aligned_entry(field_values, index) for field, field_values in values.items()}
    return values[index]


def _aligned_length(values):
    """Return aligned batch length for list/array or dict-of-aligned-fields. Initial
    return values from Hyrax models were expected to be a single numpy object. However
    in future releases we will support more complex batch structures that include
    returning a dictionary of numpy values from the model for each batch element."""
    if isinstance(values, dict):
        if not values:
            raise ValueError("result_batch dictionary must contain at least one aligned field")
        # set comprehension to collect the lengths of all fields in the dictionary
        lengths = {_aligned_length(field_values) for field_values in values.values()}
        if len(lengths) != 1:
            raise ValueError("result_batch dictionary fields must share the same length")
        return lengths.pop()
    return len(values)


def merge_batch(data_batch: dict, result_batch: list | dict[str, list]) -> list[dict]:
    """Zip a data batch and its model results into one dictionary per alert.

    Writers operate on a list of dictionaries rather than on parallel batches, so
    that post_process and post_filter implementations can treat each alert as a
    self-contained record without having to maintain alignment themselves.

    Parameters
    ----------
    data_batch : dict
        A batch of input data. Expected to contain an ``object_id`` entry, as
        produced by the Hyrax alerts consumers.
    result_batch : list | dict[str, list]
        The model results for this batch, aligned to ``data_batch``. A dict-shaped
        batch keeps its own field names; a list-shaped batch is normalized under a
        single ``data`` field so that writers always see the same structure.

    Example:
    # before merging:
    data_batch = {
        "object_id": ["id1", "id2"],
        "data": {
            "field_1": [10, 20],
            "field_2": ["a", "b"],
        },
    }

    # if the model returns a list of results:
    results = [<numpy array 1>, <numpy array 2>]

    # after merging:
    merged = [
        {
            "object_id": "id1",
            "data": {
                "field_1": 10,
                "field_2": "a",
            },
            "__hyrax_result": {
                "data": <numpy array 1>,
            },
        },
        {
            "object_id": "id2",
            "data": {
                "field_1": 20,
                "field_2": "b",
            },
            "__hyrax_result": {
                "data": <numpy array 2>,
            },
        },
    ]

    # if the model returns a dictionary of results:
    results = {
        "prediction": [<numpy array 1>, <numpy array 2>],
        "confidence": [<numpy_3>, <numpy_4>],
    }

    # after merging:
    merged = [
        {
            "object_id": "id1",
            "data": {
                "field_1": 10,
                "field_2": "a",
            },
            "__hyrax_result": {
                "prediction": <numpy array 1>,
                "confidence": <numpy_3>,
            },
        },
        {
            "object_id": "id2",
            "data": {
                "field_1": 20,
                "field_2": "b",
            },
            "__hyrax_result": {
                "prediction": <numpy array 2>,
                "confidence": <numpy_4>,
            },
        },
    ]

    Returns
    -------
    list[dict]
        One dictionary per alert, holding the data batch fields for that alert
        plus its model output under the ``__hyrax_result`` key. That value is
        always a dict, keyed by result field name.
    """
    batch_length = len(data_batch["object_id"])

    if _aligned_length(result_batch) != batch_length:
        raise ValueError("data_batch and result_batch must be the same length to preserve alignment")

    # NOTE: a list-shaped result_batch is normalized into a dict of aligned fields so
    # that every record exposes the same `__hyrax_result` structure no matter what
    # shape the model returned.
    if not isinstance(result_batch, dict):
        result_batch = {"data": result_batch}

    records = []
    for index in range(batch_length):
        record = {field: _aligned_entry(values, index) for field, values in data_batch.items()}
        # NOTE: results are nested under a single key rather than merged into the
        # record so that model output can never collide with a data batch field.
        record["__hyrax_result"] = _aligned_entry(result_batch, index)
        records.append(record)
    return records
