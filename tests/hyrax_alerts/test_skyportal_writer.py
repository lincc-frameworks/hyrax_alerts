import json
import logging
from unittest.mock import MagicMock

import numpy as np
import pytest
from hyrax_alerts.writers import skyportal_writer
from hyrax_alerts.writers.base_writer import WRITER_REGISTRY
from hyrax_alerts.writers.skyportal_writer import (
    SKYPORTAL_IMPORT_ERROR,
    HyraxAlertsSkyPortalWriter,
    _is_duplicate_annotation,
    _response_status,
)

LOGGER_NAME = "hyrax.alerts.writers.skyportal_writer"


@pytest.fixture(autouse=True)
def fake_requests_module(monkeypatch):
    """Build writers against a mock requests module, so requests is never needed."""
    monkeypatch.setattr(skyportal_writer, "_load_requests_module", MagicMock)


def _valid_config(**overrides):
    """Return a minimal valid SkyPortal writer config, with optional overrides."""
    config = {
        "base_url": "https://skyportal.example.org",
        "token": "test-token",
        "origin": "hyrax-alerts",
    }
    config.update(overrides)
    return config


def _records(object_ids):
    """Return a batch of records in the shape writers receive them."""
    return [
        {"object_id": object_id, "__hyrax_result": {"score": index}}
        for index, object_id in enumerate(object_ids)
    ]


def _response(status="success", message="", data=None, status_code=200):
    """Return a fake requests response carrying a SkyPortal-shaped body."""
    response = MagicMock()
    response.status_code = status_code
    response.ok = status_code < 400
    response.json.return_value = {"status": status, "message": message, "data": data}
    return response


def _writer(**overrides):
    """Return a writer whose session is a mock returning successful responses."""
    writer = HyraxAlertsSkyPortalWriter(config=_valid_config(**overrides))
    writer.session = MagicMock()
    writer.session.post.return_value = _response()
    return writer


def test_init_requires_base_url():
    """The writer raises when no base_url is provided."""
    with pytest.raises(ValueError, match="base_url"):
        HyraxAlertsSkyPortalWriter(config={"token": "t", "origin": "o"})


def test_init_requires_token():
    """The writer raises when no token is provided."""
    with pytest.raises(ValueError, match="token"):
        HyraxAlertsSkyPortalWriter(config={"base_url": "https://s.example.org", "origin": "o"})


def test_init_requires_origin():
    """The writer raises when no origin is provided."""
    with pytest.raises(ValueError, match="origin"):
        HyraxAlertsSkyPortalWriter(config={"base_url": "https://s.example.org", "token": "t"})


def test_base_url_trailing_slash_is_stripped():
    """A trailing slash does not produce a doubled slash in request URLs."""
    writer = HyraxAlertsSkyPortalWriter(config=_valid_config(base_url="https://s.example.org/"))

    assert writer.base_url == "https://s.example.org"


def test_writer_registered_in_registry():
    """Defining the subclass registers it in the shared writer registry."""
    assert WRITER_REGISTRY.get("HyraxAlertsSkyPortalWriter") is HyraxAlertsSkyPortalWriter


def test_session_sends_the_token_header():
    """The API token is attached to the session as SkyPortal expects it."""
    writer = HyraxAlertsSkyPortalWriter(config=_valid_config())

    writer.session.headers.update.assert_called_once_with({"Authorization": "token test-token"})


def test_write_posts_once_per_record():
    """Each record in the batch becomes its own annotation request."""
    writer = _writer()

    writer.write(_records([101, 102, 103]))

    assert writer.session.post.call_count == 3


def test_post_url_targets_the_source_annotations_route():
    """Annotations are posted to the object's annotations endpoint."""
    writer = _writer()

    writer.write(_records([101]))

    assert writer.session.post.call_args[0][0] == (
        "https://skyportal.example.org/api/sources/101/annotations"
    )


def test_payload_contains_origin_and_data():
    """The annotation body carries the configured origin and the model output."""
    writer = _writer()

    writer.write(_records([101]))

    payload = writer.session.post.call_args.kwargs["json"]
    assert payload["origin"] == "hyrax-alerts"
    assert payload["data"] == {"score": 0}


def test_group_ids_are_included_when_configured():
    """Configured groups are passed through to SkyPortal."""
    writer = _writer(group_ids=[1, 2])

    writer.write(_records([101]))

    assert writer.session.post.call_args.kwargs["json"]["group_ids"] == [1, 2]


def test_group_ids_are_omitted_when_not_configured():
    """With no groups configured the key is absent, so SkyPortal applies its default."""
    writer = _writer()

    writer.write(_records([101]))

    assert "group_ids" not in writer.session.post.call_args.kwargs["json"]


def test_obj_id_field_is_configurable():
    """A different record field can supply the SkyPortal object id."""
    writer = _writer(obj_id_field="survey_id")

    writer.write([{"survey_id": "ZTF123", "__hyrax_result": {}}])

    assert writer.session.post.call_args[0][0].endswith("/api/sources/ZTF123/annotations")


def test_record_without_an_obj_id_is_skipped():
    """A record with no object id is skipped rather than posted or raised on."""
    writer = _writer()

    writer.write([{"__hyrax_result": {"score": 1}}])

    writer.session.post.assert_not_called()


def test_numpy_values_are_coerced_in_the_payload():
    """A numpy model output is coerced so the request body is JSON-serializable."""
    writer = _writer()

    writer.write([{"object_id": 101, "__hyrax_result": {"scores": np.array([0.25, 0.75])}}])

    payload = writer.session.post.call_args.kwargs["json"]
    assert json.loads(json.dumps(payload))["data"] == {"scores": [0.25, 0.75]}


def test_duplicate_annotation_is_updated_in_place():
    """An object that already carries this origin has its annotation replaced."""
    writer = _writer()
    writer.session.post.return_value = _response(
        status="error", message="Annotation already exists: ...", status_code=400
    )
    writer.session.get.return_value = _response(
        data=[{"id": 7, "origin": "other"}, {"id": 42, "origin": "hyrax-alerts"}]
    )
    writer.session.put.return_value = _response()

    writer.write(_records([101]))

    url = writer.session.put.call_args[0][0]
    assert url == "https://skyportal.example.org/api/sources/101/annotations/42"
    assert writer.session.put.call_args.kwargs["json"]["data"] == {"score": 0}


def test_duplicate_annotation_is_left_alone_when_updates_are_off(caplog):
    """With update_existing off, the first annotation wins and nothing is updated."""
    writer = _writer(update_existing=False)
    writer.session.post.return_value = _response(
        status="error", message="Annotation already exists: ...", status_code=400
    )

    with caplog.at_level(logging.INFO, logger=LOGGER_NAME):
        writer.write(_records([101]))

    writer.session.put.assert_not_called()
    assert "already has a 'hyrax-alerts' annotation" in caplog.text
    assert "ERROR" not in caplog.text


def test_duplicate_with_no_matching_annotation_is_logged(caplog):
    """A duplicate whose annotation cannot be found is reported, not raised."""
    writer = _writer()
    writer.session.post.return_value = _response(
        status="error", message="Annotation already exists: ...", status_code=400
    )
    writer.session.get.return_value = _response(data=[{"id": 7, "origin": "other"}])

    with caplog.at_level(logging.ERROR, logger=LOGGER_NAME):
        writer.write(_records([101]))

    writer.session.put.assert_not_called()
    assert "none was found to update" in caplog.text


def test_failed_update_is_logged(caplog):
    """A rejected update is reported without aborting the run."""
    writer = _writer()
    writer.session.post.return_value = _response(
        status="error", message="Annotation already exists: ...", status_code=400
    )
    writer.session.get.return_value = _response(data=[{"id": 42, "origin": "hyrax-alerts"}])
    writer.session.put.return_value = _response(status="error", message="nope", status_code=400)

    with caplog.at_level(logging.ERROR, logger=LOGGER_NAME):
        writer.write(_records([101]))

    assert "rejected the annotation update" in caplog.text


def test_error_response_is_logged_and_does_not_raise(caplog):
    """A rejected annotation is logged rather than aborting the run."""
    writer = _writer()
    writer.session.post.return_value = _response(status="error", message="Obj 101 not found", status_code=404)

    with caplog.at_level(logging.ERROR, logger=LOGGER_NAME):
        writer.write(_records([101]))

    assert "Obj 101 not found" in caplog.text


def test_connection_error_is_swallowed(caplog):
    """An unreachable SkyPortal instance is logged rather than aborting the run."""
    writer = _writer()
    writer.session.post.side_effect = RuntimeError("connection reset")

    with caplog.at_level(logging.ERROR, logger=LOGGER_NAME):
        writer.write(_records([101]))

    assert "Failed to reach SkyPortal" in caplog.text


def test_close_closes_the_session():
    """Closing the writer releases the session's pooled connections."""
    writer = _writer()

    writer.close()

    writer.session.close.assert_called_once()


def test_response_status_trusts_the_body_over_the_status_code():
    """SkyPortal's status field decides success, even on an HTTP 200."""
    succeeded, message = _response_status(_response(status="error", message="nope", status_code=200))

    assert succeeded is False
    assert message == "nope"


def test_response_status_falls_back_to_the_status_code():
    """A body that will not parse leaves only the status code to go on."""
    response = MagicMock()
    response.json.side_effect = ValueError("not json")
    response.ok = False
    response.status_code = 502

    succeeded, message = _response_status(response)

    assert succeeded is False
    assert "502" in message


def test_is_duplicate_annotation_matches_skyportals_message():
    """The duplicate-origin conflict is recognized from SkyPortal's error text."""
    assert _is_duplicate_annotation("Annotation already exists: duplicate key ...")
    assert not _is_duplicate_annotation("Obj 101 not found")


def test_import_error_names_the_extra():
    """The missing-dependency message tells the user which extra to install."""
    assert "hyrax_alerts[skyportal]" in SKYPORTAL_IMPORT_ERROR
