from unittest.mock import MagicMock

import pytest
from hyrax_alerts.writers import slack_writer
from hyrax_alerts.writers.slack_writer import HyraxAlertsSlackWriter, _format_batch_summary


@pytest.fixture(autouse=True)
def fake_slack_client_class(monkeypatch):
    """Build writers against a mock client, so slack_sdk is never needed."""
    monkeypatch.setattr(slack_writer, "_load_client_class", lambda: MagicMock)


def _valid_config(**overrides):
    """Return a minimal valid Slack writer config, with optional overrides."""
    config = {"slack_token": "xoxb-test-token", "channel": "#hyrax-alerts"}
    config.update(overrides)
    return config


def test_init_requires_slack_token():
    """The writer raises when no slack_token is provided."""
    with pytest.raises(ValueError, match="slack_token"):
        HyraxAlertsSlackWriter(config={"channel": "#hyrax-alerts"})


def test_init_requires_channel():
    """The writer raises when no channel is provided."""
    with pytest.raises(ValueError, match="channel"):
        HyraxAlertsSlackWriter(config={"slack_token": "xoxb-test-token"})


def _records(object_ids):
    """Return a batch of records in the shape writers receive them."""
    return [
        {"object_id": object_id, "__hyrax_result": {"data": index}}
        for index, object_id in enumerate(object_ids)
    ]


def test_format_batch_summary_lists_object_ids():
    """The summary reports the count and lists the object ids."""
    summary = _format_batch_summary(_records([101, 102, 103]))

    assert "3 alerts" in summary
    assert "101, 102, 103" in summary


def test_format_batch_summary_uses_singular_for_one_result():
    """A single-object batch uses the singular 'alert'."""
    summary = _format_batch_summary(_records([101]))

    assert "1 alert -" in summary
    assert "1 alerts" not in summary


def test_format_batch_summary_truncates_long_batches():
    """Object ids beyond max_object_ids are truncated with a '+N more' suffix."""
    summary = _format_batch_summary(_records(range(15)), max_object_ids=10)

    assert "15 alerts" in summary
    assert "(+5 more)" in summary


def test_format_batch_summary_handles_empty_batch():
    """An empty batch produces a sensible message rather than failing."""
    summary = _format_batch_summary([])

    assert "no objects" in summary


def test_write_posts_message_to_configured_channel():
    """write() posts the summary to the configured channel via the client."""
    writer = HyraxAlertsSlackWriter(config=_valid_config())
    writer.client = MagicMock()

    writer.write(_records([101, 102]))

    writer.client.chat_postMessage.assert_called_once()
    _, kwargs = writer.client.chat_postMessage.call_args
    assert kwargs["channel"] == "#hyrax-alerts"
    assert "2 alerts" in kwargs["text"]


def test_write_swallows_client_errors():
    """An error from the Slack client does not propagate out of write()."""
    writer = HyraxAlertsSlackWriter(config=_valid_config())
    writer.client = MagicMock()
    writer.client.chat_postMessage.side_effect = RuntimeError("boom")

    # Should not raise.
    writer.write(_records([101]))


def test_write_swallows_connection_errors():
    """A transport failure is swallowed too, not just Slack API errors."""
    writer = HyraxAlertsSlackWriter(config=_valid_config())
    writer.client = MagicMock()
    writer.client.chat_postMessage.side_effect = ConnectionError("dns failure")

    # Should not raise.
    writer.write(_records([101]))


def test_writer_registered_in_registry():
    """Defining the subclass registers it in the shared writer registry."""
    from hyrax_alerts.writers.base_writer import WRITER_REGISTRY

    assert WRITER_REGISTRY.get("HyraxAlertsSlackWriter") is HyraxAlertsSlackWriter
