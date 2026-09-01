from unittest.mock import MagicMock

import pytest
from hyrax_alerts.writers.slack_writer import HyraxAlertsSlackWriter, _format_batch_summary
from slack_sdk.errors import SlackApiError


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


def test_write_swallows_slack_api_error():
    """A SlackApiError from the client does not propagate out of write()."""
    writer = HyraxAlertsSlackWriter(config=_valid_config())
    writer.client = MagicMock()
    writer.client.chat_postMessage.side_effect = SlackApiError("boom", response={})

    # Should not raise.
    writer.write(_records([101]))


def test_writer_registered_in_registry():
    """Defining the subclass registers it in the shared writer registry."""
    from hyrax_alerts.writers.base_writer import WRITER_REGISTRY

    assert WRITER_REGISTRY.get("HyraxAlertsSlackWriter") is HyraxAlertsSlackWriter


def _template_file(tmp_path, source):
    """Write a Jinja template into tmp_path and return its path as a string."""
    path = tmp_path / "custom.jinja"
    path.write_text(source)
    return str(path)


def _posted_text(writer, records):
    """Write `records` through `writer` and return the text posted to Slack."""
    writer.write(records)
    return writer.client.chat_postMessage.call_args.kwargs["text"]


def _writer_with(**overrides):
    """Build a Slack writer with a mocked client."""
    writer = HyraxAlertsSlackWriter(config=_valid_config(**overrides))
    writer.client = MagicMock()
    return writer


def test_custom_template_file_controls_the_message(tmp_path):
    """A template file on disk supplies the posted text."""
    template = _template_file(tmp_path, "{{ count }} found: {{ object_ids | join('/') }}")
    writer = _writer_with(message_template=template)

    assert _posted_text(writer, _records([101, 102])) == "2 found: 101/102"


def test_bundled_template_resolves_by_name():
    """A bare bundled template name resolves without any path."""
    writer = _writer_with(message_template="detailed.jinja")

    text = _posted_text(writer, _records([101, 102]))

    assert "*2* alerts" in text
    assert "`101`" in text


def test_no_template_configured_uses_builtin_summary():
    """Without message_template the writer posts its built-in summary, unchanged."""
    writer = _writer_with()
    records = _records([101, 102])

    assert _posted_text(writer, records) == _format_batch_summary(records, max_object_ids=10)


def test_template_honors_max_object_ids(tmp_path):
    """max_object_ids is exposed to the template."""
    template = _template_file(tmp_path, "limit={{ max_object_ids }}")
    writer = _writer_with(message_template=template, max_object_ids=3)

    assert _posted_text(writer, _records([101])) == "limit=3"


def test_missing_template_raises_at_construction():
    """A template that cannot be found fails at startup, not on the first batch."""
    with pytest.raises(ValueError, match="does-not-exist.jinja"):
        HyraxAlertsSlackWriter(config=_valid_config(message_template="does-not-exist.jinja"))


def test_invalid_template_raises_at_construction(tmp_path):
    """A syntax error fails at startup, not on the first batch."""
    template = _template_file(tmp_path, "{% for x in %}")

    with pytest.raises(ValueError, match="custom.jinja"):
        HyraxAlertsSlackWriter(config=_valid_config(message_template=template))


def test_render_failure_falls_back_to_builtin_summary(tmp_path, caplog):
    """A template that blows up at render time degrades to the default summary."""
    template = _template_file(tmp_path, "{{ 1 / 0 }}")
    writer = _writer_with(message_template=template)
    records = _records([101])

    with caplog.at_level("WARNING"):
        text = _posted_text(writer, records)

    assert text == _format_batch_summary(records, max_object_ids=10)
    assert "Failed to render Slack message template" in caplog.text


def test_blank_render_skips_posting(tmp_path):
    """A template that renders nothing suppresses the message entirely."""
    template = _template_file(tmp_path, "{% if count > 0 %}{{ count }} alerts{% endif %}")
    writer = _writer_with(message_template=template)

    writer.write([])

    writer.client.chat_postMessage.assert_not_called()
