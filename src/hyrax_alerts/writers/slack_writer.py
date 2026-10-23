from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

from hyrax_alerts.logging_utils import get_logger
from hyrax_alerts.template_utils import build_batch_context, load_template, render, resolve_template_path
from hyrax_alerts.writers.base_writer import HyraxAlertsBaseWriter

# NOTE: This writer posts a short, human-readable summary of each batch to a Slack
# channel. It intentionally does not send the raw model output or source data -
# Slack is a notification surface, not a results store. Use the disk writer (or a
# future database writer) for durable, complete output.

logger = get_logger(__name__)

# Default number of object ids to enumerate in a summary message before
# truncating with a "(+N more)" suffix. Keeps messages readable and well under
# Slack's message size limits.
DEFAULT_MAX_OBJECT_IDS = 10


def _format_batch_summary(result_batch: list[dict], max_object_ids: int = DEFAULT_MAX_OBJECT_IDS) -> str:
    """Build a short, human-readable summary of a batch for posting to Slack.

    Parameters
    ----------
    result_batch : list[dict]
        The post-processed, post-filtered results for this batch. Each dictionary
        is expected to contain an ``object_id`` entry, as produced by the Hyrax
        alerts consumers.
    max_object_ids : int, optional
        Maximum number of object ids to list explicitly before truncating,
        by default ``DEFAULT_MAX_OBJECT_IDS``.

    Returns
    -------
    str
        A summary string suitable for a Slack message.
    """
    object_ids = [record["object_id"] for record in result_batch if "object_id" in record]
    count = len(object_ids)

    if count == 0:
        return ":satellite_antenna: Hyrax alerts: received a batch with no objects."

    shown = object_ids[:max_object_ids]
    ids_text = ", ".join(str(object_id) for object_id in shown)
    remaining = count - len(shown)
    if remaining > 0:
        ids_text += f", ... (+{remaining} more)"

    noun = "alert" if count == 1 else "alerts"
    return f":satellite_antenna: Hyrax alerts: {count} {noun} - objects: {ids_text}"


class HyraxAlertsSlackWriter(HyraxAlertsBaseWriter):
    """Writer class for posting a per-batch summary to a Slack channel.

    Required configuration keys:

    - ``slack_token``: a Slack bot token (``xoxb-...``).
    - ``channel``: the channel to post to, as an id (``C0123ABCD``) or name
      (``#hyrax-alerts``).

    Optional configuration keys:

    - ``max_object_ids``: how many object ids to list per message before
      truncating (default ``10``).
    - ``message_template``: path to a Jinja template file controlling the
      message text. Either a path to your own ``.jinja`` file, or the bare name
      of one of the templates bundled in ``hyrax_alerts/message_templates/``
      (``default.jinja``, ``detailed.jinja``). Without this key the writer posts
      its built-in summary.

    Templates are rendered with these variables:

    - ``records``: one dictionary per alert, exactly as :meth:`write` gets them.
    - ``count``: the number of records in the batch.
    - ``object_ids``: the ``object_id`` of every record that has one.
    - ``max_object_ids``: the configured truncation budget.

    Model output lives under each record's ``__hyrax_result`` key, which reads
    most clearly with subscript syntax: ``{{ record['__hyrax_result']['data'] }}``.
    Referencing a field a record lacks renders as empty rather than failing,
    since the keys under ``data`` and ``__hyrax_result`` depend on the
    configured consumer and the model. A ``py`` filter is available to convert
    numpy arrays to plain Python, which keeps a multi-dimensional array on one
    line and lets Jinja's list filters work on it.

    A template that renders to nothing suppresses the message entirely, so
    guarding on ``{% if count > 0 %}`` avoids posting for empty batches.

    Example configuration:

    .. code-block:: toml

        [hyrax_alerts.writers.to_slack_0]
        writer_class = "HyraxAlertsSlackWriter"
        slack_token = "xoxb-your-token-here"
        channel = "#hyrax-alerts"
        message_template = "detailed.jinja"
    """

    def __init__(self, config):
        super().__init__(config)

        self.slack_token = config.get("slack_token")
        if not isinstance(self.slack_token, str) or not self.slack_token.strip():
            raise ValueError("HyraxAlertsSlackWriter requires a non-empty string 'slack_token'.")

        self.channel = config.get("channel")
        if not isinstance(self.channel, str) or not self.channel.strip():
            raise ValueError("HyraxAlertsSlackWriter requires a non-empty string 'channel'.")

        self.max_object_ids = config.get("max_object_ids", DEFAULT_MAX_OBJECT_IDS)

        # NOTE: the template is resolved and compiled here rather than lazily on
        # first write, so that a missing file or a syntax error surfaces at
        # startup instead of partway through a long-running stream.
        template_config = config.get("message_template")
        self.template = load_template(resolve_template_path(template_config)) if template_config else None

        self.client = WebClient(token=self.slack_token)

    def _build_message(self, result_batch: list[dict]) -> str:
        """Return the message text for a batch, from the template if one is configured."""
        if self.template is None:
            return _format_batch_summary(result_batch, self.max_object_ids)

        context = build_batch_context(result_batch, max_object_ids=self.max_object_ids)
        try:
            return render(self.template, context)
        except Exception as error:
            # A template that fails on one batch - say, one calling a filter on a
            # value of an unexpected type - should degrade to the built-in
            # summary rather than take down the run. write_batch() re-raises
            # anything a writer throws as a RuntimeError that aborts all
            # processing, and a notification is never worth that.
            #
            # NOTE: this catches Exception rather than jinja2.TemplateError on
            # purpose. Jinja only raises TemplateError for its own failures
            # (undefined values, bad syntax); an exception raised by the
            # expressions inside a template propagates as its original type.
            logger.warning(f"Failed to render Slack message template, using the default summary: {error}")
            return _format_batch_summary(result_batch, self.max_object_ids)

    def write(self, result_batch: list[dict]):
        """Post a summary of a batch of results to the configured Slack channel."""
        summary = self._build_message(result_batch)

        # Writers are called even for empty batches, so a template that renders
        # nothing is how a user opts out of "no alerts this time" noise.
        if not summary:
            return

        try:
            self.client.chat_postMessage(channel=self.channel, text=summary)
        except SlackApiError as error:
            # A failure to notify should not abort the whole processing run.
            logger.warning(f"Failed to post Hyrax alerts summary to Slack: {error}")

    def __enter__(self):
        """Enter the context manager for the writer."""
        return self

    def close(self):
        """Close the writer and release any resources. The Slack ``WebClient``
        holds no persistent connection, so there is nothing to clean up."""
        pass
