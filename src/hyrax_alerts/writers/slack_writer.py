from hyrax_alerts.logging_utils import get_logger
from hyrax_alerts.writers.base_writer import HyraxAlertsBaseWriter

# NOTE: This writer posts a short, human-readable summary of each batch to a Slack
# channel. It intentionally does not send the raw model output or source data -
# Slack is a notification surface, not a results store. Use the disk writer (or a
# future database writer) for durable, complete output.

# NOTE: The slack_sdk import is deferred into _load_client_class so that
# hyrax_alerts.writers can import this module - and therefore register this
# writer - without the `slack` extra installed.

logger = get_logger(__name__)

# Default number of object ids to enumerate in a summary message before
# truncating with a "(+N more)" suffix. Keeps messages readable and well under
# Slack's message size limits.
DEFAULT_MAX_OBJECT_IDS = 10

SLACK_IMPORT_ERROR = (
    "HyraxAlertsSlackWriter requires slack_sdk. Install it with: pip install 'hyrax_alerts[slack]'"
)


def _load_client_class():
    """Import and return ``slack_sdk.WebClient``.

    Returns
    -------
    type
        The ``slack_sdk.WebClient`` class.

    Raises
    ------
    ImportError
        If slack_sdk is not installed, with a message naming the extra that
        provides it.
    """
    try:
        from slack_sdk import WebClient
    except ImportError as error:
        raise ImportError(SLACK_IMPORT_ERROR) from error
    return WebClient


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

    Requires the ``slack`` extra: ``pip install 'hyrax_alerts[slack]'``.

    Required configuration keys:

    - ``slack_token``: a Slack bot token (``xoxb-...``).
    - ``channel``: the channel to post to, as an id (``C0123ABCD``) or name
      (``#hyrax-alerts``).

    Optional configuration keys:

    - ``max_object_ids``: how many object ids to list per message before
      truncating (default ``10``).

    Example configuration:

    .. code-block:: toml

        [hyrax_alerts.writers.to_slack_0]
        writer_class = "HyraxAlertsSlackWriter"
        slack_token = "xoxb-your-token-here"
        channel = "#hyrax-alerts"
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

        client_class = _load_client_class()
        self.client = client_class(token=self.slack_token)

    def write(self, result_batch: list[dict]):
        """Post a summary of a batch of results to the configured Slack channel."""
        summary = _format_batch_summary(result_batch, self.max_object_ids)

        try:
            self.client.chat_postMessage(channel=self.channel, text=summary)
        except Exception as error:
            # A failure to notify should not abort the whole processing run, and
            # write_batch turns any writer exception into one that does - so this
            # deliberately catches more than just SlackApiError. A DNS failure or
            # a connection reset is just as much "we could not notify" as an API
            # error is.
            logger.error(f"Failed to post Hyrax alerts summary to Slack: {error}")

    def __enter__(self):
        """Enter the context manager for the writer."""
        return self

    def close(self):
        """Close the writer and release any resources. The Slack ``WebClient``
        holds no persistent connection, so there is nothing to clean up."""
        pass
