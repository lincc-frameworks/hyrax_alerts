from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

from hyrax_alerts.writers.base_writer import HyraxAlertsBaseWriter

# NOTE: This writer posts a short, human-readable summary of each batch to a Slack
# channel. It intentionally does not send the raw model output or source data -
# Slack is a notification surface, not a results store. Use the disk writer (or a
# future database writer) for durable, complete output.

# Default number of object ids to enumerate in a summary message before
# truncating with a "(+N more)" suffix. Keeps messages readable and well under
# Slack's message size limits.
DEFAULT_MAX_OBJECT_IDS = 10


def _format_batch_summary(
    data_batch: dict, result_batch, max_object_ids: int = DEFAULT_MAX_OBJECT_IDS
) -> str:
    """Build a short, human-readable summary of a batch for posting to Slack.

    Parameters
    ----------
    data_batch : dict
        A batch of input data. Expected to contain an ``object_id`` entry, as
        produced by the Hyrax alerts consumers.
    result_batch : list | dict
        The post-processed, post-filtered model results for this batch. Only its
        length is used for the summary.
    max_object_ids : int, optional
        Maximum number of object ids to list explicitly before truncating,
        by default ``DEFAULT_MAX_OBJECT_IDS``.

    Returns
    -------
    str
        A summary string suitable for a Slack message.
    """
    object_ids = list(data_batch.get("object_id", [])) if isinstance(data_batch, dict) else []
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

        self.client = WebClient(token=self.slack_token)

    def write(self, data_batch: dict, result_batch: list | dict):
        """Post a summary of a batch of results to the configured Slack channel."""
        summary = _format_batch_summary(data_batch, result_batch, self.max_object_ids)

        try:
            self.client.chat_postMessage(channel=self.channel, text=summary)
        except SlackApiError as error:
            # A failure to notify should not abort the whole processing run.
            # TODO: Log the Slack API error instead of printing once logging is available.
            print(f"Warning: failed to post Hyrax alerts summary to Slack: {error}")

    def __enter__(self):
        """Enter the context manager for the writer."""
        return self

    def close(self):
        """Close the writer and release any resources. The Slack ``WebClient``
        holds no persistent connection, so there is nothing to clean up."""
        pass
