# Writers

A writer receives a batch of alerts that have been processed by the model. It applies its own `post_process` and
`post_filter` hooks and then writes the alerts that remain. You can add any number of writers, including
several of the same class with different filters. Each `[hyrax_alerts.writers.<name>]` table in your config
adds one writer:

```toml
[hyrax_alerts.writers.to_disk]
writer_class = "HyraxAlertsDiskWriter"

[hyrax_alerts.writers.to_slack]
writer_class = "HyraxAlertsSlackWriter"
slack_token = "xoxb-..."
channel = "#hyrax-alerts"
```

You choose each writer's `<name>`, which is also the name of its output folder. Writers run in parallel,
and each one gets its own copy of the alerts, so one writer's hooks can't affect another writer.

## What a writer receives

Each batch is a list of alerts. Each alert is a dict containing its input data and the model's output:

```python
{
    "object_id": "...",                           # from primary_id_field
    "data": {"<field>": <value>, ...},            # the fields passed to the model
    "__hyrax_result": {"data": <model output>},   # or one key per output, if the model returns a dict
}
```

Values are usually numpy scalars or arrays.

## Settings for every writer

| Key             | Meaning                                                                                                    |
| --------------- | ---------------------------------------------------------------------------------------------------------- |
| `writer_class`  | `HyraxAlertsDiskWriter`, `HyraxAlertsSlackWriter`, or the class name of [your own writer](#making-a-custom-writer) |
| `post_process`  | Dotted path of a function that transforms the alerts                                                       |
| `post_filter`   | Dotted path of a function that removes alerts. It runs after `post_process`.                               |
| `reject_writer` | A table that changes where this writer's rejected alerts go ([see below](#output-and-rejected-alerts))     |

Each hook takes `(self, batch)` and returns a batch, where `self` is the writer. A `post_filter` should
return the alerts it keeps without copying them, because hyrax-alerts compares the returned dicts with the
originals to see which alerts were removed:

```python
# my_hooks.py
def high_score_only(self, batch):
    return [alert for alert in batch if alert["__hyrax_result"]["data"] > 0.9]
```

```toml
[hyrax_alerts.writers.to_slack]
writer_class = "HyraxAlertsSlackWriter"
post_filter = "my_hooks.high_score_only"
# ...
```

## `HyraxAlertsDiskWriter`

Saves each batch of alerts as a numpy file.

| Key                        | Default                              | Meaning                                                                                                     |
| -------------------------- | ------------------------------------ | ----------------------------------------------------------------------------------------------------------- |
| `output_location`          | `<run directory>/<name>/`            | The directory to write to                                                                                   |
| `timestamped_subdirectory` | `true`                               | If `output_location` is set, write each run to a new `<timestamp>-hyrax_alerts-<id>` folder inside it. If `false`, write directly into `output_location`. |

The writer saves batches as `batch_00000000.npy`, `batch_00000001.npy`, and so on. It creates its
directory when it writes the first batch. To read a batch back:

```python
import numpy as np

alerts = np.load("results/<run>/to_disk/batch_00000000.npy", allow_pickle=True)
```

## `HyraxAlertsSlackWriter`

Posts a short summary of each batch to a Slack channel. It's meant for notifications, so pair it with a
disk writer if you need complete results.

| Key              | Default  | Meaning                                                     |
| ---------------- | -------- | ----------------------------------------------------------- |
| `slack_token`    | required | A Slack bot token (`xoxb-...`)                              |
| `channel`        | required | The channel's name (`#hyrax-alerts`) or id (`C0123ABCD`)    |
| `max_object_ids` | `10`     | How many object ids to list in a message before `(+N more)` |

The writer posts once per batch. A batch where every alert was filtered out still gets a post, which says
*"received a batch with no objects"*. If a post fails, the writer prints a warning and the run continues.

To get a token, [create a Slack app](https://api.slack.com/apps) and give it the `chat:write` bot scope.
Install the app to your workspace, then invite the bot to your channel. The token is stored in your config
file, so keep that file out of version control.

## Output and rejected alerts

All of a run's output goes in one directory:

```
results/<timestamp>-hyrax_alerts-<id>/
├── to_disk/            ← alerts the writer kept
└── rejected/
    ├── consumer/       ← alerts removed by pre_filter
    └── to_disk/        ← alerts removed by to_disk's post_filter
```

Rejected alerts are saved in the same `.npy` format. Each one is tagged with the stage that removed it:
`"__hyrax_filter_stage": "pre_filter"` or `"post_filter"`. The `reject_output_root` setting controls where
rejected alerts go:

```toml
[hyrax_alerts]
reject_output_root = true            # default: in the run directory, as shown above
# reject_output_root = false         # don't save them (the logs still count them)
# reject_output_root = "/data/rej"   # in a new timestamped folder under this path
```

To send one writer's rejected alerts somewhere else, give that writer a `reject_writer` table:

```toml
[hyrax_alerts.writers.to_disk.reject_writer]
writer_class = "HyraxAlertsDiskWriter"
output_location = "./to_disk_rejects"
```

## Making a custom writer

To send results somewhere new, subclass `HyraxAlertsBaseWriter` and implement `write()`. If your writer
holds resources such as open files, also override `close()`, which runs when the pipeline stops. Your writer
gets `post_process`, `post_filter`, and rejected-alert handling automatically. Every key in its config table
is available in `config`:

```python
# my_writers.py
import csv

from hyrax_alerts.writers.base_writer import HyraxAlertsBaseWriter


class ScoreCsvWriter(HyraxAlertsBaseWriter):
    """Append one row per alert: object id and model score."""

    def __init__(self, config):
        super().__init__(config)
        self.file = open(config.get("path", "scores.csv"), "a", newline="")
        self.rows = csv.writer(self.file)

    def write(self, result_batch):
        for record in result_batch:
            self.rows.writerow([record["object_id"], float(record["__hyrax_result"]["data"])])

    def close(self):
        self.file.close()
```

```toml
# my_config.toml
[hyrax_alerts.writers.scores]
writer_class = "ScoreCsvWriter"
path = "./scores.csv"
```

Writers are looked up by class name, and a class is registered when its module is imported. Make sure that
happens before the run starts. The simplest way is to start the run from Python:

```python
import my_writers  # registers ScoreCsvWriter

from hyrax_alerts.process_alerts import process_alerts

process_alerts(config_filepath="my_config.toml")
```
