# hyrax-alerts

[![Unit tests](https://img.shields.io/github/actions/workflow/status/lincc-frameworks/hyrax_alerts/testing-and-coverage.yml?label=tests)](https://github.com/lincc-frameworks/hyrax_alerts/actions/workflows/testing-and-coverage.yml)
[![Codecov](https://codecov.io/gh/lincc-frameworks/hyrax_alerts/branch/main/graph/badge.svg)](https://codecov.io/gh/lincc-frameworks/hyrax_alerts)
[![Template](https://img.shields.io/badge/Template-LINCC%20Frameworks%20Python%20Project%20Template-brightgreen)](https://lincc-ppt.readthedocs.io/en/latest/)

**Run machine-learning models on live astronomical alert streams.**

hyrax-alerts connects a Kafka alert stream to a [Hyrax](https://github.com/lincc-frameworks/hyrax) model.
You choose one of the built-in consumers to read the stream, and you can filter or transform alerts both
before and after the model runs. The results go to one or more writers, which can save them to disk or post
a summary to Slack. A single TOML config file controls the whole pipeline.

```
Kafka stream ─► consumer ─► pre_filter ─► pre_process ─► model ─► post_process ─► post_filter ─► writers
```

## Installation

hyrax-alerts requires Python 3.11 or later. It isn't on PyPI yet, so install it from GitHub:

```bash
pip install git+https://github.com/lincc-frameworks/hyrax_alerts.git
```

## Getting started

**1. Write a config file.** This example reads alerts from the Babamul stream, runs an autoencoder on them,
and saves the results to disk:

```toml
# Where alerts come from
[data_request.infer_stream.data]
dataset_class = "hyrax_alerts.consumers.babamul_consumer.BabamulConsumer"
data_location = "kafka://<host>:<port>/<topic>"   # your Babamul broker and topic
primary_id_field = "candid"                        # the field that identifies each alert
fields = ["<key_1>", "<key_2>"]                   # a list of one or more alert keys that will be passed to the model

# The model to run
[model]
name = "HyraxAutoencoderV2"

[infer_stream]
model_weights_file = "./autoencoder_weights.pth"   # weights from a trained model

# Where results go
[hyrax_alerts.writers.to_disk]
writer_class = "HyraxAlertsDiskWriter"
```

hyrax-alerts runs on Hyrax, so your config file can also use any of Hyrax's configuration parameters. The
[Hyrax default config](https://hyrax.readthedocs.io/en/stable/hyrax_default_config.html) lists them.

**2. Run it.**

```bash
hyrax_alerts --config my_config.toml
```

Or run it from Python:

```python
from hyrax_alerts.process_alerts import process_alerts

process_alerts(config_filepath="my_config.toml")
```

The pipeline keeps processing alerts as they arrive until you stop it. Each run writes its results to a new
directory, `./results/<timestamp>-hyrax_alerts-<id>/`.

## Consumers

Set `dataset_class` to one of the built-in consumers:

| `dataset_class`                                                     | Reads                                                              |
| ------------------------------------------------------------------- | ------------------------------------------------------------------ |
| `hyrax_alerts.consumers.babamul_consumer.BabamulConsumer`           | Avro-encoded alerts from the Babamul stream                        |
| `hyrax_alerts.consumers.babamul_consumer.BabamulPhotometryConsumer` | Babamul alerts converted into photometry features for a classifier |

The [consumers README](src/hyrax_alerts/consumers/README.md) describes the fields and settings for each
consumer, and explains how to add a consumer for a new Kafka source.

## Filtering and processing

You can attach your own Python functions at four points in the pipeline:

| Hook           | Set in                          | Runs                                  |
| -------------- | ------------------------------- | ------------------------------------- |
| `pre_filter`   | `[hyrax_alerts.consumer]`       | Before the model                      |
| `pre_process`  | `[hyrax_alerts.consumer]`       | Before the model, after `pre_filter`  |
| `post_process` | `[hyrax_alerts.writers.<name>]` | After the model, in that writer       |
| `post_filter`  | `[hyrax_alerts.writers.<name>]` | After the model, after `post_process` |

Each hook is a function that takes `(self, batch)` and returns a batch, where a batch is a list of alerts as
dicts:

```python
# my_hooks.py
def high_score_only(self, batch):
    return [alert for alert in batch if alert["__hyrax_result"]["data"] > 0.9]
```

```toml
[hyrax_alerts.writers.to_disk]
writer_class = "HyraxAlertsDiskWriter"
post_filter = "my_hooks.high_score_only"
```

Alerts removed by a filter are saved in a `rejected/` folder, so you can check what was dropped. Your hook
module must be importable when the run starts, so install it, add its directory to `PYTHONPATH`, or start
the run from Python in that directory.

## Writers

Each `[hyrax_alerts.writers.<name>]` table adds a writer, and you can add as many as you like:

- **`HyraxAlertsDiskWriter`** saves each batch of results to disk.
- **`HyraxAlertsSlackWriter`** posts a short summary of each batch to a Slack channel.

The [writers README](src/hyrax_alerts/writers/README.md) describes each writer's settings and the output
layout, and explains how to write your own writer.

## Development

```bash
git clone https://github.com/lincc-frameworks/hyrax_alerts.git
cd hyrax_alerts
conda create -n hyrax-alerts python=3.12 && conda activate hyrax-alerts
./.setup_dev.sh      # installs the package for development and sets up pre-commit
python -m pytest
```

For more about the project tooling, see the
[LINCC Frameworks Python Project Template](https://lincc-ppt.readthedocs.io/en/latest/) documentation.

## License

MIT. See [LICENSE](LICENSE). hyrax-alerts is developed by LINCC Frameworks.
