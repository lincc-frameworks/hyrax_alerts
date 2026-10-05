# Consumers

A consumer reads alerts from a Kafka topic and decodes them. It then applies your `pre_filter` and
`pre_process` hooks and passes the remaining alerts to the model. You choose a consumer with `dataset_class`:

```toml
[data_request.infer_stream.data]
dataset_class = "hyrax_alerts.consumers.babamul_consumer.BabamulConsumer"
data_location = "kafka://<host>:<port>/<topic>"   # your Babamul broker and topic
primary_id_field = "candid"                        # the field that identifies each alert
fields = ["<key_1>", "<key_2>"]                   # a list of one or more alert keys that will be passed to the model
```

| Key                | Meaning                                                                         |
| ------------------ | ------------------------------------------------------------------------------- |
| `dataset_class`    | The full dotted path of the consumer class                                      |
| `data_location`    | The Kafka broker and topic, as `kafka://<host>:<port>/<topic>`                  |
| `primary_id_field` | The field that identifies each alert. It becomes the alert's `object_id`.       |
| `fields`           | The fields passed to the model. Each one must be a field the consumer provides. |

## Built-in consumers

Every consumer returns alerts in batches. A batch is a list of dictionaries, and each dictionary is one
alert. You can set the batch size with the `data_loader.batch_size` config parameter. If not enough alerts
arrive to fill a batch before a timeout passes, the consumer returns a partial batch, so a quiet stream
still gets processed.

### `BabamulConsumer`

`hyrax_alerts.consumers.babamul_consumer.BabamulConsumer` reads Avro-encoded alerts from the Babamul stream.

| Field    | Value                                                   |
| -------- | ------------------------------------------------------- |
| `candid` | The alert's candidate id. Use it as `primary_id_field`. |

### `BabamulPhotometryConsumer`

`hyrax_alerts.consumers.babamul_consumer.BabamulPhotometryConsumer` extends `BabamulConsumer`. It converts
each alert's photometry history into light-curve features for a photometry classifier. It was designed
for the AppleCiDEr `HyraxBaselineCLS` model.

| Field        | Value                                                                                                                                                                  |
| ------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `candid`     | The alert's candidate id, as in `BabamulConsumer`                                                                                                                      |
| `photometry` | One row per observation, with 7 columns: time since the first observation, time since the previous observation, log10 flux, log10 flux error, and a one-hot g/r/i band. Each batch is padded or truncated to 257 observations, and a `pad_mask` marks the padding. |
| `mean`       | The mean of the first four photometry columns, read from `stats_path`                                                                                                  |
| `std`        | The standard deviation of the first four photometry columns, read from `stats_path`                                                                                    |

`BabamulPhotometryConsumer` needs the feature statistics that were computed from the model's training set:

```toml
[hyrax_alerts.consumer.BabamulPhotometryConsumer]
stats_path = "./feature_stats_day100.npz"   # an .npz file with "mean" and "std" arrays
```

### `LocalTestingConsumer`

`hyrax_alerts.consumers.local_testing_consumer.LocalTestingConsumer` is for testing and development. It
reads synthetic JSON messages sent by Hyrax's
[test producer](https://github.com/lincc-frameworks/hyrax/blob/main/stream_test/producer.py), and provides
the fields `id`, `image`, `label`, `value`, `timestamp`, and `time_series`.

## Consumer settings

These settings apply whichever consumer you choose:

```toml
[hyrax_alerts.consumer]
pre_filter = "my_hooks.keep_well_sampled"   # remove alerts before the model runs
pre_process = "my_hooks.add_features"       # transform alerts before the model runs
alert_limit = false                         # process at most this many alerts, then stop; false means no limit
```

Each hook is the dotted path of a function that takes `(self, batch)` and returns a batch. Here, `batch` is a
list of decoded alerts as dicts, and `self` is the consumer. A `pre_filter` should return the alerts it
keeps without copying them, because hyrax-alerts compares the returned dicts with the originals to see which
alerts were removed:

```python
# my_hooks.py
def keep_well_sampled(self, batch):
    """Keep alerts with at least 3 photometry points."""
    return [alert for alert in batch if len(alert["fp_hists"]) >= 3]
```

Alerts removed by `pre_filter` are saved in `rejected/consumer/` in the run's output directory. The
[writers README](../writers/README.md#output-and-rejected-alerts) explains how to change this.

By default, both hooks are example functions that pass every alert through and print one line per batch.

`alert_limit` never splits a batch. If the next batch would take the total past the limit, the run stops
without processing that batch.

## Adding a new Kafka source

`hyrax_alerts.consumers.kafka_consumer.HyraxKafkaConsumer` is the base class for every consumer here. It
connects to Kafka, decodes JSON messages, and applies the `pre_filter` and `pre_process` hooks. It isn't
meant to be used directly. To read a new source, subclass it and add a `get_<field>` method for
`primary_id_field` and for each entry in `fields`. Each method receives one decoded alert and returns that
field's value:

```python
# my_package/consumers.py
import numpy as np

from hyrax_alerts.consumers.kafka_consumer import HyraxKafkaConsumer


class MySurveyConsumer(HyraxKafkaConsumer):
    def get_alert_id(self, alert):
        return alert["alert_id"]

    def get_cutout(self, alert):
        return np.asarray(alert["cutout"], dtype=np.float32)
```

```toml
[data_request.infer_stream.data]
dataset_class = "my_package.consumers.MySurveyConsumer"
data_location = "kafka://broker.example.org:9092/alerts"
primary_id_field = "alert_id"
fields = ["cutout"]

# hyrax-alerts loads its default settings only when dataset_class is inside the
# hyrax_alerts package. A consumer from another package needs this table.
[hyrax_alerts.consumer]
alert_limit = false
```

You can also override these methods:

- **`_decode(self, msg)`** decodes messages that aren't JSON. It receives the raw Kafka message, so
  `msg.value()` holds the message bytes. `BabamulConsumer` overrides it to decode Avro.
- **`collate_<field>(self, samples)`** controls how a field's values are combined into a batch, for example
  by padding variable-length light curves. `BabamulPhotometryConsumer.collate_photometry` is an example.

[`babamul_consumer.py`](babamul_consumer.py) and [`local_testing_consumer.py`](local_testing_consumer.py)
are complete working examples.
