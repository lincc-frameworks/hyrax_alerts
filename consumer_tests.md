Can you set up some unit tests for the basic `hyrax_alerts` functionality of the `Consumer`, where you mock out a Kafka alert and make sure that the `HyraxKafkaConsumer` in src/hyrax/consumers/kafka_consumer.py can ingest those mocked alerts in a manner similar to how it works in `process_alerts` in src/hyrax/process_alerts.py?

Please put at least one test in a new file, tests/hyrax_alerts/test_kafka_consumer.py.

As an example of how to mock kafka alerts within the `hyrax` ecosystem, you can take a look at the setup here: https://github.com/lincc-frameworks/hyrax/blob/main/tests/hyrax/test_kafka_stream_dataset.py
