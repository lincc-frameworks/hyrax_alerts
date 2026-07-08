import argparse
from contextlib import ExitStack

from hyrax import Hyrax

from hyrax_alerts.writers.base_writer import get_writers


def process_alerts(config_filepath=None):
    """Main function to process alerts."""
    h = Hyrax(config_file=config_filepath)
    writers = get_writers(h.config)

    with ExitStack() as stack:
        writers = [stack.enter_context(writer) for writer in writers]

        with h.infer_stream() as session:
            print("Streaming inference session started, waiting for alerts")
            consumer = session.data_loader.dataset._stream
            for i, batch in enumerate(session.data_loader):
                # TODO: Log "Processing batch {i + 1} with size {len(batch['object_id'])}")
                batch = consumer.pre_filter(batch)
                batch = consumer.pre_process(batch)
                if not batch:
                    continue

                results = session.process(batch)

                # TODO: Parallelize writers
                for writer in writers:
                    processed_results = writer.post_process(results)
                    filtered_batch, filtered_results = writer._post_filter_batches(batch, processed_results)
                    writer.write(filtered_batch, filtered_results)


if __name__ == "__main__":
    # Add CLI handling so that we can pass a config file path to the process_alerts function
    parser = argparse.ArgumentParser(description="Process alerts with Hyrax.")
    parser.add_argument("--config", type=str, help="Path to the configuration file", default=None)
    args = parser.parse_args()

    process_alerts(config_filepath=args.config)
