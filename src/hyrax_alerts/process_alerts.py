import argparse
from contextlib import ExitStack

from hyrax import Hyrax

from hyrax_alerts.writers.base_writer import get_writers


def process_alerts(config_filepath=None):
    """Main function to process alerts."""
    h = Hyrax(config_file=config_filepath)
    writers = get_writers(h.config)

    # default limit is false, which means no limit.
    limit = h.config["hyrax_alerts"]["consumer"]["alert_limit"]
    count = 0

    with ExitStack() as stack:
        writers = [stack.enter_context(writer) for writer in writers]

        with h.infer_stream() as session:
            consumer = session.data_loader.dataset._stream
            for _, batch in enumerate(session.data_loader):
                # TODO: Log "Processing batch {i + 1} with size {len(batch['object_id'])}")
                batch = consumer.pre_filter(batch)
                batch = consumer.pre_process(batch)
                if not batch:
                    continue

                count += len(batch["object_id"])
                if limit and count > limit:
                    print(f"Alert limit of {limit} reached. Stopping processing.")
                    break

                results = session.process(batch)

                # TODO: Parallelize writers
                for writer in writers:
                    processed_results = writer.post_process(results)
                    filtered_batch, filtered_results = writer._post_filter_batches(batch, processed_results)
                    writer.write(filtered_batch, filtered_results)


def main():
    """Main function to process alerts."""
    parser = argparse.ArgumentParser(description="Process alerts with Hyrax.")
    parser.add_argument("--config", type=str, help="Path to the configuration file", default=None)
    args = parser.parse_args()

    process_alerts(config_filepath=args.config)


if __name__ == "__main__":
    main()
