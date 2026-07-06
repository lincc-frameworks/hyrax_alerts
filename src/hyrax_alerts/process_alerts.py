import argparse

from hyrax import Hyrax

from hyrax_alerts.writers.base_writer import get_writers


def process_alerts(config_filepath=None):
    """Main function to process alerts."""
    h = Hyrax(config_file=config_filepath)
    writers = get_writers(h.config)

    with h.infer_stream() as session:
        print("Streaming inference session started, waiting for alerts")
        for batch in session.data_loader:
            # TODO: Can we parallelize?
            batch = session.data_loader.pre_filter(batch)
            batch = session.data_loader.pre_process(batch)
            if batch:
                results = session.process(batch)

            for writer in writers:
                # TODO: Can we parallelize?
                post_processed_results = writer.post_process(results)
                filtered_results = writer.post_filter(post_processed_results)
                writer.write(filtered_results)


if __name__ == "__main__":
    # Add CLI handling so that we can pass a config file path to the process_alerts function
    parser = argparse.ArgumentParser(description="Process alerts with Hyrax.")
    parser.add_argument("--config", type=str, help="Path to the configuration file", default=None)
    args = parser.parse_args()

    process_alerts(config_filepath=args.config)
