import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack

from hyrax import Hyrax

from hyrax_alerts.logging_utils import get_logger
from hyrax_alerts.writers.writer_utils import get_writers, max_writer_workers, write_batch

logger = get_logger(__name__)


def process_alerts(config_filepath=None):
    """Main function to process alerts."""
    h = Hyrax(config_file=config_filepath)
    writers = get_writers(h.config)

    # default limit is false, which means no limit.
    limit = h.config["hyrax_alerts"]["consumer"]["alert_limit"]
    count = 0

    with ExitStack() as stack:
        executor = None
        if writers:
            writers = [stack.enter_context(writer) for writer in writers]
            executor = stack.enter_context(ThreadPoolExecutor(max_workers=max_writer_workers(len(writers))))
        else:
            logger.warning("No writers configured; continuing without alert writers.")

        with h.infer_stream() as session:
            for batch_num, batch in enumerate(session.data_loader, start=1):
                logger.info(f"Processing batch {batch_num} with size {len(batch['object_id'])}")
                if not batch:
                    continue

                count += len(batch["object_id"])
                if limit and count > limit:
                    logger.info(f"Alert limit of {limit} reached. Stopping processing.")
                    break

                results = session.process(batch)
                if executor is not None:
                    write_batch(executor, writers, batch, results)


def main():
    """Main function to process alerts."""
    parser = argparse.ArgumentParser(description="Process alerts with Hyrax.")
    parser.add_argument("--config", type=str, help="Path to the configuration file", default=None)
    args = parser.parse_args()

    process_alerts(config_filepath=args.config)


if __name__ == "__main__":
    main()
