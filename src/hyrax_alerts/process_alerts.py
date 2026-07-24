import argparse
import os
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from copy import deepcopy

from hyrax import Hyrax

from hyrax_alerts.logging_utils import get_logger
from hyrax_alerts.writers.base_writer import get_writers

logger = get_logger(__name__)


# Writers are I/O-bound, so a small 2x oversubscription improves throughput
# without creating an excessive number of threads.
WRITER_THREAD_MULTIPLIER = 2

def _run_writer(writer, batch, results):
    """Post-process, filter, and write one batch for one writer."""
    processed_results = writer.post_process(deepcopy(results))
    filtered_batch, filtered_results = writer._post_filter_batches(deepcopy(batch), processed_results)
    writer.write(filtered_batch, filtered_results)


def _max_writer_workers(writer_count):
    """Return a bounded worker count for parallel writer execution."""
    cpus = os.cpu_count() or 1
    return min(writer_count, max(1, cpus * WRITER_THREAD_MULTIPLIER))


def _write_batch(executor, writers, batch, results):
    """Write one processed batch to all configured writers."""
    futures = [(executor.submit(_run_writer, writer, batch, results), writer) for writer in writers]
    for future, writer in futures:
        try:
            future.result()
        except Exception as error:
            message = (
                f"Writer {writer.__class__.__name__} failed while processing a batch: "
                f"{type(error).__name__}"
            )
            raise RuntimeError(message) from error


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
            executor = stack.enter_context(ThreadPoolExecutor(max_workers=_max_writer_workers(len(writers))))
        else:
            logger.warning("No writers configured; continuing without alert writers.")

        with h.infer_stream() as session:
            consumer = session.data_loader.dataset._stream
            for batch_num, batch in enumerate(session.data_loader, start=1):
                logger.info(f"Processing batch {batch_num} with size {len(batch['object_id'])}")
                batch = consumer.pre_filter(batch)
                batch = consumer.pre_process(batch)
                if not batch:
                    continue

                count += len(batch["object_id"])
                if limit and count > limit:
                    logger.info(f"Alert limit of {limit} reached. Stopping processing.")
                    break

                results = session.process(batch)
                if executor is not None:
                    _write_batch(executor, writers, batch, results)


def main():
    """Main function to process alerts."""
    parser = argparse.ArgumentParser(description="Process alerts with Hyrax.")
    parser.add_argument("--config", type=str, help="Path to the configuration file", default=None)
    args = parser.parse_args()

    process_alerts(config_filepath=args.config)


if __name__ == "__main__":
    main()
