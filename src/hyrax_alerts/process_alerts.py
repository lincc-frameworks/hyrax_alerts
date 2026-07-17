import argparse
import os
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from copy import deepcopy

from hyrax import Hyrax

from hyrax_alerts.writers.base_writer import get_writers

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


def process_alerts(config_filepath=None):
    """Main function to process alerts."""
    h = Hyrax(config_file=config_filepath)
    writers = get_writers(h.config)

    with ExitStack() as stack:
        executor = None
        if writers:
            writers = [stack.enter_context(writer) for writer in writers]
            executor = stack.enter_context(ThreadPoolExecutor(max_workers=_max_writer_workers(len(writers))))

        with h.infer_stream() as session:
            consumer = session.data_loader.dataset._stream
            for _, batch in enumerate(session.data_loader):
                # TODO: Log "Processing batch {i + 1} with size {len(batch['object_id'])}")
                batch = consumer.pre_filter(batch)
                batch = consumer.pre_process(batch)
                if not batch:
                    continue

                results = session.process(batch)
                if not writers:
                    continue

                futures = [
                    (executor.submit(_run_writer, writer, batch, results), writer) for writer in writers
                ]
                for future, writer in futures:
                    try:
                        future.result()
                    except Exception as error:
                        message = (
                            f"Writer {writer.__class__.__name__} failed while processing a batch: "
                            f"{type(error).__name__}"
                        )
                        raise RuntimeError(message) from error


def main():
    """Main function to process alerts."""
    parser = argparse.ArgumentParser(description="Process alerts with Hyrax.")
    parser.add_argument("--config", type=str, help="Path to the configuration file", default=None)
    args = parser.parse_args()

    process_alerts(config_filepath=args.config)


if __name__ == "__main__":
    main()
