import os
from copy import deepcopy

from hyrax_alerts.decollation_utils import merge_batch
from hyrax_alerts.filter_utils import apply_filter
from hyrax_alerts.logging_utils import get_logger
from hyrax_alerts.writers.base_writer import get_reject_writer, instantiate_writer

logger = get_logger(__name__)


# Writers are I/O-bound, so a small 2x oversubscription improves throughput
# without creating an excessive number of threads.
WRITER_THREAD_MULTIPLIER = 2


def _run_writer(writer, records):
    """Post-process, filter, and write one batch of records for one writer."""
    processed_results = writer.post_process(deepcopy(records))
    filtered_results, removed_batch = apply_filter(writer.post_filter, processed_results)
    if removed_batch:
        logger.info(f"Removed {len(removed_batch)} items from batch due to post_filter.")
        if writer.reject_writer is not None:
            tagged = [{**record, "__hyrax_filter_stage": "post_filter"} for record in removed_batch]
            writer.reject_writer.write(tagged)
        else:
            logger.info(
                "Configure 'reject_output_root' or this writer's 'reject_writer' to persist removed records."
            )
    writer.write(filtered_results)


def max_writer_workers(writer_count):
    """Return a bounded worker count for parallel writer execution."""
    cpus = os.cpu_count() or 1
    return min(writer_count, max(1, cpus * WRITER_THREAD_MULTIPLIER))


def write_batch(executor, writers, batch, results):
    """Write one processed batch to all configured writers."""
    records = merge_batch(batch, results)
    futures = [(executor.submit(_run_writer, writer, records), writer) for writer in writers]
    for future, writer in futures:
        try:
            future.result()
        except Exception as error:
            message = (
                f"Writer {writer.__class__.__name__} failed while processing a batch: {type(error).__name__}"
            )
            raise RuntimeError(message) from error


def get_writers(config):
    """Return a list of writer instances based on the provided configuration.
    Parameters
    ----------
    config : dict
        Configuration dictionary containing writer settings.

    Returns
    -------
    list
        A list of instantiated writer objects.
    """

    writers = []
    hyrax_alerts_config = config.get("hyrax_alerts", {})
    writer_config = hyrax_alerts_config.get("writers", {})
    reject_output_root = hyrax_alerts_config.get("reject_output_root")
    for writer_friendly_name, writer_cfg in writer_config.items():
        writer = instantiate_writer(writer_cfg)
        if writer is not None:
            writer.reject_writer = get_reject_writer(writer_cfg, writer_friendly_name, reject_output_root)
            writers.append(writer)
            logger.info(f"Created writer '{writer_friendly_name}' of class '{writer_cfg['writer_class']}'")
    return writers
