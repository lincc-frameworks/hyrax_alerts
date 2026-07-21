import argparse
import logging

from hyrax import Hyrax

logger = logging.getLogger(__name__)


def train_on_alerts(config_filepath=None):
    """Main function to process alerts."""
    h = Hyrax(config_file=config_filepath)

    with h.train_stream() as session:
        consumer = session.data_loader.dataset._stream
        for batch_num, batch in enumerate(session.data_loader, start=1):
            logger.info(f"Processing batch {batch_num} with size {len(batch['object_id'])}")
            batch = consumer.pre_filter(batch)
            batch = consumer.pre_process(batch)
            if not batch:
                continue

            model_metrics = session.process_with_trainer()

            if batch_num % 1 == 0:
                session.checkpoint(model_metrics=model_metrics)


def main():
    """Main function to process alerts."""
    parser = argparse.ArgumentParser(description="Process alerts with Hyrax.")
    parser.add_argument("--config", type=str, help="Path to the configuration file", default=None)
    args = parser.parse_args()

    train_on_alerts(config_filepath=args.config)


if __name__ == "__main__":
    main()
