def example_pre_filter(self, batch):
    """Basic no-op pre-filtering function that returns the input batch unchanged."""
    print("Example pre-filtering function called.")
    return batch


def example_pre_process(self, batch):
    """Basic no-op pre-processing function that returns the input batch unchanged."""
    print("Example pre-processing function called.")
    return batch


def example_post_process(self, result_batch):
    """Basic no-op post-processing function that returns the input result batch unchanged."""
    print("Example post-processing function called.")
    return result_batch


def example_post_filter(self, result_batch):
    """Basic no-op post-filtering function that returns the input result batch unchanged."""
    print("Example post-filtering function called.")
    return result_batch
