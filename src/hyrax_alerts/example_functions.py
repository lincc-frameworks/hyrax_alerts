def example_pre_filter(self, batch):
    """Basic no-op pre-filtering function that returns the input batch unchanged.

    Parameters
    ----------
    self : object
        The instance of the class calling this function.
    batch : list
        A batch of data to be pre-filtered.

    Returns
    -------
    list
        The pre-filtered batch of data, unchanged from the input.
    """
    print("Example pre-filtering function called.")
    return batch


def example_pre_process(self, batch):
    """Basic no-op pre-processing function that returns the input batch unchanged.

    Parameters
    ----------
    self : object
        The instance of the class calling this function.
    batch : list
        A batch of data to be pre-processed.

    Returns
    -------
    list
        The pre-processed batch of data, unchanged from the input.
    """
    print("Example pre-processing function called.")
    return batch


def example_post_process(self, result_batch):
    """Basic no-op post-processing function that returns the input result batch
    unchanged.

    Parameters
    ----------
    self : object
        The instance of the class calling this function.
    result_batch : list
        A batch of results to be post-processed.

    Returns
    -------
    list
        The post-processed batch of results, unchanged from the input.
    """
    print("Example post-processing function called.")
    return result_batch


def example_post_filter(self, result_batch):
    """Basic no-op post-filtering function that keeps each result.

    Parameters
    ----------
    self : object
        The instance of the class calling this function.
    result_batch : list
        A batch of results to be post-filtered.

    Returns
    -------
    list
        A list of boolean values indicating which results to keep, all True in
        this example.
    """
    print("Example post-filtering function called.")
    return [True] * len(result_batch)
