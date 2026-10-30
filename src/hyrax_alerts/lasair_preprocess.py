def lasair_pre_process(self, batch):
    """Basic printing for "pre-processing"

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
    for i, sample in enumerate(batch):
        print(f"Sample {i}: {sample}")
        alert = sample.get("alert")
        if alert is None:
            print("---diaSourcesList not found---")
        else:
            print("diaSourcesList:")
            # print the first 5 entries
            for i in alert["diaSourcesList"][:5]:
                print(f"  {i}")

            # print the first 5 entries
            print("diaForcedSourcesList:")
            for i in alert["diaForcedSourcesList"][:5]:
                print(f"  {i}")
    return batch
