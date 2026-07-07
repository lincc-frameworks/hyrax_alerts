from importlib import import_module


def load_callable(dotted_path: str) -> callable:
    """Load a callable from a dotted path like package.module.function.

    Parameters
    ----------
    dotted_path : str
        The dotted path to the callable.

    Returns
    -------
    callable
        The loaded callable.

    Raises
    ------
    TypeError
        If the loaded object is not callable.
    """
    module_path, func_name = dotted_path.rsplit(".", 1)
    module = import_module(module_path)
    function = getattr(module, func_name)
    if not callable(function):
        raise TypeError(f"{dotted_path} is not callable")
    return function
