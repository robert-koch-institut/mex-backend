import contextlib
from collections.abc import Callable
from typing import ParamSpec, TypeVar

T = TypeVar("T")
P = ParamSpec("P")


def extend_list_in_dict(dict_: dict[str, list[T]], key: str, item: list[T] | T) -> None:
    """Extend a list in a dict for a given key with the given unique item(s)."""
    list_ = dict_.setdefault(key, [])
    if not isinstance(item, list):
        item = [item]
    for mergeable in item:
        if mergeable not in list_:
            list_.append(mergeable)


def prune_list_in_dict(dict_: dict[str, list[T]], key: str, item: list[T] | T) -> None:
    """Safely remove item(s) from a list in a dict for the given key."""
    list_ = dict_.setdefault(key, [])
    if not isinstance(item, list):
        item = [item]
    for removable in item:
        with contextlib.suppress(ValueError):
            list_.remove(removable)


def reraising(
    original_error: type[Exception],
    reraise_as: type[Exception],
    fn: Callable[P, T],
    *args: P.args,
    **kwargs: P.kwargs,
) -> T:
    """Execute a function and catch a specific exception, re-raising it as a different.

    This utility allows you to call any given function `fn` with its arguments `args`
    and `kwargs`, catch a specified exception type (`original_error`), and re-raise
    it as another exception type (`reraise_as`) while preserving the original traceback.

    Args:
        original_error: The exception class to catch.
        reraise_as: The exception class to re-raise as.
        fn: The function to be called.
        *args: Positional arguments to be passed to the function `fn`.
        **kwargs: Keyword arguments to be passed to the function `fn`.

    Raises:
        reraise_as: If `fn` raises an exception of type `original_error`, it is caught
                    and re-raised as `reraise_as`, while preserving the original
                    exception as the cause.

    Returns:
        The return value of the function `fn` if it executes successfully.
    """
    try:
        return fn(*args, **kwargs)
    except original_error as error:
        raise reraise_as from error
