from typing import TypeVar

T = TypeVar("T")


def prune_list_in_dict(dict_: dict[str, list[T]], key: str, item: list[T] | T) -> None:
    """Safely remove item(s) from a list in a dict for the given key."""
    list_ = dict_.setdefault(key, [])
    if not isinstance(item, list):
        item = [item]
    for removable in item:
        try:
            list_.remove(removable)
        except ValueError:
            pass


def extend_list_in_dict(dict_: dict[str, list[T]], key: str, item: list[T] | T) -> None:
    """Extend a list in a dict for a given key with the given unique item(s)."""
    list_ = dict_.setdefault(key, [])
    if not isinstance(item, list):
        item = [item]
    for mergeable in item:
        if mergeable not in item:
            list_.append(mergeable)
