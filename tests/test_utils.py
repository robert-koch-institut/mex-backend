import pytest

from mex.backend.utils import extend_list_in_dict, prune_list_in_dict, reraising


def test_extend_list_in_dict() -> None:
    dict_ = {"existing-key": ["already-here"]}

    extend_list_in_dict(dict_, "new-key", "single-value")
    assert dict_ == {
        "existing-key": ["already-here"],
        "new-key": ["single-value"],
    }

    extend_list_in_dict(dict_, "existing-key", "another-value")
    assert dict_ == {
        "existing-key": ["already-here", "another-value"],
        "new-key": ["single-value"],
    }


def test_prune_list_in_dict() -> None:
    dict_ = {
        "existing-key": ["leave-me-alone"],
        "prune-me": ["42", "foo"],
    }

    prune_list_in_dict(dict_, "prune-me", "foo")
    assert dict_ == {
        "existing-key": ["leave-me-alone"],
        "prune-me": ["42"],
    }

    prune_list_in_dict(dict_, "does-not-exist", "42")
    assert dict_ == {
        "existing-key": ["leave-me-alone"],
        "prune-me": ["42"],
        "does-not-exist": [],
    }


def test_reraising_no_exception() -> None:
    def add(a: int, b: int) -> int:
        return a + b

    result = reraising(ValueError, RuntimeError, add, 1, 2)
    assert result == 1 + 2


def test_reraising_with_caught_exception() -> None:
    class CustomDivisionError(Exception):
        pass

    def divide(a: int, b: int) -> float:
        return a / b

    with pytest.raises(CustomDivisionError) as exc_info:
        reraising(ZeroDivisionError, CustomDivisionError, divide, 1, 0)

    assert isinstance(exc_info.value, CustomDivisionError)
    assert exc_info.value.__cause__ is not None
    assert isinstance(exc_info.value.__cause__, ZeroDivisionError)


def test_reraising_propagates_other_exceptions() -> None:
    def raise_type_error() -> None:
        msg = "This is a TypeError"
        raise TypeError(msg)

    with pytest.raises(TypeError) as exc_info:
        reraising(ValueError, RuntimeError, raise_type_error)

    assert isinstance(exc_info.value, TypeError)
