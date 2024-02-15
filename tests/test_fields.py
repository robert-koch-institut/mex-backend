from typing import Any

import pytest
from pydantic import BaseModel

from mex.backend.fields import (
    _get_inner_types,
    _has_exact_type,
    _has_true_subclass_type,
)
from mex.common.types import Identifier, PersonID, Text


@pytest.mark.parametrize(
    ("annotation", "expected_types"),
    (
        (str, [str]),
        (str | None, [str]),
        (list[str | int | list[str]], [str, int, str]),
        (None, []),
    ),
)
def test_get_inner_types(annotation: Any, expected_types: list[type]) -> None:
    assert list(_get_inner_types(annotation)) == expected_types


@pytest.mark.parametrize(
    ("annotation", "expected"),
    (
        (None, False),
        (str, False),
        (str | None, False),
        (Text, False),
        (Identifier, False),
        (PersonID, True),
        (Identifier | None, False),
        (PersonID | None, True),
        (list[PersonID], True),
        (list[None | Identifier], False),
        (list[None | PersonID], True),
    ),
)
def test_has_true_subclass_type(annotation: Any, expected: bool) -> None:
    class DummyModel(BaseModel):
        attribute: annotation

    assert (
        _has_true_subclass_type(DummyModel.model_fields["attribute"], Identifier)
        == expected
    )


@pytest.mark.parametrize(
    ("annotation", "expected"),
    (
        (str, False),
        (str | None, False),
        (list[str | int | list[str]], False),
        (None, False),
        (Identifier, True),
        (PersonID, False),
        (list[PersonID], False),
        (str | PersonID, False),
        (list[None | Identifier], True),
        (list[None | list[Identifier]], True),
    ),
)
def test_has_exact_type(annotation: Any, expected: bool) -> None:
    class DummyModel(BaseModel):
        attribute: annotation

    assert _has_exact_type(DummyModel.model_fields["attribute"], Identifier) == expected
