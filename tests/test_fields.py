from typing import Any

import pytest
from pydantic import BaseModel

from mex.backend.fields import (
    _get_inner_types,
    _has_any_type,
)
from mex.common.types import (
    MERGED_IDENTIFIER_CLASSES,
    Identifier,
    MergedPersonIdentifier,
)


@pytest.mark.parametrize(
    ("annotation", "expected_types"),
    (
        (str, [str]),
        (str | None, [str]),
        (str | int, [str, int]),
        (list[str | int | list[str]], [str, int, str]),
        (None, []),
    ),
    ids=[
        "simple annotation",
        "optional type",
        "type union",
        "complex nested types",
        "static None",
    ],
)
def test_get_inner_types(annotation: Any, expected_types: list[type]) -> None:
    assert list(_get_inner_types(annotation)) == expected_types


@pytest.mark.parametrize(
    ("annotation", "types", "expected"),
    (
        (None, [str], False),
        (str, [str], True),
        (str, [Identifier], False),
        (Identifier, [str], False),
        (list[str | int | list[str]], [str, float], False),
        (list[str | int | list[str]], [int, str], True),
        (MergedPersonIdentifier | None, MERGED_IDENTIFIER_CLASSES, True),
    ),
    ids=[
        "static None",
        "simple str",
        "str vs identifier",
        "identifier vs str",
        "complex miss",
        "complex hit",
        "optional identifier",
    ],
)
def test_has_any_type(annotation: Any, types: list[type], expected: bool) -> None:
    class DummyModel(BaseModel):
        attribute: annotation

    assert _has_any_type(DummyModel.model_fields["attribute"], *types) == expected
