from typing import Annotated, Any

import pytest
from pydantic import BaseModel

from mex.backend.fields import (
    _contains_only_types,
    _get_inner_types,
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
        (Annotated[list[str | int], "some-annotation"], [str, int]),
        (None, []),
    ),
    ids=[
        "simple annotation",
        "optional type",
        "type union",
        "complex nested types",
        "annotated list",
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
def test_contains_only_types(
    annotation: Any, types: list[type], expected: bool
) -> None:
    class DummyModel(BaseModel):
        attribute: annotation

    assert (
        _contains_only_types(DummyModel.model_fields["attribute"], *types) == expected
    )
