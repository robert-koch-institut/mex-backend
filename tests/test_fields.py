from typing import Any

import pytest
from pydantic import BaseModel

from mex.backend.fields import _get_inner_types, _has_exact_type, _has_subclass_type
from mex.common.types import Identifier, OrganizationalUnitID, PersonID, Text


@pytest.mark.parametrize(
    ("annotation", "expected_types"),
    (
        (str, [str]),
        (str | None, [str, type(None)]),
        (list[str | int | list[str]], [str, int, str]),
        (None, [type(None)]),
    ),
)
def test_get_inner_types(annotation: Any, expected_types: list[type]) -> None:
    assert list(_get_inner_types(annotation)) == expected_types


@pytest.mark.parametrize(
    ("annotation", "expected"),
    (
        (str, False),
        (str | None, False),
        (list[str | int | list[str]], False),
        (None, False),
        (Identifier, True),
        (PersonID, True),
        (list[PersonID], True),
        (str | PersonID, True),
        (list[None | OrganizationalUnitID], True),
        (list[None | list[OrganizationalUnitID]], True),
    ),
)
def test_has_exact_type(annotation: Any, expected: bool) -> None:
    class DummyModel(BaseModel):
        attribute: annotation

    assert (
        _has_subclass_type(DummyModel.model_fields["attribute"], Identifier) == expected
    )


@pytest.mark.parametrize(
    ("annotation", "expected"),
    (
        (str, False),
        (str | None, False),
        (list[str | int | list[str]], False),
        (None, False),
        (Identifier, False),
        (Text, True),
        (list[Text], True),
        (str | Text, True),
        (list[None | Text], True),
        (list[None | list[Text]], True),
    ),
)
def test_has_subclass_type(annotation: Any, expected: bool) -> None:
    class DummyModel(BaseModel):
        attribute: annotation

    assert _has_exact_type(DummyModel.model_fields["attribute"], Text) == expected
