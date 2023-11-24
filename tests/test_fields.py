from typing import Any

import pytest
from pydantic import BaseModel

from mex.backend.fields import _get_inner_types, is_reference_field, is_text_field
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
def test__get_inner_types(annotation: Any, expected_types: list[type]) -> None:
    assert list(_get_inner_types(annotation)) == expected_types


@pytest.mark.parametrize(
    ("annotation", "is_reference"),
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
def test_is_reference_field(annotation: Any, is_reference: bool) -> None:
    class DummyModel(BaseModel):
        attribute: annotation

    assert is_reference_field(DummyModel.model_fields["attribute"]) == is_reference


@pytest.mark.parametrize(
    ("annotation", "is_text"),
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
def test_is_text_field(annotation: Any, is_text: bool) -> None:
    class DummyModel(BaseModel):
        attribute: annotation

    assert is_text_field(DummyModel.model_fields["attribute"]) == is_text
