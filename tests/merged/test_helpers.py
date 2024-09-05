from typing import Any

from mex.backend.merged.helpers import (
    _apply_additive_rule,
    _apply_subtractive_rule,
    _merge_extracted_items_and_apply_preventive_rule,
)
from mex.common.models import (
    AdditiveOrganizationalUnit,
    AnyExtractedModel,
    ExtractedContactPoint,
    PreventiveContactPoint,
    SubtractiveOrganizationalUnit,
)
from mex.common.types import Identifier, Text, TextLanguage


def test_apply_preventive_rule() -> None:
    merged_dict: dict[str, Any] = {}
    contact_points: list[AnyExtractedModel] = [
        ExtractedContactPoint(
            email=["info@contact-point.one"],
            hadPrimarySource=Identifier.generate(seed=1),
            identifierInPrimarySource="one",
        ),
        ExtractedContactPoint(
            email=["hello@contact-point.two"],
            hadPrimarySource=Identifier.generate(seed=2),
            identifierInPrimarySource="two",
        ),
    ]
    rule = PreventiveContactPoint(
        email=[contact_points[1].hadPrimarySource],
    )
    _merge_extracted_items_and_apply_preventive_rule(
        merged_dict,
        ["email"],
        contact_points,
        rule,
    )
    assert merged_dict == {
        "email": ["info@contact-point.one"],
    }

    merged_dict.clear()
    _merge_extracted_items_and_apply_preventive_rule(
        merged_dict,
        ["email"],
        contact_points,
        None,
    )
    assert merged_dict == {
        "email": ["info@contact-point.one", "hello@contact-point.two"],
    }


def test_apply_additive_rule() -> None:
    merged_dict: dict[str, Any] = {
        "email": ["info@org-unit.one"],
    }
    rule = AdditiveOrganizationalUnit(
        email=["new-mail@who.dis", "info@org-unit.one"],
        name=[Text(value="org unit one", language=TextLanguage.EN)],
    )
    _apply_additive_rule(
        merged_dict,
        ["email", "name"],
        rule,
    )
    assert merged_dict == {
        "email": ["info@org-unit.one", "new-mail@who.dis"],
        "name": [Text(value="org unit one", language=TextLanguage.EN)],
    }


def test_apply_subtractive_rule() -> None:
    merged_dict: dict[str, Any] = {
        "email": ["info@org-unit.one"],
        "name": [Text(value="org unit one", language=TextLanguage.EN)],
    }
    rule = SubtractiveOrganizationalUnit(
        email=["unknown@email.why", "info@org-unit.one"],
        name=[Text(value="org unit one", language=TextLanguage.DE)],
    )
    _apply_subtractive_rule(
        merged_dict,
        ["email", "name"],
        rule,
    )
    assert merged_dict == {
        "email": [],
        "name": [Text(value="org unit one", language=TextLanguage.EN)],
    }
