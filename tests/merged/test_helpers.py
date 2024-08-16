from typing import Any

from mex.backend.merged.helpers import (
    _apply_additive_rule,
    _apply_preventive_rule,
    _apply_subtractive_rule,
    create_merged_item,
)
from mex.common.models import (
    AdditiveOrganizationalUnit,
    AdditivePerson,
    AnyExtractedModel,
    ExtractedContactPoint,
    ExtractedPerson,
    PersonRuleSetRequest,
    PreventiveContactPoint,
    PreventivePerson,
    SubtractiveOrganizationalUnit,
    SubtractivePerson,
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
    _apply_preventive_rule(
        merged_dict,
        ["email"],
        contact_points,
        rule,
    )
    assert merged_dict == {
        "email": ["info@contact-point.one"],
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


def test_create_merged_item() -> None:
    merged_person = create_merged_item(
        Identifier.generate(seed=42),
        [
            ExtractedPerson(
                fullName="Dr. Zoidberg",
                affiliation=Identifier.generate(seed=99),
                email="z@express.planet",
                identifierInPrimarySource="drz",
                hadPrimarySource=Identifier.generate(seed=9),
            ),
            ExtractedPerson(
                fullName="Mr. Krabs",
                email="manager@krusty.ocean",
                affiliation=Identifier.generate(seed=101),
                memberOf=[Identifier.generate(seed=500), Identifier.generate(seed=750)],
                hadPrimarySource=Identifier.generate(seed=11),
                identifierInPrimarySource="mrk",
            ),
        ],
        PersonRuleSetRequest(
            additive=AdditivePerson(
                givenName=["Eugene", "Harold", "John"],
                memberOf=[Identifier.generate(seed=500)],
            ),
            subtractive=SubtractivePerson(
                email=["manager@krusty.ocean"],
                givenName=["John"],
            ),
            preventive=PreventivePerson(
                email=[Identifier.generate(seed=9)],
                fullName=[Identifier.generate(seed=9)],
            ),
        ),
    )
    assert merged_person.model_dump(exclude_defaults=True) == {
        "affiliation": [Identifier.generate(seed=99), Identifier.generate(seed=101)],
        "fullName": ["Mr. Krabs"],
        "givenName": ["Eugene", "Harold"],
        "memberOf": [Identifier.generate(seed=500), Identifier.generate(seed=750)],
        "identifier": Identifier.generate(seed=42),
    }
