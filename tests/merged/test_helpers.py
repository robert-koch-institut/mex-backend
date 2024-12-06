from typing import Any, Literal

import pytest

from mex.backend.merged.helpers import (
    _apply_additive_rule,
    _apply_subtractive_rule,
    _merge_extracted_items_and_apply_preventive_rule,
    create_merged_item,
    search_merged_items_in_graph,
)
from mex.common.models import (
    ActivityRuleSetRequest,
    AdditiveOrganizationalUnit,
    AdditivePerson,
    AnyExtractedModel,
    AnyRuleSetRequest,
    ExtractedActivity,
    ExtractedContactPoint,
    ExtractedPerson,
    PersonRuleSetRequest,
    PreventiveContactPoint,
    PreventivePerson,
    SubtractiveActivity,
    SubtractiveOrganizationalUnit,
    SubtractivePerson,
)
from mex.common.types import Identifier, Text, TextLanguage
from tests.conftest import MockedGraph


def test_merge_extracted_items_and_apply_preventive_rule() -> None:
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

    merged_dict.clear()
    _merge_extracted_items_and_apply_preventive_rule(
        merged_dict,
        ["email"],
        [],
        rule,
    )
    assert merged_dict == {}


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


@pytest.mark.parametrize(
    ("extracted_items", "rule_set", "validate_cardinality", "expected"),
    [
        (
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
                    memberOf=[
                        Identifier.generate(seed=500),
                        Identifier.generate(seed=750),
                    ],
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
            True,
            {
                "affiliation": [
                    Identifier.generate(seed=99),
                    Identifier.generate(seed=101),
                ],
                "fullName": ["Mr. Krabs"],
                "givenName": ["Eugene", "Harold"],
                "memberOf": [
                    Identifier.generate(seed=500),
                    Identifier.generate(seed=750),
                ],
                "identifier": Identifier.generate(seed=42),
            },
        ),
        (
            [],
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
            True,
            {
                "givenName": ["Eugene", "Harold"],
                "memberOf": [
                    Identifier.generate(seed=500),
                ],
                "identifier": Identifier.generate(seed=42),
            },
        ),
        (
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
                    memberOf=[
                        Identifier.generate(seed=500),
                        Identifier.generate(seed=750),
                    ],
                    hadPrimarySource=Identifier.generate(seed=11),
                    identifierInPrimarySource="mrk",
                ),
            ],
            None,
            True,
            {
                "affiliation": [
                    Identifier.generate(seed=99),
                    Identifier.generate(seed=101),
                ],
                "email": ["z@express.planet", "manager@krusty.ocean"],
                "fullName": ["Dr. Zoidberg", "Mr. Krabs"],
                "memberOf": [
                    Identifier.generate(seed=500),
                    Identifier.generate(seed=750),
                ],
                "identifier": Identifier.generate(seed=42),
            },
        ),
        ([], None, True, "One of rule_set or extracted_items is required."),
        (
            [
                ExtractedActivity(
                    title=Text(value="Burger flipping"),
                    contact=Identifier.generate(seed=97),
                    responsibleUnit=Identifier.generate(seed=98),
                    identifierInPrimarySource="BF",
                    hadPrimarySource=Identifier.generate(seed=99),
                ),
            ],
            ActivityRuleSetRequest(
                subtractive=SubtractiveActivity(title=Text(value="Burger flipping")),
            ),
            False,
            {
                "contact": [Identifier.generate(seed=97)],
                "identifier": Identifier.generate(seed=42),
                "responsibleUnit": [Identifier.generate(seed=98)],
            },
        ),
    ],
    ids=(
        "extracted items and rule set",
        "only rule set",
        "only extracted items",
        "error if neither is supplied",
        "get preview of merged items",
    ),
)
def test_create_merged_item(
    extracted_items: list[AnyExtractedModel],
    rule_set: AnyRuleSetRequest | None,
    validate_cardinality: Literal[True, False],
    expected: Any,
) -> None:
    try:
        merged_item = create_merged_item(
            Identifier.generate(seed=42),
            extracted_items,
            rule_set,
            validate_cardinality,
        )
    except Exception as error:
        if str(expected) not in str(error):
            raise
    else:
        assert merged_item.model_dump(exclude_defaults=True) == expected


@pytest.mark.usefixtures("load_dummy_data", "load_dummy_rule_set")
@pytest.mark.integration
def test_search_merged_items_in_graph() -> None:
    merged_result = search_merged_items_in_graph(stable_target_id="bFQoRhcVH5DHUB")
    assert merged_result.model_dump(exclude_defaults=True) == {
        "items": [
            {
                "identifier": "bFQoRhcVH5DHUB",
                "name": [
                    {"language": "en", "value": "Unit 1.6"},
                    {"language": "en", "value": "Unit 1.7"},
                ],
                "parentUnit": "bFQoRhcVH5DHUv",
                "website": [{"title": "Unit Homepage", "url": "https://unit-1-7"}],
            },
        ],
        "total": 1,
    }


@pytest.mark.parametrize(
    ("mocked_graph_result", "expected"),
    [
        (
            [
                {
                    "items": [
                        {
                            "components": [
                                {
                                    "identifier": "jbZ5Br9Vninm08ptYZFxW",
                                    "identifierInPrimarySource": "unit-1",
                                    "stableTargetId": "e5rfAc2p5zV39WUVZeAR1",
                                    "email": ["test@foo.bar"],
                                    "entityType": "ExtractedOrganizationalUnit",
                                    "_refs": [
                                        {
                                            "label": "hadPrimarySource",
                                            "position": 0,
                                            "value": "2222222222222222",
                                        },
                                        {
                                            "label": "name",
                                            "position": 0,
                                            "value": {
                                                "value": "Eine unit von einer Org.",
                                                "language": "de",
                                            },
                                        },
                                    ],
                                }
                            ],
                            "entityType": "MergedOrganizationalUnit",
                            "identifier": "e5rfAc2p5zV39WUVZeAR1",
                        }
                    ],
                    "total": 1,
                }
            ],
            {
                "items": [
                    {
                        "email": ["test@foo.bar"],
                        "identifier": "e5rfAc2p5zV39WUVZeAR1",
                        "name": [
                            {"language": "de", "value": "Eine unit von einer Org."},
                        ],
                    }
                ],
                "total": 1,
            },
        ),
        (
            [
                {
                    "items": [
                        {
                            "components": [
                                {
                                    "identifier": "jbZ5Br9Vninm08ptYZFxW",
                                    "identifierInPrimarySource": "unit-1",
                                    "stableTargetId": "e5rfAc2p5zV39WUVZeAR1",
                                    "email": ["test@foo.bar"],
                                    "entityType": "ExtractedOrganizationalUnit",
                                    "_refs": [
                                        {
                                            "label": "hadPrimarySource",
                                            "position": 0,
                                            "value": "2222222222222222",
                                        },
                                        {
                                            "label": "name",
                                            "position": 0,
                                            "value": {
                                                "value": "Eine unit von einer Org.",
                                                "language": "de",
                                            },
                                        },
                                    ],
                                },
                                {
                                    "stableTargetId": "e5rfAc2p5zV39WUVZeAR1",
                                    "email": ["bar@foo.bar"],
                                    "entityType": "AdditiveOrganizationalUnit",
                                    "_refs": [],
                                },
                            ],
                            "entityType": "MergedOrganizationalUnit",
                            "identifier": "e5rfAc2p5zV39WUVZeAR1",
                        }
                    ],
                    "total": 1,
                }
            ],
            "inconsistent number of rules found: 1",
        ),
        (
            [
                {
                    "items": [
                        {
                            "components": [
                                {
                                    "identifier": "jbZ5Br9Vninm08ptYZFxW",
                                    "identifierInPrimarySource": "unit-1",
                                    "email": ["test@foo.bar"],
                                    "entityType": "ExtractedOrganizationalUnit",
                                    "_refs": [
                                        {
                                            "label": "hadPrimarySource",
                                            "position": 0,
                                            "value": "2222222222222222",
                                        },
                                        {
                                            "label": "name",
                                            "position": 0,
                                            "value": {
                                                "value": "Eine unit von einer Org.",
                                                "language": "de",
                                            },
                                        },
                                        {
                                            "label": "stableTargetId",
                                            "position": 0,
                                            "value": "e5rfAc2p5zV39WUVZeAR1",
                                        },
                                    ],
                                },
                                {
                                    "email": ["bar@foo.bar"],
                                    "entityType": "AdditiveOrganizationalUnit",
                                    "_refs": [
                                        {
                                            "label": "stableTargetId",
                                            "position": 0,
                                            "value": "e5rfAc2p5zV39WUVZeAR1",
                                        },
                                    ],
                                },
                                {
                                    "email": ["test@foo.bar"],
                                    "entityType": "SubtractiveOrganizationalUnit",
                                    "_refs": [
                                        {
                                            "label": "stableTargetId",
                                            "position": 0,
                                            "value": "e5rfAc2p5zV39WUVZeAR1",
                                        },
                                    ],
                                },
                                {
                                    "entityType": "PreventiveOrganizationalUnit",
                                    "_refs": [
                                        {
                                            "label": "stableTargetId",
                                            "position": 0,
                                            "value": "e5rfAc2p5zV39WUVZeAR1",
                                        },
                                    ],
                                },
                            ],
                            "entityType": "MergedOrganizationalUnit",
                            "identifier": "e5rfAc2p5zV39WUVZeAR1",
                        }
                    ],
                    "total": 1,
                }
            ],
            {
                "items": [
                    {
                        "email": ["bar@foo.bar"],
                        "identifier": "e5rfAc2p5zV39WUVZeAR1",
                        "name": [
                            {"language": "de", "value": "Eine unit von einer Org."},
                        ],
                    }
                ],
                "total": 1,
            },
        ),
    ],
    ids=["no_rules", "one_rule_raises_error", "three_rules"],
)
def test_search_merged_items_in_graph_mocked(
    mocked_graph_result: list[dict[str, Any]], expected: Any, mocked_graph: MockedGraph
) -> None:
    mocked_graph.return_value = mocked_graph_result

    try:
        merged_result = search_merged_items_in_graph(stable_target_id="bFQoRhcVH5DHUB")
    except Exception as error:
        if str(expected) not in str(error):
            raise
    else:
        assert merged_result.model_dump(exclude_defaults=True) == expected
