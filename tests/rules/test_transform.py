from collections.abc import Callable
from typing import Any

import pytest

from mex.backend.rules.transform import (
    merge_rule_sets,
    merge_rules,
    remove_primary_source_from_rule,
    transform_raw_rules_to_rule_set_response,
)
from mex.common.models import (
    AdditivePerson,
    AnyAdditiveModel,
    AnyPreventiveModel,
    AnySubtractiveModel,
    PersonRuleSetResponse,
    PreventivePerson,
    SubtractivePerson,
)
from mex.common.types import (
    AnyPrimitiveType,
    Identifier,
    MergedPrimarySourceIdentifier,
    TextLanguage,
)


@pytest.mark.parametrize(
    ("items", "expected"),
    [
        ([], "inconsistent number of rules found: 0"),
        (
            [
                {
                    "entityType": "AdditiveAccessPlatform",
                    "stableTargetId": ["00000000000005"],
                },
                {
                    "entityType": "SubtractiveActivity",
                    "stableTargetId": ["00000000000005"],
                },
                {
                    "entityType": "PreventivePerson",
                    "stableTargetId": ["00000000000005"],
                },
            ],
            "inconsistent rule item stem types",
        ),
        (
            [
                {
                    "entityType": "PreventiveVariable",
                    "stableTargetId": ["00000000000001"],
                },
                {
                    "entityType": "PreventiveVariable",
                    "stableTargetId": ["00000000000002"],
                },
                {
                    "entityType": "PreventiveVariable",
                    "stableTargetId": ["00000000000003"],
                },
            ],
            "inconsistent rule item stableTargetIds: "
            "00000000000001, 00000000000002, 00000000000003",
        ),
        (
            [
                {
                    "entityType": "AdditiveVariableGroup",
                    "stableTargetId": ["00000000000007"],
                    "label": [{"value": "group one"}],
                },
                {
                    "entityType": "SubtractiveVariableGroup",
                    "stableTargetId": ["00000000000007"],
                    "label": [{"value": "group two"}],
                },
                {
                    "entityType": "PreventiveVariableGroup",
                    "stableTargetId": ["00000000000007"],
                    "containedBy": ["00000000000042"],
                },
            ],
            {
                "additive": {
                    "containedBy": [],
                    "label": [{"value": "group one", "language": TextLanguage.EN}],
                    "entityType": "AdditiveVariableGroup",
                },
                "subtractive": {
                    "containedBy": [],
                    "label": [{"value": "group two", "language": TextLanguage.EN}],
                    "entityType": "SubtractiveVariableGroup",
                },
                "preventive": {
                    "entityType": "PreventiveVariableGroup",
                    "containedBy": ["00000000000042"],
                    "label": [],
                },
                "entityType": "VariableGroupRuleSetResponse",
                "stableTargetId": "00000000000007",
            },
        ),
    ],
    ids=[
        "wrong item count",
        "incompatible types",
        "incompatible stableTargetIds",
        "valid rule items",
    ],
)
def test_transform_raw_rules_to_rule_set_response(
    items: list[Any],
    expected: str | dict[str, Any],
) -> None:
    try:
        rule_set = transform_raw_rules_to_rule_set_response(items)
    except Exception as error:
        if str(expected) not in str(error):
            raise AssertionError(expected) from error
    else:
        assert rule_set.model_dump() == expected


@pytest.mark.parametrize(
    ("source_rule", "target_rule", "expected_target", "value_filter"),
    [
        # Additive rules
        (
            AdditivePerson(
                givenName=["Alice", "Alicia"],
                familyName=["Smith"],
                email=["alice@example.com"],
            ),
            AdditivePerson(
                givenName=["Bob"],
                familyName=["Jones"],
                email=[],
            ),
            {
                "email": ["alice@example.com"],
                "familyName": ["Jones", "Smith"],
                "givenName": ["Bob", "Alice", "Alicia"],
            },
            bool,
        ),
        # Subtractive rules
        (
            SubtractivePerson(
                givenName=["Alice"],
                familyName=["Smith", "Jones"],
                email=["old@example.com"],
            ),
            SubtractivePerson(
                givenName=["Bob"],
                familyName=[],
                email=[],
            ),
            {
                "email": ["old@example.com"],
                "familyName": ["Smith", "Jones"],
                "givenName": ["Bob", "Alice"],
            },
            bool,
        ),
        # Additive with filter
        (
            AdditivePerson(
                givenName=["Alice", "Valid"],
                familyName=["Smith", "Valid"],
                email=["alice@example.com", "valid@test.com"],
            ),
            AdditivePerson(
                givenName=["Bob"],
                familyName=[],
                email=["valid@bob.com"],
            ),
            {
                "email": ["valid@bob.com", "valid@test.com"],
                "familyName": ["Valid"],
                "givenName": ["Bob", "Valid"],
            },
            lambda x: str(x).lower().startswith("valid"),
        ),
        # Empty source
        (
            AdditivePerson(
                givenName=[],
                familyName=[],
                email=[],
            ),
            AdditivePerson(
                givenName=["Bob"],
                familyName=["Jones"],
                email=["bob@example.com"],
            ),
            {
                "email": ["bob@example.com"],
                "familyName": ["Jones"],
                "givenName": ["Bob"],
            },
            bool,
        ),
        # Empty target
        (
            AdditivePerson(
                givenName=["Alice"],
                familyName=["Smith"],
                email=["alice@example.com"],
            ),
            AdditivePerson(
                givenName=[],
                familyName=[],
                email=[],
            ),
            {
                "email": ["alice@example.com"],
                "familyName": ["Smith"],
                "givenName": ["Alice"],
            },
            bool,
        ),
        # Preventive rules
        (
            PreventivePerson(
                givenName=[MergedPrimarySourceIdentifier.generate(seed=42)],
                familyName=[MergedPrimarySourceIdentifier.generate(seed=42)],
            ),
            PreventivePerson(
                givenName=[MergedPrimarySourceIdentifier.generate(seed=99)]
            ),
            {
                "familyName": ["bFQoRhcVH5DHU6"],
                "givenName": ["bFQoRhcVH5DHV1", "bFQoRhcVH5DHU6"],
            },
            bool,
        ),
    ],
    ids=[
        "additive_rules",
        "subtractive_rules",
        "additive_with_filter",
        "empty_source",
        "empty_target",
        "preventive_rules",
    ],
)
def test_merge_rules(
    source_rule: AnyAdditiveModel | AnySubtractiveModel | AnyPreventiveModel,
    target_rule: AnyAdditiveModel | AnySubtractiveModel | AnyPreventiveModel,
    expected_target: dict[str, Any],
    value_filter: Callable[[AnyPrimitiveType], bool],
) -> None:
    merge_rules(source_rule, target_rule, value_filter)

    assert target_rule.model_dump(exclude_defaults=True, mode="json") == expected_target


@pytest.mark.parametrize(
    (
        "source_rule_set",
        "target_rule_set",
        "primary_source_identifiers",
        "expected_target",
    ),
    [
        # Basic merge with no filtering
        (
            PersonRuleSetResponse(
                additive=AdditivePerson(givenName=["Alice"], familyName=["Smith"]),
                subtractive=SubtractivePerson(email=["old@example.com"]),
                preventive=PreventivePerson(
                    affiliation=[MergedPrimarySourceIdentifier.generate(seed=42)]
                ),
                stableTargetId=Identifier.generate(seed=100),
            ),
            PersonRuleSetResponse(
                additive=AdditivePerson(givenName=["Bob"]),
                subtractive=SubtractivePerson(familyName=["Jones"]),
                preventive=PreventivePerson(
                    affiliation=[MergedPrimarySourceIdentifier.generate(seed=99)]
                ),
                stableTargetId=Identifier.generate(seed=200),
            ),
            [],
            {
                "additive": {
                    "familyName": ["Smith"],
                    "givenName": ["Bob", "Alice"],
                },
                "subtractive": {
                    "email": ["old@example.com"],
                    "familyName": ["Jones"],
                },
                "preventive": {"affiliation": ["bFQoRhcVH5DHV1"]},
                "stableTargetId": "bFQoRhcVH5DHXE",
            },
        ),
        # Merge with primary source filtering
        (
            PersonRuleSetResponse(
                additive=AdditivePerson(givenName=["Charlie"]),
                subtractive=SubtractivePerson(email=["remove@example.com"]),
                preventive=PreventivePerson(
                    givenName=[MergedPrimarySourceIdentifier.generate(seed=42)],
                    familyName=[MergedPrimarySourceIdentifier.generate(seed=55)],
                ),
                stableTargetId=Identifier.generate(seed=300),
            ),
            PersonRuleSetResponse(
                additive=AdditivePerson(givenName=["David"]),
                subtractive=SubtractivePerson(familyName=["Wilson"]),
                preventive=PreventivePerson(
                    givenName=[MergedPrimarySourceIdentifier.generate(seed=99)]
                ),
                stableTargetId=Identifier.generate(seed=400),
            ),
            [MergedPrimarySourceIdentifier.generate(seed=42)],
            {
                "additive": {"givenName": ["David", "Charlie"]},
                "subtractive": {
                    "email": ["remove@example.com"],
                    "familyName": ["Wilson"],
                },
                "preventive": {
                    "givenName": ["bFQoRhcVH5DHV1", "bFQoRhcVH5DHU6"],
                },
                "stableTargetId": "bFQoRhcVH5DH0S",
            },
        ),
        # Empty source rule set
        (
            PersonRuleSetResponse(
                additive=AdditivePerson(),
                subtractive=SubtractivePerson(),
                preventive=PreventivePerson(),
                stableTargetId=Identifier.generate(seed=500),
            ),
            PersonRuleSetResponse(
                additive=AdditivePerson(givenName=["Eve"]),
                subtractive=SubtractivePerson(familyName=["Brown"]),
                preventive=PreventivePerson(
                    email=[MergedPrimarySourceIdentifier.generate(seed=77)]
                ),
                stableTargetId=Identifier.generate(seed=600),
            ),
            [],
            {
                "additive": {"givenName": ["Eve"]},
                "subtractive": {"familyName": ["Brown"]},
                "preventive": {"email": ["bFQoRhcVH5DHVF"]},
                "stableTargetId": "bFQoRhcVH5DH36",
            },
        ),
    ],
    ids=[
        "basic_merge",
        "with_primary_source_filtering",
        "empty_source",
    ],
)
def test_merge_rule_sets(
    source_rule_set: PersonRuleSetResponse,
    target_rule_set: PersonRuleSetResponse,
    primary_source_identifiers: list[MergedPrimarySourceIdentifier],
    expected_target: dict[str, AnyPrimitiveType],
) -> None:
    """Test merge_rule_sets with various scenarios."""
    merge_rule_sets(source_rule_set, target_rule_set, primary_source_identifiers)

    assert (
        target_rule_set.model_dump(exclude_defaults=True, mode="json")
        == expected_target
    )


@pytest.mark.parametrize(
    ("rule", "primary_source_identifier", "expected_result"),
    [
        # Remove identifier from single field
        (
            PreventivePerson(
                givenName=[
                    MergedPrimarySourceIdentifier.generate(seed=42),
                    MergedPrimarySourceIdentifier.generate(seed=99),
                ]
            ),
            MergedPrimarySourceIdentifier.generate(seed=42),
            {"givenName": ["bFQoRhcVH5DHV1"]},
        ),
        # Remove identifier from multiple fields
        (
            PreventivePerson(
                givenName=[
                    MergedPrimarySourceIdentifier.generate(seed=42),
                    MergedPrimarySourceIdentifier.generate(seed=99),
                ],
                familyName=[
                    MergedPrimarySourceIdentifier.generate(seed=42),
                    MergedPrimarySourceIdentifier.generate(seed=77),
                ],
                email=[MergedPrimarySourceIdentifier.generate(seed=55)],
            ),
            MergedPrimarySourceIdentifier.generate(seed=42),
            {
                "givenName": ["bFQoRhcVH5DHV1"],
                "familyName": ["bFQoRhcVH5DHVF"],
                "email": ["bFQoRhcVH5DHVj"],
            },
        ),
        # Remove identifier that appears multiple times in same field
        (
            PreventivePerson(
                givenName=[
                    MergedPrimarySourceIdentifier.generate(seed=42),
                    MergedPrimarySourceIdentifier.generate(seed=99),
                    MergedPrimarySourceIdentifier.generate(seed=42),
                ]
            ),
            MergedPrimarySourceIdentifier.generate(seed=42),
            {"givenName": ["bFQoRhcVH5DHV1"]},
        ),
        # Remove identifier that doesn't exist
        (
            PreventivePerson(
                givenName=[MergedPrimarySourceIdentifier.generate(seed=99)],
                familyName=[MergedPrimarySourceIdentifier.generate(seed=77)],
            ),
            MergedPrimarySourceIdentifier.generate(seed=42),
            {
                "givenName": ["bFQoRhcVH5DHV1"],
                "familyName": ["bFQoRhcVH5DHVF"],
            },
        ),
        # Remove from empty rule
        (
            PreventivePerson(),
            MergedPrimarySourceIdentifier.generate(seed=42),
            {},
        ),
        # Remove all identifiers from field (field becomes empty)
        (
            PreventivePerson(
                givenName=[MergedPrimarySourceIdentifier.generate(seed=42)],
                familyName=[MergedPrimarySourceIdentifier.generate(seed=99)],
            ),
            MergedPrimarySourceIdentifier.generate(seed=42),
            {"familyName": ["bFQoRhcVH5DHV1"]},
        ),
    ],
    ids=[
        "remove_from_single_field",
        "remove_from_multiple_fields",
        "remove_multiple_occurrences",
        "remove_nonexistent_identifier",
        "remove_from_empty_rule",
        "remove_all_from_field",
    ],
)
def test_remove_primary_source_from_rule(
    rule: AnyPreventiveModel,
    primary_source_identifier: MergedPrimarySourceIdentifier,
    expected_result: dict[str, Any],
) -> None:
    """Test remove_primary_source_from_rule with various scenarios."""
    remove_primary_source_from_rule(rule, primary_source_identifier)

    assert rule.model_dump(exclude_defaults=True, mode="json") == expected_result
