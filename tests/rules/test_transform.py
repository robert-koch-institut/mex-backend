from typing import Any

import pytest

from mex.backend.rules.transform import (
    build_tombstone_rule_set,
    transform_raw_rule_set_to_rule_set_response,
    transform_raw_rules_to_rule_set_response,
)
from mex.common.models import (
    MergedContactPoint,
    MergedOrganizationalUnit,
)
from mex.common.types import (
    MergedContactPointIdentifier,
    MergedOrganizationalUnitIdentifier,
    TextLanguage,
)


@pytest.mark.parametrize(
    ("items", "expected"),
    [
        pytest.param(
            [],
            "inconsistent number of rules found: 0",
            id="wrong-item-count",
        ),
        pytest.param(
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
                {
                    "entityType": "WorkflowResource",
                    "stableTargetId": ["00000000000005"],
                },
            ],
            "inconsistent rule item stem types",
            id="incompatible-types",
        ),
        pytest.param(
            [
                {
                    "entityType": "AdditiveVariable",
                    "stableTargetId": ["00000000000001"],
                },
                {
                    "entityType": "SubtractiveVariable",
                    "stableTargetId": ["00000000000002"],
                },
                {
                    "entityType": "PreventiveVariable",
                    "stableTargetId": ["00000000000003"],
                },
                {
                    "entityType": "WorkflowVariable",
                    "stableTargetId": ["00000000000004"],
                },
            ],
            "inconsistent rule item stableTargetIds: "
            "00000000000001, 00000000000002, 00000000000003, 00000000000004",
            id="incompatible-ids",
        ),
        pytest.param(
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
                {
                    "entityType": "WorkflowVariableGroup",
                    "stableTargetId": ["00000000000007"],
                    "forbiddenPublishingTarget": [],
                },
            ],
            {
                "additive": {
                    "containedBy": [],
                    "label": [{"value": "group one", "language": TextLanguage.EN}],
                    "entityType": "AdditiveVariableGroup",
                    "supersededBy": None,
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
                "workflow": {
                    "entityType": "WorkflowVariableGroup",
                    "forbiddenPublishingTarget": [],
                },
                "entityType": "VariableGroupRuleSetResponse",
                "stableTargetId": "00000000000007",
            },
            id="valid-rule-items",
        ),
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
    ("raw", "expected"),
    [
        pytest.param(
            {
                "additive": {"entityType": "AdditiveVariableGroup"},
                "subtractive": None,
                "preventive": None,
                "workflow": None,
                "stableTargetId": "00000000000007",
            },
            "inconsistent rule set, only found: additive",
            id="missing-rules",
        ),
        pytest.param(
            {
                "additive": {"entityType": "AdditiveAccessPlatform"},
                "subtractive": {"entityType": "SubtractiveActivity"},
                "preventive": {"entityType": "PreventivePerson"},
                "workflow": {"entityType": "WorkflowResource"},
                "stableTargetId": "00000000000005",
            },
            "inconsistent rule item stem types",
            id="incompatible-types",
        ),
        pytest.param(
            {
                "additive": {
                    "entityType": "AdditiveVariableGroup",
                    "label": [{"value": "group one"}],
                },
                "subtractive": {
                    "entityType": "SubtractiveVariableGroup",
                    "label": [{"value": "group two"}],
                },
                "preventive": {
                    "entityType": "PreventiveVariableGroup",
                    "containedBy": ["00000000000042"],
                },
                "workflow": {
                    "entityType": "WorkflowVariableGroup",
                    "forbiddenPublishingTarget": [],
                },
                "stableTargetId": "00000000000007",
            },
            {
                "additive": {
                    "containedBy": [],
                    "label": [{"value": "group one", "language": TextLanguage.EN}],
                    "entityType": "AdditiveVariableGroup",
                    "supersededBy": None,
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
                "workflow": {
                    "entityType": "WorkflowVariableGroup",
                    "forbiddenPublishingTarget": [],
                },
                "entityType": "VariableGroupRuleSetResponse",
                "stableTargetId": "00000000000007",
            },
            id="valid-rule-set",
        ),
    ],
)
def test_transform_raw_rule_set_to_rule_set_response(
    raw: dict[str, Any],
    expected: str | dict[str, Any],
) -> None:
    try:
        rule_set = transform_raw_rule_set_to_rule_set_response(raw)
    except Exception as error:
        if str(expected) not in str(error):
            raise AssertionError(expected) from error
    else:
        assert rule_set.model_dump() == expected


GONER = MergedOrganizationalUnitIdentifier("bFQoRhcVH5DHUA")
KEEPER = MergedOrganizationalUnitIdentifier("bFQoRhcVH5DHUB")


def test_build_tombstone_rule_set() -> None:
    goner = MergedOrganizationalUnit(identifier=GONER, name=[{"value": "Unit 1"}])
    keeper = MergedOrganizationalUnit(identifier=KEEPER, name=[{"value": "Unit 2"}])

    rule_set = build_tombstone_rule_set(goner, keeper)

    assert rule_set.entityType == "OrganizationalUnitRuleSetResponse"
    assert rule_set.stableTargetId == GONER
    assert rule_set.additive.supersededBy == KEEPER
    # nothing but supersededBy survives, that is what makes it a tombstone
    assert rule_set.additive.model_dump(exclude_defaults=True) == {
        "supersededBy": KEEPER
    }
    assert rule_set.subtractive.model_dump(exclude_defaults=True) == {}
    assert rule_set.preventive.model_dump(exclude_defaults=True) == {}
    assert rule_set.workflow.model_dump(exclude_defaults=True) == {}


def test_build_tombstone_rule_set_picks_the_matching_stem_type() -> None:
    goner = MergedContactPoint(
        identifier=MergedContactPointIdentifier("bFQoRhcVH5DHUC"),
        email=["one@contact-point.tld"],
    )
    keeper = MergedContactPoint(
        identifier=MergedContactPointIdentifier("bFQoRhcVH5DHUD"),
        email=["two@contact-point.tld"],
    )

    rule_set = build_tombstone_rule_set(goner, keeper)

    assert rule_set.entityType == "ContactPointRuleSetResponse"
    assert rule_set.additive.entityType == "AdditiveContactPoint"
    assert rule_set.additive.supersededBy == keeper.identifier
