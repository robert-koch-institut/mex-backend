from typing import Any

import pytest

from mex.backend.rules.transform import transform_raw_rules_to_rule_set_response
from mex.common.types import TextLanguage


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
