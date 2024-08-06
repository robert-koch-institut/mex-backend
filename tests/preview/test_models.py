import pytest
from pydantic import ValidationError

from mex.backend.preview.models import RuleSet


def test_rule_set_validation() -> None:
    with pytest.raises(ValidationError, match="All rules must have same stemType"):
        RuleSet.model_validate(
            {
                "additive": {"$type": "AdditivePerson"},
                "preventive": {
                    "$type": "PreventiveActivity",
                },
                "subtractive": {
                    "$type": "SubtractiveResource",
                },
            }
        )
