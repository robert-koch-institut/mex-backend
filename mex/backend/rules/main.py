from typing import Annotated, Any

from fastapi import APIRouter, Body

router = APIRouter()


@router.post("/rule-set", tags=["editor"])
def create_rule_set(rule_set: Annotated[dict[str, Any], Body()]) -> dict[str, Any]:
    """Create a new set of rules."""
    return rule_set
