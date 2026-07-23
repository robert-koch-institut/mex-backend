from collections import deque
from typing import TYPE_CHECKING, Any

from mex.backend.graph.connector import GraphConnector
from mex.backend.graph.exceptions import InconsistentGraphError
from mex.common.logging import logger
from mex.common.models import (
    RULE_MODEL_CLASSES_BY_NAME,
    RULE_MODEL_CLASSES_BY_TYPE_BY_NAME,
    RULE_SET_RESPONSE_CLASSES_BY_NAME,
    AnyRuleModel,
    AnyRuleSetResponse,
)
from mex.common.transform import ensure_postfix

if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Generator, Sequence


def add_workflow_rule_to_all_rule_sets() -> None:
    """Add workflow rule to all rule sets in database."""
    logger.info("migration add_workflow_rule_to_all_rule_sets started")
    connector = GraphConnector.get()

    number_of_rules_after_migration = 4

    for index, raw_rules in enumerate(get_all_raw_rules(connector), start=1):
        if len(raw_rules) == number_of_rules_after_migration:
            continue
        rule_set = transform_three_raw_rules_to_rule_set_response(raw_rules)

        deque(connector.ingest_items([rule_set]))
        logger.info(
            "ingested %s: %s:%s", index, rule_set.entityType, rule_set.stableTargetId
        )
    logger.info("migration add_workflow_rule_to_all_rule_sets complete")


def get_all_raw_rules(connector: GraphConnector) -> Generator[list[dict[str, Any]]]:
    """Get all raw rules in database."""
    graph_result = connector.fetch_rule_items(
        query_string=None,
        identifier=None,
        entity_type=None,
        reference_filters=None,
        skip=0,
        limit=4000,
    )
    items = graph_result.one()["items"]
    rule_ids = sorted({item["stableTargetId"][0] for item in items})

    logger.info("found %s rule_sets", len(rule_ids))

    for rule_id in rule_ids:
        result = connector.fetch_merged_items(
            query_string=None,
            identifier=rule_id,
            entity_type=None,
            reference_filters=None,
            skip=0,
            limit=100,
        )
        if len(result["items"]) != 1:
            raise RuntimeError
        for item in result["items"]:
            raw_rules = [
                component
                for component in item["_components"]
                if component["entityType"] in RULE_MODEL_CLASSES_BY_NAME
            ]
            if raw_rules:
                yield raw_rules


def transform_three_raw_rules_to_rule_set_response(
    raw_rules: Sequence[dict[str, Any]],
) -> AnyRuleSetResponse:
    """Transform a set of plain rules into a rule set response."""
    stem_types: list[str] = []
    stable_target_ids: list[str] = []
    response: dict[str, Any] = {}
    model: type[AnyRuleModel] | None

    number_of_rules_before_migration = 3

    if (num_raw_rules := len(raw_rules)) != number_of_rules_before_migration:
        msg = f"inconsistent number of rules found: {num_raw_rules}, expected 3."
        raise InconsistentGraphError(msg)

    for rule in raw_rules:
        for (
            field_name,
            model_class_lookup,
        ) in RULE_MODEL_CLASSES_BY_TYPE_BY_NAME.items():
            if model := model_class_lookup.get(str(rule.get("entityType"))):
                response[field_name] = rule
                stem_types.append(model.stemType)
                stable_target_ids.extend(rule.pop("stableTargetId", []))

    if len(set(stem_types)) != 1:
        msg = "inconsistent rule item stem types"
        raise InconsistentGraphError(msg)
    if len(set(stable_target_ids)) != 1:
        msg = f"inconsistent rule item stableTargetIds: {', '.join(stable_target_ids)}"
        raise InconsistentGraphError(msg)

    response["stableTargetId"] = stable_target_ids[0]
    response_class_name = ensure_postfix(stem_types[0], "RuleSetResponse")
    response_class = RULE_SET_RESPONSE_CLASSES_BY_NAME[response_class_name]
    return response_class.model_validate(response)  # here an empty workflow rule is set


if __name__ == "__main__":
    add_workflow_rule_to_all_rule_sets()
