from collections import deque

from mex.backend.graph.connector import GraphConnector
from mex.backend.graph.exceptions import NoResultFoundError
from mex.backend.rules.transform import transform_raw_rules_to_rule_set_response
from mex.common.models import (
    RULE_SET_RESPONSE_CLASSES_BY_NAME,
    AnyRuleSetRequest,
    AnyRuleSetResponse,
)
from mex.common.transform import ensure_postfix, ensure_prefix
from mex.common.types import Identifier


def create_and_get_rule_set(
    rule_set: AnyRuleSetRequest,
    stable_target_id: Identifier | None = None,
) -> AnyRuleSetResponse:
    """Merge a rule set into the graph and read it back."""
    if stable_target_id is None:
        stable_target_id = Identifier.generate()

    connector = GraphConnector.get()
    response_class_name = ensure_postfix(rule_set.stemType, "RuleSetResponse")
    response_class = RULE_SET_RESPONSE_CLASSES_BY_NAME[response_class_name]
    rule_set_response = response_class(
        additive=rule_set.additive,
        preventive=rule_set.preventive,
        subtractive=rule_set.subtractive,
        stableTargetId=stable_target_id,
    )
    deque(connector.ingest_items([rule_set_response]))
    rule_types = [
        rule_set.additive.entityType,
        rule_set.subtractive.entityType,
        rule_set.preventive.entityType,
    ]
    graph_result = connector.fetch_rule_items(
        query_string=None,
        identifier=None,
        stable_target_id=stable_target_id,
        entity_type=rule_types,
        referenced_identifiers=None,
        reference_field=None,
        skip=0,
        limit=3,
    )
    return transform_raw_rules_to_rule_set_response(graph_result.one()["items"])


def get_rule_set_from_graph(
    stable_target_id: Identifier,
) -> AnyRuleSetResponse | None:
    """Read a rule set from the graph."""
    connector = GraphConnector.get()
    graph_result = connector.fetch_rule_items(
        query_string=None,
        identifier=None,
        stable_target_id=stable_target_id,
        entity_type=None,
        referenced_identifiers=None,
        reference_field=None,
        skip=0,
        limit=3,
    )
    if raw_rules := graph_result.one()["items"]:
        return transform_raw_rules_to_rule_set_response(raw_rules)
    return None


def update_and_get_rule_set(
    rule_set: AnyRuleSetRequest,
    stable_target_id: Identifier,
) -> AnyRuleSetResponse:
    """Merge a rule set into the graph and read it back."""
    connector = GraphConnector.get()
    if not connector.exists_item(
        stable_target_id, [ensure_prefix(rule_set.stemType, "Merged")]
    ):
        msg = "no merged item found for given identifier and type"
        raise NoResultFoundError(msg)
    return create_and_get_rule_set(rule_set, stable_target_id)


def delete_rule_set_from_graph(stable_target_id: Identifier) -> None:
    """Delete a rule-set by stableTargetId from the graph."""
    connector = GraphConnector.get()
    result = connector.delete_rule_set(stable_target_id)
    record = result.one_or_none()
    # If the MATCH didn't find the merged item, the query returns no rows
    if not record:
        msg = "Merged item was not found."
        raise NoResultFoundError(msg)
    # Note: We don't check if rules were deleted because per requirements,
    # we should return 204 even if no rules exist
