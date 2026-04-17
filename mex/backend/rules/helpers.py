from collections import deque

from mex.backend.graph.connector import GraphConnector
from mex.backend.graph.exceptions import NoResultFoundError
from mex.backend.models import ReferenceFilter
from mex.backend.rules.transform import transform_raw_rules_to_rule_set_response
from mex.backend.types import ReferenceFieldName
from mex.common.logging import logger
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

    if fetched_rule_set := get_rule_set_from_graph(stable_target_id):
        return fetched_rule_set

    msg = f"rule-set was not found after it was just inserted: {stable_target_id}"
    raise RuntimeError(msg)


def get_rule_set_from_graph(
    stable_target_id: Identifier,
) -> AnyRuleSetResponse | None:
    """Read a rule set from the graph."""
    connector = GraphConnector.get()
    graph_result = connector.fetch_rule_items(
        query_string=None,
        identifier=None,
        entity_type=None,
        reference_filters=[
            ReferenceFilter(
                field=ReferenceFieldName("stableTargetId"),
                identifiers=[stable_target_id],
            )
        ],
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
    if not record:
        msg = "Merged item was not found."
        raise NoResultFoundError(msg)
    logger.info("deleted rule set for %s: %s", stable_target_id, record)
