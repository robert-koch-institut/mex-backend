from collections import deque
from typing import TYPE_CHECKING, Annotated

from pydantic import Field
from pydantic_core import ValidationError

from mex.backend.graph.connector import GraphConnector
from mex.backend.graph.exceptions import InconsistentGraphError, NoResultFoundError
from mex.backend.rules.transform import transform_raw_rule_set_to_rule_set_response
from mex.common.logging import logger
from mex.common.models import (
    RULE_SET_RESPONSE_CLASSES_BY_NAME,
    AnyRuleModel,
    AnyRuleSetRequest,
    AnyRuleSetResponse,
    PaginatedItemsContainer,
)
from mex.common.transform import ensure_postfix, ensure_prefix
from mex.common.types import Identifier

if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Sequence

    from mex.backend.models import ReferenceFilter


def search_rule_items_in_graph(
    *,
    query_string: str | None = None,
    entity_type: Sequence[str] | None = None,
    reference_filters: Sequence[ReferenceFilter] | None = None,
    skip: int = 0,
    limit: int = 100,
) -> PaginatedItemsContainer[AnyRuleModel]:
    """Search for rule items in the graph.

    Args:
        query_string: Full text search query term
        entity_type: Optional entity type filter
        reference_filters: Optional reference field filters
        skip: How many items to skip for pagination
        limit: How many items to return at most

    Raises:
        InconsistentGraphError: When the graph response cannot be parsed

    Returns:
        Paginated list of rule items
    """
    connector = GraphConnector.get()
    graph_result = connector.fetch_rule_items(
        query_string=query_string,
        identifier=None,
        entity_type=entity_type,
        reference_filters=reference_filters,
        skip=skip,
        limit=limit,
    )
    search_result = graph_result.one()
    for item in search_result["items"]:
        # stableTargetId is expanded from the stableTargetId relationship, but is not
        # a field on rule models yet, so drop it before the items are validated
        item.pop("stableTargetId", None)
    try:
        return PaginatedItemsContainer[
            Annotated[AnyRuleModel, Field(discriminator="entityType")]
        ].model_validate(search_result)
    except ValidationError as error:
        raise InconsistentGraphError from error


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
        workflow=rule_set.workflow,
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
    result = connector.fetch_rule_set_response(stable_target_id)
    if record := result.one_or_none():
        return transform_raw_rule_set_to_rule_set_response(record)
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
