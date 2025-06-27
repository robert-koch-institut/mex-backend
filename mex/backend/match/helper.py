from mex.backend.extracted.helpers import search_extracted_items_in_graph
from mex.backend.fields import INBOUND_REFERENCE_FIELDS_BY_CLASS_NAME
from mex.backend.graph.connector import GraphConnector
from mex.backend.rules.helpers import (
    get_rule_set_from_graph,
    merge_rule_sets,
    remove_primary_source_from_rule,
)
from mex.common.exceptions import MExError
from mex.common.models import (
    RULE_SET_RESPONSE_CLASSES_BY_NAME,
    AnyExtractedModel,
    AnyMergedModel,
    ExtractedPerson,
    ExtractedPrimarySource,
)
from mex.common.transform import ensure_postfix

QUERY_SANITY_LIMIT = 50


def match_item_in_graph(
    extracted_item: AnyExtractedModel,
    merged_item: AnyMergedModel,
) -> None:
    """Match an extracted item to a merged item in the graph database.

    Args:
        extracted_item: The extracted item to match
        merged_item: The merged item to match to

    Raises:
        MExError: if stemTypes don't match
    """
    # check preconditions
    if extracted_item.stemType != merged_item.stemType:
        msg = (
            f"Type mismatch: extracted item is "
            f"{extracted_item.stemType}, merged item is "
            f"{merged_item.stemType}."
        )
        raise MExError(msg)
    if extracted_item.entityType in [
        ExtractedPerson.entityType,  # not supported because of consent
        ExtractedPrimarySource.entityType,  # too many inbound references
    ]:
        msg = f"Type not supported: cannot match type {extracted_item.stemType} yet."
        raise MExError(msg)

    # gather models for context
    old_extracted_siblings = [
        extracted_sibling
        for extracted_sibling in search_extracted_items_in_graph(
            stable_target_id=extracted_item.stableTargetId,
            limit=QUERY_SANITY_LIMIT,
        ).items
        if extracted_sibling.identifier != extracted_item.identifier
    ]
    new_extracted_siblings = search_extracted_items_in_graph(
        stable_target_id=merged_item.identifier,
        limit=QUERY_SANITY_LIMIT,
    ).items
    old_inbound_references = [
        extracted_sibling
        for field in INBOUND_REFERENCE_FIELDS_BY_CLASS_NAME[merged_item.entityType]
        for extracted_sibling in search_extracted_items_in_graph(
            referenced_identifiers=[merged_item.identifier],
            reference_field=field,
            limit=QUERY_SANITY_LIMIT,
        ).items
        if extracted_sibling.identifier != extracted_item.identifier
    ]
    old_rule_set = get_rule_set_from_graph(
        stable_target_id=extracted_item.stableTargetId
    ) or RULE_SET_RESPONSE_CLASSES_BY_NAME[
        ensure_postfix(merged_item.stemType, "RuleSetResponse")
    ](stableTargetId=merged_item.identifier)
    new_rule_set = get_rule_set_from_graph(
        stable_target_id=extracted_item.stableTargetId,
    ) or RULE_SET_RESPONSE_CLASSES_BY_NAME[
        ensure_postfix(merged_item.stemType, "RuleSetResponse")
    ](stableTargetId=merged_item.identifier)
    old_primary_source_identifiers = [
        extracted_sibling.hadPrimarySource
        for extracted_sibling in [*new_extracted_siblings, extracted_item]
    ]

    # bail if the item seems to complex
    if any(
        len(items) >= QUERY_SANITY_LIMIT
        for items in [
            old_extracted_siblings,
            new_extracted_siblings,
            old_inbound_references,
        ]
    ):
        msg = "The item is too complex to merge in a reasonable time."
        raise MExError(msg)

    # run matching steps
    merge_rule_sets(
        old_rule_set,
        new_rule_set,
        old_primary_source_identifiers,
    )
    remove_primary_source_from_rule(
        old_rule_set.preventive,
        extracted_item.hadPrimarySource,
    )
    if old_extracted_siblings:
        update_rule_sets = None
        delete_merged_item = None
    else:
        update_rule_sets = [old_rule_set, new_rule_set]
        delete_merged_item = merged_item
    connector = GraphConnector.get()
    connector.match_item(
        update_extracted_item=extracted_item,
        new_stable_target_id=merged_item.identifier,
        update_rule_sets=update_rule_sets,
        delete_merged_item=delete_merged_item,
    )
