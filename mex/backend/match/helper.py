from collections import deque

from mex.backend.extracted.helpers import get_extracted_item_from_graph
from mex.backend.graph.connector import GraphConnector
from mex.backend.merged.helpers import get_merged_item_from_graph
from mex.backend.rules.helpers import get_rule_set_from_graph
from mex.common.models import RULE_SET_RESPONSE_CLASSES_BY_NAME
from mex.common.transform import ensure_postfix
from mex.common.types import AnyExtractedIdentifier, AnyMergedIdentifier


def match_item_in_graph(
    extracted_identifier: AnyExtractedIdentifier,
    merged_identifier: AnyMergedIdentifier,
) -> None:
    """Match an extracted item to a merged item in the graph database."""
    connector = GraphConnector.get()
    extracted_item = get_extracted_item_from_graph(extracted_identifier)
    merged_item = get_merged_item_from_graph(merged_identifier)

    # ensure that the old merged item has a rule set
    if not (rule_set := get_rule_set_from_graph(extracted_item.stableTargetId)):
        rule_set = RULE_SET_RESPONSE_CLASSES_BY_NAME[
            ensure_postfix(extracted_item.stemType, "RuleSetResponse")
        ](stableTargetId=extracted_item.stableTargetId)
        deque(connector.ingest_items([rule_set]))

    connector.match_item(extracted_item, merged_item)
