from mex.backend.exceptions import BackendError
from mex.backend.extracted.helpers import get_extracted_item_from_graph
from mex.backend.graph.connector import GraphConnector
from mex.backend.merged.helpers import get_merged_item_from_graph
from mex.common.types import AnyExtractedIdentifier, AnyMergedIdentifier

RESTRICTED_STEM_TYPES_FOR_MERGING = ["Person", "Consent"]


def match_items_in_graph(
    extracted_item_identifier: AnyExtractedIdentifier,
    merged_item_identifier: AnyMergedIdentifier,
) -> None:
    """Assign an extracted item to a new merged item."""
    connector = GraphConnector.get()
    extracted_item = get_extracted_item_from_graph(extracted_item_identifier)
    merged_item = get_merged_item_from_graph(merged_item_identifier)

    if extracted_item.stemType != merged_item.stemType:
        msg = "Cannot match extracted item to merged item of different type."
        raise BackendError(msg)
    if merged_item.stemType in RESTRICTED_STEM_TYPES_FOR_MERGING:
        msg = "Cannot match and merge the given items due to type restrictions."
        raise BackendError(msg)

    connector.match_items(
        extracted_item.identifier,
        extracted_item.entityType,
        merged_item.identifier,
        merged_item.entityType,
    )
