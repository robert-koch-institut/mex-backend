from mex.backend.graph.connector import GraphConnector
from mex.common.types import AnyExtractedIdentifier, AnyMergedIdentifier


def match_item_in_graph(
    extracted_identifier: AnyExtractedIdentifier,
    merged_identifier: AnyMergedIdentifier,
) -> None:
    """Match an extracted item to a merged item in the graph database."""
    connector = GraphConnector.get()
    connector.match_item(
        extracted_identifier=str(extracted_identifier),
        merged_identifier=str(merged_identifier),
    )
