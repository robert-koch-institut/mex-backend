from mex.backend.graph.connector import GraphConnector
from mex.backend.ingest.models import BulkIngestResponse
from mex.common.models import AnyExtractedModel


def ingest_extracted_items_into_graph(
    items: list[AnyExtractedModel],
) -> BulkIngestResponse:
    """Ingest a batch of extracted items and return their identifiers.

    Args:
        items: list of AnyExtractedModel

    Returns:
        List of identifiers of the ingested items
    """
    connector = GraphConnector.get()
    identifiers = connector.ingest(items)
    return BulkIngestResponse(identifiers=identifiers)
