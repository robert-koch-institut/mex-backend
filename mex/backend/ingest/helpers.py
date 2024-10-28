from pydantic import ValidationError

from mex.backend.graph.connector import GraphConnector
from mex.backend.graph.exceptions import InconsistentGraphError
from mex.backend.ingest.models import BulkIngestResponse
from mex.backend.utils import reraising
from mex.common.models import AnyExtractedModel


def ingest_extracted_items_into_graph(
    items: list[AnyExtractedModel],
) -> BulkIngestResponse:
    """Ingest a batch of extracted items and return their identifiers.

    Args:
        items: list of AnyExtractedModel

    Raises:
        InconsistentGraphError: When the graph response cannot be parsed

    Returns:
        List of identifiers of the ingested items
    """
    connector = GraphConnector.get()
    identifiers = connector.ingest(items)
    return reraising(
        ValidationError,
        InconsistentGraphError,
        BulkIngestResponse,
        identifiers=identifiers,
    )
