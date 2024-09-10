from fastapi import APIRouter
from starlette import status

from mex.backend.graph.connector import GraphConnector
from mex.backend.ingest.models import BulkIngestRequest, BulkIngestResponse

router = APIRouter()


@router.post("/ingest", status_code=status.HTTP_201_CREATED, tags=["extractors"])
def ingest_extracted_items(request: BulkIngestRequest) -> BulkIngestResponse:
    """Ingest batches of extracted items grouped by their type."""
    connector = GraphConnector.get()
    identifiers = connector.ingest(request.items)
    return BulkIngestResponse(identifiers=identifiers)
