from fastapi import APIRouter
from starlette import status

from mex.backend.ingest.helpers import ingest_extracted_items_into_graph
from mex.backend.ingest.models import BulkIngestRequest, BulkIngestResponse

router = APIRouter()


@router.post("/ingest", status_code=status.HTTP_201_CREATED, tags=["extractors"])
def ingest_extracted_items(request: BulkIngestRequest) -> BulkIngestResponse:
    """Ingest a batch of extracted items and return their identifiers."""
    return ingest_extracted_items_into_graph(request.items)
