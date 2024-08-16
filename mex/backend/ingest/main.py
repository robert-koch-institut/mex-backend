from typing import Annotated

from fastapi import APIRouter
from pydantic import PlainSerializer
from starlette import status

from mex.backend.graph.connector import GraphConnector
from mex.backend.ingest.models import BulkIngestRequest, BulkIngestResponse
from mex.backend.serialization import to_primitive

router = APIRouter()


@router.post("/ingest", status_code=status.HTTP_201_CREATED, tags=["extractors"])
def ingest_extracted_items(
    request: BulkIngestRequest,
) -> Annotated[BulkIngestResponse, PlainSerializer(to_primitive)]:
    """Ingest batches of extracted items grouped by their type."""
    connector = GraphConnector.get()
    identifiers = connector.ingest_extracted(request.items)
    return BulkIngestResponse(identifiers=identifiers)
