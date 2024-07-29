from fastapi import APIRouter
from fastapi.responses import JSONResponse

from mex.backend.graph.connector import GraphConnector
from mex.backend.ingest.models import BulkIngestRequest, BulkIngestResponse
from mex.backend.transform import to_primitive

router = APIRouter()


@router.post("/ingest", status_code=201, tags=["extractors"])
def ingest_extracted_items(request: BulkIngestRequest) -> BulkIngestResponse:
    """Ingest batches of extracted items grouped by their type."""
    connector = GraphConnector.get()
    models = request.get_all()
    identifiers = connector.ingest(models)
    response = BulkIngestResponse(identifiers=identifiers)
    return JSONResponse(to_primitive(response))  # type: ignore[return-value]
