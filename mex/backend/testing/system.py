from fastapi import APIRouter, Depends, HTTPException
from starlette import status

from mex.backend.graph.connector import GraphConnector
from mex.backend.security import has_write_access
from mex.backend.settings import BackendSettings
from mex.common.connector import CONNECTOR_STORE
from mex.common.models import Status

database_deletion_router = APIRouter(dependencies=[Depends(has_write_access)])


@database_deletion_router.delete(
    "/_system/graph",
    tags=["system"],
)
def flush_graph_database() -> Status:
    """Flush the database."""
    settings = BackendSettings.get()
    if settings.debug is True:
        connector = GraphConnector.get()
        connector.flush()
        connector.close()
        CONNECTOR_STORE.pop(GraphConnector)
        return Status(status="ok")
    raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="refusing to flush the database",
    )
