from fastapi import APIRouter, Depends, HTTPException
from starlette import status

from mex.backend.graph.connector import GraphConnector
from mex.backend.security import has_write_access
from mex.backend.settings import BackendSettings
from mex.backend.system.models import SystemStatus
from mex.common.connector import CONNECTOR_STORE

router = APIRouter()


@router.get("/_system/check", tags=["system"])
def check_system_status() -> SystemStatus:
    """Check that the backend server is healthy and responsive."""
    return SystemStatus(status="ok")


@router.delete(
    "/_system/graph",
    dependencies=[Depends(has_write_access)],
    tags=["system"],
)
def flush_graph_database() -> SystemStatus:
    """Flush the database (only in debug mode)."""
    settings = BackendSettings.get()
    if settings.debug is True:
        connector = GraphConnector.get()
        connector.flush()
        connector.close()
        CONNECTOR_STORE.pop(GraphConnector)
        return SystemStatus(status="ok")
    raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail=f"refusing to flush the database debug={settings.debug}",
    )
