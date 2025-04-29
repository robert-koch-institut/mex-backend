from importlib.metadata import version

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import PlainTextResponse
from starlette import status

from mex.backend.graph.connector import GraphConnector
from mex.backend.security import has_write_access
from mex.backend.settings import BackendSettings
from mex.backend.system.models import GraphStatus, SystemStatus
from mex.common.connector import CONNECTOR_STORE

router = APIRouter()


@router.get(
    "/_system/check",
    tags=["system"],
)
def check_system_status() -> SystemStatus:
    """Check that the backend server is healthy and responsive."""
    return SystemStatus(status="ok", version=version("mex-backend"))


@router.delete(
    "/_system/graph",
    dependencies=[Depends(has_write_access)],
    tags=["system"],
)
def flush_graph_database() -> GraphStatus:
    """Flush the database (only in debug mode)."""
    settings = BackendSettings.get()
    if settings.debug is True:
        connector = GraphConnector.get()
        connector.flush()
        connector.close()
        CONNECTOR_STORE.pop(GraphConnector)
        return GraphStatus(status="ok")
    raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="refusing to flush the database",
    )


@router.get(
    "/_system/metrics",
    response_class=PlainTextResponse,
    tags=["system"],
)
def get_prometheus_metrics() -> str:
    """Get connector metrics for prometheus."""
    return "\n\n".join(
        f"# TYPE {key} counter\n{key} {value}"
        for key, value in CONNECTOR_STORE.metrics().items()
    )
