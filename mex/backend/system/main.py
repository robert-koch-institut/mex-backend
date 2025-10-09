from importlib.metadata import version

from fastapi import APIRouter
from fastapi.responses import PlainTextResponse

from mex.common.connector import CONNECTOR_STORE
from mex.common.models import VersionStatus

router = APIRouter()


@router.get(
    "/_system/check",
    tags=["system"],
)
def check_system_status() -> VersionStatus:
    """Check that the backend server is healthy and responsive."""
    return VersionStatus(status="ok", version=version("mex-backend"))


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
