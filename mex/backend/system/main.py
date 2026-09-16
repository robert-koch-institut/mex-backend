from importlib.metadata import version

from fastapi import APIRouter
from fastapi.responses import PlainTextResponse

from mex.backend.cache import get_cache_connector
from mex.backend.graph.connector import get_graph_status
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
    "/_system/neo4j",
    tags=["system"],
)
def check_neo4j_status() -> VersionStatus:
    """Check the status and version of the neo4j graph database."""
    return get_graph_status()


@router.get(
    "/_system/valkey",
    tags=["system"],
)
def check_valkey_status() -> VersionStatus:
    """Check the status and version of the configured cache connector."""
    return get_cache_connector().get_status()


@router.get(
    "/_system/metrics",
    response_class=PlainTextResponse,
    tags=["system"],
)
def get_prometheus_metrics() -> str:
    """Get connector metrics for prometheus.

    Monotonically increasing metrics are named with a `_total` suffix, as per
    prometheus convention, and are announced as counters. All others are gauges.
    """
    return "\n\n".join(
        f"# TYPE {key} {metric_type}\n{key} {value}"
        for key, value, metric_type in (
            (key, value, "counter" if key.endswith("_total") else "gauge")
            for key, value in CONNECTOR_STORE.metrics().items()
        )
    )
