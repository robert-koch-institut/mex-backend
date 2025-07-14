from typing import TYPE_CHECKING, cast

import pytest
from fastapi.testclient import TestClient
from neo4j import GraphDatabase
from starlette import status

from mex.backend.main import app
from mex.backend.settings import BackendSettings

if TYPE_CHECKING:  # pragma: no cover
    from starlette.routing import Route


def test_all_endpoints_require_authorization(client: TestClient) -> None:
    excluded_routes = [
        "/openapi.json",
        "/docs",
        "/docs/oauth2-redirect",
        "/redoc",
        "/v0/_system/check",
        "/v0/_system/metrics",
    ]
    for route in cast("list[Route]", app.routes):
        if route.methods and route.path not in excluded_routes:
            for method in route.methods:
                client_method = getattr(client, method.lower())
                assert (
                    client_method(route.path).status_code
                    == status.HTTP_401_UNAUTHORIZED
                )


@pytest.mark.integration
def test_database_is_empty(settings: BackendSettings) -> None:
    with GraphDatabase.driver(
        settings.graph_url,
        auth=(
            settings.graph_user.get_secret_value(),
            settings.graph_password.get_secret_value(),
        ),
        database=settings.graph_db,
    ) as driver:
        result = driver.execute_query("MATCH (n) RETURN n;")
    assert result.records == []
