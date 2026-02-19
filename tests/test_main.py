from typing import TYPE_CHECKING, cast

import pytest
from neo4j import GraphDatabase
from starlette import status

if TYPE_CHECKING:  # pragma: no cover
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from starlette.routing import Route

    from mex.backend.settings import BackendSettings


def test_all_endpoints_require_authorization(entrypoint_app: TestClient) -> None:
    excluded_routes = [
        "/openapi.json",
        "/docs",
        "/docs/oauth2-redirect",
        "/redoc",
        "/v0/_system/check",
        "/v0/_system/metrics",
    ]
    app = cast("FastAPI", entrypoint_app.app)
    for route in cast("list[Route]", app.routes):
        if route.methods and route.path not in excluded_routes:
            for method in route.methods:
                client_method = getattr(entrypoint_app, method.lower())
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
