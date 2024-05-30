import pytest
from fastapi.testclient import TestClient
from neo4j import GraphDatabase

from mex.backend.main import app
from mex.backend.settings import BackendSettings


def test_openapi_schema(client: TestClient) -> None:
    response = client.get("/openapi.json")
    assert response.status_code == 200, response.text

    schema = response.json()
    assert schema["info"]["title"] == "mex-backend"
    assert schema["servers"] == [{"url": "http://localhost:8080/"}]
    assert schema["components"]["schemas"]["MergedPersonIdentifier"] == {
        "title": "MergedPersonIdentifier",
        "type": "string",
        "description": "Identifier for merged persons.",
        "pattern": "^[a-zA-Z0-9]{14,22}$",
    }

    cached = client.get("/openapi.json")
    assert cached.json() == schema


def test_health_check(client: TestClient) -> None:
    response = client.get("/v0/_system/check")
    assert response.status_code == 200, response.text
    assert response.json() == {"status": "ok"}


def test_all_endpoints_require_authorization(client: TestClient) -> None:
    excluded_routes = [
        "/openapi.json",
        "/docs",
        "/docs/oauth2-redirect",
        "/redoc",
        "/v0/_system/check",
    ]
    for route in app.routes:
        if route.path not in excluded_routes:
            for method in route.methods:
                client_method = getattr(client, method.lower())
                assert client_method(route.path).status_code == 401


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
