import json
import logging
from typing import Any
from unittest.mock import MagicMock, Mock

import pydantic_core
import pytest
from fastapi.testclient import TestClient
from neo4j import GraphDatabase
from pydantic import ValidationError
from pytest import LogCaptureFixture

from mex.backend.main import (
    app,
    close_connectors,
    handle_uncaught_exception,
)
from mex.backend.settings import BackendSettings
from mex.common.connector import ConnectorContext
from mex.common.exceptions import MExError


def test_openapi_schema(client: TestClient) -> None:
    response = client.get("/openapi.json")
    assert response.status_code == 200, response.text

    schema = response.json()
    assert schema["info"]["title"] == "mex-backend"
    assert schema["servers"] == [{"url": "http://localhost:8080/"}]
    assert schema["components"]["schemas"]["PersonID"] == {
        "title": "PersonID",
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


@pytest.mark.parametrize(
    ("exception", "expected"),
    [
        (
            TypeError("foo"),
            {"debug": {"errors": [{"type": "TypeError"}]}, "message": "foo"},
        ),
        (
            ValidationError.from_exception_data(
                "foo",
                [
                    {
                        "type": pydantic_core.PydanticCustomError(
                            "TestError", "You messed up!"
                        ),
                        "loc": ("integerAttribute",),
                        "input": "mumbojumbo",
                    }
                ],
            ),
            {
                "debug": {
                    "errors": [
                        {
                            "input": "mumbojumbo",
                            "loc": ["integerAttribute"],
                            "msg": "You messed up!",
                            "type": "TestError",
                        }
                    ]
                },
                "message": "1 validation error for foo\n"
                "integerAttribute\n"
                "  You messed up! [type=TestError, input_value='mumbojumbo', "
                "input_type=str]",
            },
        ),
        (
            MExError("bar"),
            {"message": "MExError: bar ", "debug": {"errors": [{"type": "MExError"}]}},
        ),
    ],
    ids=["TypeError", "ValidationError", "MExError"],
)
def test_handle_uncaught_exception(
    exception: Exception, expected: dict[str, Any]
) -> None:
    response = handle_uncaught_exception(Mock(), exception)
    assert response.status_code == 500, response.body
    assert json.loads(response.body) == expected


def test_close_all_connectors(caplog: LogCaptureFixture) -> None:
    context = {
        "ConnectorA": Mock(close=MagicMock()),
        "ConnectorB": Mock(close=MagicMock(side_effect=Exception())),
    }
    ConnectorContext.set(context)

    with caplog.at_level(logging.INFO):
        close_connectors()
    assert "Closed ConnectorA" in caplog.text
    assert "Error closing ConnectorB" in caplog.text


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
def test_database_is_empty(settings: BackendSettings):
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
