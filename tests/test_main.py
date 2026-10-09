from typing import TYPE_CHECKING, cast

import pytest
from fastapi.testclient import TestClient
from packaging.version import Version
from pytest import MonkeyPatch
from starlette import status

from mex.backend.graph import connector as connector_module
from mex.backend.graph.exceptions import IncompatibleVersionError
from mex.backend.main import app
from tests.conftest import get_graph

if TYPE_CHECKING:
    from fastapi import FastAPI


def test_all_endpoints_require_authorization(entrypoint_app: TestClient) -> None:
    excluded_routes = [
        "/v0/_system/check",
        "/v0/_system/metrics",
        "/v0/_system/neo4j",
        "/v0/_system/valkey",
    ]
    app = cast("FastAPI", entrypoint_app.app)
    for path, operations in app.openapi()["paths"].items():
        if path in excluded_routes:
            continue
        for method in operations:
            client_method = getattr(entrypoint_app, method.lower())
            assert client_method(path).status_code == status.HTTP_401_UNAUTHORIZED


def test_app_startup_fails_on_incompatible_neo4j_version(
    monkeypatch: MonkeyPatch,
) -> None:
    """Guard the `GraphConnector.get` entry in `main.startup_tasks`.

    Without that entry, the neo4j version check only runs lazily on the first
    graph-touching request, where `IncompatibleVersionError` gets caught by the
    `BackendError` exception handler and turned into a 400 response instead of
    failing application startup.
    """
    monkeypatch.setattr(connector_module, "MIN_NEO4J_VERSION", Version("9999.1.0"))
    with pytest.raises(IncompatibleVersionError), TestClient(app):
        pass


@pytest.mark.integration
def test_database_is_seeded() -> None:
    assert get_graph() == [
        {
            "end": "00000000000000",
            "label": "hadPrimarySource",
            "position": 0,
            "start": "00000000000001",
        },
        {
            "end": "00000000000000",
            "label": "hadPrimarySource",
            "position": 0,
            "start": "00000000000003",
        },
        {
            "end": "00000000000000",
            "label": "stableTargetId",
            "position": 0,
            "start": "00000000000001",
        },
        {
            "end": "00000000000002",
            "label": "stableTargetId",
            "position": 0,
            "start": "00000000000003",
        },
        {
            "identifier": "00000000000000",
            "label": "MergedPrimarySource",
        },
        {
            "identifier": "00000000000001",
            "identifierInPrimarySource": "mex",
            "label": "ExtractedPrimarySource",
        },
        {
            "identifier": "00000000000002",
            "label": "MergedPrimarySource",
        },
        {
            "identifier": "00000000000003",
            "identifierInPrimarySource": "mex-editor",
            "label": "ExtractedPrimarySource",
        },
    ]
