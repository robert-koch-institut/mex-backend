from typing import TYPE_CHECKING

import pytest
from pytest import MonkeyPatch
from starlette import status

if TYPE_CHECKING:  # pragma: no cover
    from fastapi.testclient import TestClient

    from mex.backend.settings import BackendSettings


@pytest.mark.integration
def test_flush_graph_database_unauthorized(testing_app_client: TestClient) -> None:
    response = testing_app_client.delete("/v0/_system/graph")
    assert response.status_code == status.HTTP_401_UNAUTHORIZED, response.text
    assert response.json() == {"detail": "Not authenticated"}


@pytest.mark.integration
def test_flush_graph_database_forbidden(
    testing_app_client_with_api_key_read_permission: TestClient,
) -> None:
    response = testing_app_client_with_api_key_read_permission.delete(
        "/v0/_system/graph"
    )
    assert response.status_code == status.HTTP_403_FORBIDDEN, response.text
    assert response.json() == {"detail": "Unauthorized API Key."}


def test_flush_graph_database_refused(
    testing_app_client_with_api_key_write_permission: TestClient,
) -> None:
    response = testing_app_client_with_api_key_write_permission.delete(
        "/v0/_system/graph"
    )
    assert response.status_code == status.HTTP_403_FORBIDDEN, response.text
    assert response.json() == {"detail": "refusing to flush the database"}


@pytest.mark.integration
def test_flush_graph_database(
    testing_app_client_with_api_key_write_permission: TestClient,
    settings: BackendSettings,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.setattr(settings, "debug", True)
    response = testing_app_client_with_api_key_write_permission.delete(
        "/v0/_system/graph"
    )
    assert response.status_code == status.HTTP_200_OK, response.text
    assert response.json() == {"status": "ok"}
