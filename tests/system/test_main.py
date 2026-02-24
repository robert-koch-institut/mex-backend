from typing import TYPE_CHECKING

from starlette import status

from mex.common.testing import Joker

if TYPE_CHECKING:  # pragma: no cover
    from fastapi.testclient import TestClient


def test_openapi(client: TestClient) -> None:
    response = client.get("/openapi.json")
    assert response.status_code == status.HTTP_200_OK, response.text
    response_json = response.json()
    assert "/v0/extracted-item/{identifier}" in response_json["paths"]
    assert "PreventiveOrganizationalUnit" in response_json["components"]["schemas"]


def test_health_check(client: TestClient) -> None:
    response = client.get("/v0/_system/check")
    assert response.status_code == status.HTTP_200_OK, response.text
    assert response.json() == {"status": "ok", "version": Joker()}
