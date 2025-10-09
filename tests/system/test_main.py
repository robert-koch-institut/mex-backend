from fastapi.testclient import TestClient
from starlette import status

from mex.common.testing import Joker


def test_health_check(client: TestClient) -> None:
    response = client.get("/v0/_system/check")
    assert response.status_code == status.HTTP_200_OK, response.text
    assert response.json() == {"status": "ok", "version": Joker()}
