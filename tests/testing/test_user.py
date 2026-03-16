from typing import TYPE_CHECKING

import pytest
from starlette import status

if TYPE_CHECKING:  # pragma: no cover
    from fastapi.testclient import TestClient


@pytest.mark.integration
def test_get_current_user_success(
    client_with_bearer_write_permission: "TestClient",
) -> None:
    response = client_with_bearer_write_permission.get("/v0/user/me")
    assert response.status_code == status.HTTP_200_OK, response.text
    data = response.json()
    assert data["email"] == ["Writer@rki.com"]
    assert data["fullName"] == ["Writer"]
    assert data["$type"] == "MergedPerson"
