from typing import TYPE_CHECKING

import pytest
from starlette import status

if TYPE_CHECKING:  # pragma: no cover
    from fastapi.testclient import TestClient


@pytest.mark.integration
def test_get_current_person_success(
    testing_app_client_with_bearer_write_permission: TestClient,
) -> None:
    response = testing_app_client_with_bearer_write_permission.get(
        "/v0/merged-person/self"
    )
    assert response.status_code == status.HTTP_200_OK, response.text
    data = response.json()
    assert data["email"] == ["Writer@rki.com"]
    assert data["fullName"] == ["Writer"]
    assert data["$type"] == "MergedPerson"
