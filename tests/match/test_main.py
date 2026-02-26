from typing import TYPE_CHECKING

import pytest
from starlette import status

if TYPE_CHECKING:  # pragma: no cover
    from starlette.testclient import TestClient

    from tests.conftest import DummyData


@pytest.mark.integration
def test_match_item(
    client_with_api_key_write_permission: TestClient,
    loaded_dummy_data: DummyData,
) -> None:
    response = client_with_api_key_write_permission.post(
        "/v0/match/",
        json={
            "extractedIdentifier": loaded_dummy_data["organization_1"].identifier,
            "mergedIdentifier": loaded_dummy_data["organization_2"].stableTargetId,
        },
    )
    assert response.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR, response.text
    assert "NotImplemented" in response.text
