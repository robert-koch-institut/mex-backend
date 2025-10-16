import pytest
from fastapi.testclient import TestClient

from mex.common.models import MEX_PRIMARY_SOURCE_STABLE_TARGET_ID


@pytest.mark.integration
def test_get_merged_person_from_login_success(
    client_with_basic_auth_write_permission: TestClient,
) -> None:
    response = client_with_basic_auth_write_permission.post(
        "/v0/merged-person-from-login"
    )
    assert response.status_code == 200
    data = response.json()
    assert data["email"] == ["Writer@rki.com"]
    assert data["fullName"] == ["Writer"]
    assert data["$type"] == "MergedPerson"


@pytest.mark.integration
def test_search_persons_or_contact_points_in_ldap_success(
    client_with_basic_auth_write_permission: TestClient,
) -> None:
    response = client_with_basic_auth_write_permission.get(
        "/v0/ldap", params={"q": "mex", "limit": 1}
    )
    assert response.status_code == 200
    data = response.json()
    assert data == {...}
    item = data["items"][0]
    assert item["hadPrimarySource"] == MEX_PRIMARY_SOURCE_STABLE_TARGET_ID
    assert item["identifierInPrimarySource"] == "mex"
    assert item["email"] == ["mex@rki.com"]
    assert item["fullName"] == ["mex"]
    assert item["$type"] == "ExtractedPerson"
