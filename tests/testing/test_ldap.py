from typing import TYPE_CHECKING

import pytest
from starlette import status

from mex.common.models import MEX_PRIMARY_SOURCE_STABLE_TARGET_ID

if TYPE_CHECKING:
    from fastapi.testclient import TestClient


@pytest.mark.integration
def test_get_merged_person_from_login_success(
    client_with_basic_auth_write_permission: TestClient,
) -> None:
    response = client_with_basic_auth_write_permission.post(
        "/v0/merged-person-from-login"
    )
    assert response.status_code == status.HTTP_200_OK, response.text
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
    assert response.status_code == status.HTTP_200_OK, response.text
    data = response.json()
    assert data == {
        "items": [
            {
                "$type": "ExtractedPerson",
                "affiliation": [],
                "email": [
                    "mex1@rki.com",
                ],
                "familyName": [],
                "fullName": [
                    "mex1",
                ],
                "givenName": [],
                "hadPrimarySource": MEX_PRIMARY_SOURCE_STABLE_TARGET_ID,
                "identifier": "bFQoRhcVH5DHUq",
                "identifierInPrimarySource": "mex1",
                "isniId": [],
                "memberOf": [],
                "orcidId": [],
                "stableTargetId": "bFQoRhcVH5DHUr",
            },
        ],
        "total": 1,
    }
