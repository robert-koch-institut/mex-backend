from typing import TYPE_CHECKING

import pytest
from starlette import status

from mex.common.models import MEX_PRIMARY_SOURCE_STABLE_TARGET_ID

if TYPE_CHECKING:  # pragma: no cover
    from fastapi.testclient import TestClient


@pytest.mark.integration
def test_get_preview_person_from_login_success(
    testing_app_client_that_is_ldap_authenticated: TestClient,
) -> None:
    response = testing_app_client_that_is_ldap_authenticated.post(
        "/v0/preview-person-from-login"
    )
    assert response.status_code == status.HTTP_200_OK, response.text
    data = response.json()
    assert data["email"] == ["Writer@rki.com"]
    assert data["fullName"] == ["Writer"]
    assert data["$type"] == "PreviewPerson"


@pytest.mark.integration
def test_search_persons_or_contact_points_in_ldap_success(
    testing_app_client_with_api_key_read_permission: TestClient,
) -> None:
    response = testing_app_client_with_api_key_read_permission.get(
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
