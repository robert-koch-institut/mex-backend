from typing import TYPE_CHECKING

import pytest
from starlette import status

from mex.backend.auxiliary.constants import RKI_WIKIDATA_ID
from mex.backend.auxiliary.helpers import cached_organization

if TYPE_CHECKING:  # pragma: no cover
    from fastapi.testclient import TestClient


@pytest.mark.usefixtures("mocked_ldap", "mocked_wikidata")
def test_search_persons_or_contact_points_in_ldap(
    client_with_api_key_read_permission: TestClient,
) -> None:
    response = client_with_api_key_read_permission.get("/v0/ldap", params={"q": "*"})
    rki_organization = cached_organization(RKI_WIKIDATA_ID)
    assert response.status_code == status.HTTP_200_OK, response.text
    response_json = response.json()
    assert response_json["total"] == 5
    assert {
        "hadPrimarySource": "ebs5siX85RkdrhBRlsYgRP",
        "identifierInPrimarySource": "00000000-0000-4000-8000-000000000141",
        "affiliation": [rki_organization.stableTargetId],
        "email": ["MoritzM@ldapmock.local"],
        "familyName": ["Mueller"],
        "fullName": ["Moritz Mueller"],
        "givenName": ["Moritz"],
        "isniId": [],
        "memberOf": ["cjna2jitPngp6yIV63cdi9"],
        "orcidId": [],
        "$type": "ExtractedPerson",
        "identifier": "c6OEuSCqKQYzHNHPC96hEF",
        "stableTargetId": "e9KbtvmWDHuiNRgo3KhiBv",
    } in response_json["items"]


@pytest.mark.usefixtures("mocked_ldap", "mocked_wikidata")
def test_ldap_pagination(
    client_with_api_key_read_permission: TestClient,
) -> None:
    all_response = client_with_api_key_read_permission.get(
        "/v0/ldap", params={"q": "*"}
    )
    assert all_response.status_code == status.HTTP_200_OK, all_response.text
    all_items = all_response.json()

    paginated_response = client_with_api_key_read_permission.get(
        "/v0/ldap", params={"q": "*", "offset": 1, "limit": 2}
    )
    assert paginated_response.status_code == status.HTTP_200_OK, paginated_response.text
    paginated_items = paginated_response.json()

    assert paginated_items["total"] == all_items["total"]
    assert len(paginated_items["items"]) == 2
    assert paginated_items["items"] == all_items["items"][1:3]
