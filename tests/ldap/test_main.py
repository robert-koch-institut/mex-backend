from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

import pytest
from starlette import status

from mex.backend.auxiliary.constants import RKI_WIKIDATA_ID
from mex.backend.auxiliary.helpers import cached_organization
from mex.common.models import MergedPerson
from mex.common.types import MergedPersonIdentifier

if TYPE_CHECKING:
    from fastapi.testclient import TestClient


def test_get_merged_person_from_login(
    client_that_is_ldap_authenticated: TestClient,
) -> None:
    with (
        patch("mex.backend.security.ldap.initialize") as mock_initialize,
        patch("mex.backend.ldap.main.LDAPConnector.get") as mock_ldap_connector_get,
        patch("mex.backend.ldap.main.get_provider") as mock_get_provider,
        patch("mex.backend.ldap.main.get_merged_item") as mock_get_merged_item,
        patch(
            "mex.backend.ldap.main.cached_primary_source"
        ) as mock_cached_primary_source,
    ):
        mock_initialize.return_value = MagicMock()

        mock_ldap_connector = MagicMock()
        mock_ldap_connector.get_person.return_value = MagicMock(
            objectGUID="bFQoRhcVH5DHUI"
        )
        mock_ldap_connector_get.return_value = mock_ldap_connector
        mock_cached_primary_source.return_value = MagicMock(
            stableTargetId="mocked-primary-source-id"
        )
        mock_provider = MagicMock()
        mock_provider.fetch.side_effect = [
            [MagicMock(stableTargetId="bFQoRhcVH5DHUI")],
        ]
        mock_get_provider.return_value = mock_provider

        mock_person = MergedPerson(
            email=["person_1@example.com"],
            fullName=["Bernd, Brot"],
            identifier=MergedPersonIdentifier("bFQoRhcVH5DHUI"),
            entityType="MergedPerson",
        )
        mock_get_merged_item.return_value = mock_person

        response = client_that_is_ldap_authenticated.post(
            "/v0/merged-person-from-login",
        )
        assert response.status_code == 200
        result = response.json()
        assert result is not None
        assert result["identifier"] == "bFQoRhcVH5DHUI"
        assert result["email"] == ["person_1@example.com"]
        assert result["fullName"] == ["Bernd, Brot"]


@pytest.mark.usefixtures("mocked_ldap", "mocked_wikidata")
def test_search_persons_in_ldap(
    client_with_api_key_read_permission: TestClient,
) -> None:
    response = client_with_api_key_read_permission.get("/v0/ldap/persons")
    rki_organization = cached_organization(RKI_WIKIDATA_ID)
    assert response.status_code == status.HTTP_200_OK, response.text
    response_json = response.json()
    assert response_json["total"] == 3
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
def test_search_persons_in_ldap_with_filters(
    client_with_api_key_read_permission: TestClient,
) -> None:
    response = client_with_api_key_read_permission.get(
        "/v0/ldap/persons", params={"surname": "Mueller", "given_name": "Max"}
    )
    assert response.status_code == status.HTTP_200_OK, response.text
    response_json = response.json()
    assert response_json["total"] == 1
    assert response_json["items"][0]["givenName"] == ["Max"]
    assert response_json["items"][0]["familyName"] == ["Mueller"]


@pytest.mark.usefixtures("mocked_ldap", "mocked_wikidata")
def test_ldap_persons_pagination(
    client_with_api_key_read_permission: TestClient,
) -> None:
    all_response = client_with_api_key_read_permission.get("/v0/ldap/persons")
    assert all_response.status_code == status.HTTP_200_OK, all_response.text
    all_items = all_response.json()

    paginated_response = client_with_api_key_read_permission.get(
        "/v0/ldap/persons", params={"offset": 1, "limit": 1}
    )
    assert paginated_response.status_code == status.HTTP_200_OK, paginated_response.text
    paginated_items = paginated_response.json()

    assert paginated_items["total"] == all_items["total"]
    assert len(paginated_items["items"]) == 1
    assert paginated_items["items"] == all_items["items"][1:2]


@pytest.mark.usefixtures("mocked_ldap", "mocked_wikidata")
def test_search_functional_accounts_in_ldap(
    client_with_api_key_read_permission: TestClient,
) -> None:
    response = client_with_api_key_read_permission.get("/v0/ldap/functional-accounts")
    assert response.status_code == status.HTTP_200_OK, response.text
    response_json = response.json()
    assert response_json["total"] == 2
    info_item = next(
        item
        for item in response_json["items"]
        if item["email"] == ["info@mail.provider"]
    )
    assert info_item == {
        "hadPrimarySource": "ebs5siX85RkdrhBRlsYgRP",
        "identifierInPrimarySource": "00000000-0000-4000-8000-00000000021f",
        "email": ["info@mail.provider"],
        "$type": "ExtractedContactPoint",
        "identifier": info_item["identifier"],
        "stableTargetId": info_item["stableTargetId"],
    }


@pytest.mark.usefixtures("mocked_ldap", "mocked_wikidata")
def test_search_functional_accounts_in_ldap_with_filter(
    client_with_api_key_read_permission: TestClient,
) -> None:
    response = client_with_api_key_read_permission.get(
        "/v0/ldap/functional-accounts", params={"mail": "help@account.test"}
    )
    assert response.status_code == status.HTTP_200_OK, response.text
    response_json = response.json()
    assert response_json["total"] == 1
    assert response_json["items"][0]["email"] == ["help@account.test"]


@pytest.mark.usefixtures("mocked_ldap", "mocked_wikidata")
def test_ldap_functional_accounts_pagination(
    client_with_api_key_read_permission: TestClient,
) -> None:
    all_response = client_with_api_key_read_permission.get(
        "/v0/ldap/functional-accounts"
    )
    assert all_response.status_code == status.HTTP_200_OK, all_response.text
    all_items = all_response.json()

    paginated_response = client_with_api_key_read_permission.get(
        "/v0/ldap/functional-accounts", params={"offset": 1, "limit": 1}
    )
    assert paginated_response.status_code == status.HTTP_200_OK, paginated_response.text
    paginated_items = paginated_response.json()

    assert paginated_items["total"] == all_items["total"]
    assert len(paginated_items["items"]) == 1
    assert paginated_items["items"] == all_items["items"][1:2]
