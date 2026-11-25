from typing import TYPE_CHECKING, Any

import pytest
from starlette import status

from mex.backend.auxiliary.constants import LDAP_PRIMARY_SOURCE_NAME, RKI_WIKIDATA_ID
from mex.backend.auxiliary.helpers import (
    cached_organization,
    cached_organizational_units,
    cached_primary_source,
)
from mex.backend.main import app
from mex.common.testing import Joker

if TYPE_CHECKING:
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


@pytest.mark.parametrize("offset", [5, 6], ids=["offset-at-total", "offset-past-total"])
@pytest.mark.usefixtures("mocked_ldap", "mocked_wikidata")
def test_ldap_pagination_skip_past_last_result(
    client_with_api_key_read_permission: TestClient, offset: int
) -> None:
    response = client_with_api_key_read_permission.get(
        "/v0/ldap", params={"q": "*", "offset": offset, "limit": 1}
    )
    assert response.status_code == status.HTTP_200_OK, response.text
    assert response.json() == {"items": [], "total": 5}


@pytest.mark.usefixtures("mocked_ldap", "mocked_wikidata")
def test_search_persons_in_ldap(
    client_with_api_key_read_permission: TestClient,
) -> None:
    response = client_with_api_key_read_permission.post(
        "/v0/ldap/_search", json={"entityType": ["ExtractedPerson"]}
    )
    rki_organization = cached_organization(RKI_WIKIDATA_ID)
    ldap_primary_source = cached_primary_source(LDAP_PRIMARY_SOURCE_NAME)
    unit_fg99 = next(
        unit
        for unit in cached_organizational_units()
        if unit.identifierInPrimarySource == "fg99"
    )
    assert response.status_code == status.HTTP_200_OK, response.text
    response_json = response.json()
    assert response_json["total"] == 3
    assert {item["$type"] for item in response_json["items"]} == {"ExtractedPerson"}
    assert {
        "hadPrimarySource": ldap_primary_source.stableTargetId,
        "identifierInPrimarySource": "00000000-0000-4000-8000-000000000141",
        "affiliation": [rki_organization.stableTargetId],
        "email": ["MoritzM@ldapmock.local"],
        "familyName": ["Mueller"],
        "fullName": ["Moritz Mueller"],
        "givenName": ["Moritz"],
        "isniId": [],
        "memberOf": [unit_fg99.stableTargetId],
        "orcidId": [],
        "$type": "ExtractedPerson",
        "identifier": "c6OEuSCqKQYzHNHPC96hEF",
        "stableTargetId": "e9KbtvmWDHuiNRgo3KhiBv",
    } in response_json["items"]


@pytest.mark.parametrize(
    ("filters", "expected_full_names"),
    [
        pytest.param(
            {"surname": "Mueller"}, ["Max Mueller", "Moritz Mueller"], id="sn"
        ),
        pytest.param({"employeeId": "def"}, ["Moritz Example"], id="employee-id"),
        pytest.param({"mail": "MaxM@ldapmock.local"}, ["Max Mueller"], id="mail-exact"),
        pytest.param(
            {"surname": "Mueller", "givenName": "Moritz"},
            ["Moritz Mueller"],
            id="combined",
        ),
        pytest.param(
            {"displayName": "Moritz*"},
            ["Moritz Example", "Moritz Mueller"],
            id="wildcard-suffix",
        ),
        pytest.param(
            {"mail": "*E@ldapmock.local"}, ["Moritz Example"], id="wildcard-prefix"
        ),
        pytest.param({"surname": "Mueller", "givenName": "Nobody"}, [], id="no-match"),
    ],
)
@pytest.mark.usefixtures("mocked_ldap", "mocked_wikidata")
def test_search_persons_in_ldap_with_filters(
    client_with_api_key_read_permission: TestClient,
    filters: dict[str, str],
    expected_full_names: list[str],
) -> None:
    response = client_with_api_key_read_permission.post(
        "/v0/ldap/_search",
        json={"entityType": ["ExtractedPerson"], **filters},
    )
    assert response.status_code == status.HTTP_200_OK, response.text
    response_json = response.json()
    assert response_json["total"] == len(expected_full_names)
    assert (
        sorted(name for item in response_json["items"] for name in item["fullName"])
        == expected_full_names
    )


@pytest.mark.usefixtures("mocked_ldap", "mocked_wikidata")
def test_ldap_persons_pagination(
    client_with_api_key_read_permission: TestClient,
) -> None:
    all_response = client_with_api_key_read_permission.post(
        "/v0/ldap/_search", json={"entityType": ["ExtractedPerson"]}
    )
    assert all_response.status_code == status.HTTP_200_OK, all_response.text
    all_items = all_response.json()

    paginated_response = client_with_api_key_read_permission.post(
        "/v0/ldap/_search",
        json={"entityType": ["ExtractedPerson"], "skip": 1, "limit": 1},
    )
    assert paginated_response.status_code == status.HTTP_200_OK, paginated_response.text
    paginated_items = paginated_response.json()

    assert paginated_items["total"] == all_items["total"]
    assert paginated_items["items"] == all_items["items"][1:2]


@pytest.mark.parametrize("skip", [3, 4], ids=["skip-at-total", "skip-past-total"])
@pytest.mark.usefixtures("mocked_ldap", "mocked_wikidata")
def test_ldap_persons_skip_past_last_result(
    client_with_api_key_read_permission: TestClient, skip: int
) -> None:
    response = client_with_api_key_read_permission.post(
        "/v0/ldap/_search",
        json={"entityType": ["ExtractedPerson"], "skip": skip, "limit": 1},
    )
    assert response.status_code == status.HTTP_200_OK, response.text
    assert response.json() == {"items": [], "total": 3}


@pytest.mark.parametrize(
    "body",
    [
        pytest.param(["ExtractedPerson"], id="body-not-an-object"),
        pytest.param({}, id="missing-entity-type"),
        pytest.param({"entityType": []}, id="empty-entity-type"),
        pytest.param({"entityType": "ExtractedPerson"}, id="scalar-entity-type"),
        pytest.param({"entityType": ["MergedPerson"]}, id="unknown-entity-type"),
        pytest.param(
            {"entityType": ["ExtractedPerson", "ExtractedPerson"]},
            id="repeated-entity-type",
        ),
        pytest.param(
            {"entityType": ["ExtractedPerson"], "sn": "Mueller"}, id="unknown-field"
        ),
        pytest.param(
            {"entityType": ["ExtractedPerson"], "surname": "x" * 1001},
            id="value-too-long",
        ),
        pytest.param({"entityType": ["ExtractedPerson"], "limit": 0}, id="limit-0"),
        pytest.param({"entityType": ["ExtractedPerson"], "limit": 101}, id="limit-101"),
        pytest.param({"entityType": ["ExtractedPerson"], "skip": -1}, id="skip-neg"),
    ],
)
def test_search_in_ldap_invalid_body(
    client_with_api_key_read_permission: TestClient, body: object
) -> None:
    response = client_with_api_key_read_permission.post("/v0/ldap/_search", json=body)
    assert response.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT, response.text


@pytest.mark.parametrize(
    ("headers", "expected_status"),
    [
        pytest.param({}, status.HTTP_401_UNAUTHORIZED, id="no-key"),
        pytest.param(
            {"X-API-Key": "wrong_key"}, status.HTTP_403_FORBIDDEN, id="wrong-key"
        ),
    ],
)
def test_search_in_ldap_requires_read_access(
    client: TestClient, headers: dict[str, str], expected_status: int
) -> None:
    client.headers.update(headers)
    response = client.post("/v0/ldap/_search", json={"entityType": ["ExtractedPerson"]})
    assert response.status_code == expected_status, response.text


def test_search_in_ldap_is_documented_as_auxiliary() -> None:
    operation = app.openapi()["paths"]["/v0/ldap/_search"]["post"]
    assert operation["tags"] == ["auxiliary"]


@pytest.mark.usefixtures("mocked_ldap", "mocked_wikidata")
def test_search_functional_accounts_in_ldap(
    client_with_api_key_read_permission: TestClient,
) -> None:
    response = client_with_api_key_read_permission.post(
        "/v0/ldap/_search", json={"entityType": ["ExtractedContactPoint"]}
    )
    ldap_primary_source = cached_primary_source(LDAP_PRIMARY_SOURCE_NAME)
    assert response.status_code == status.HTTP_200_OK, response.text
    response_json = response.json()
    assert response_json["total"] == 2
    assert {item["$type"] for item in response_json["items"]} == {
        "ExtractedContactPoint"
    }
    info_item = next(
        item
        for item in response_json["items"]
        if item["email"] == ["info@mail.provider"]
    )
    assert info_item == {
        "hadPrimarySource": ldap_primary_source.stableTargetId,
        "identifierInPrimarySource": "00000000-0000-4000-8000-00000000021f",
        "email": ["info@mail.provider"],
        "$type": "ExtractedContactPoint",
        "identifier": Joker(),
        "stableTargetId": Joker(),
    }


@pytest.mark.parametrize(
    ("mail", "expected_emails"),
    [
        pytest.param("help@account.test", ["help@account.test"], id="exact"),
        pytest.param("HELP@account.test", ["help@account.test"], id="case-insensitive"),
        pytest.param("info@*", ["info@mail.provider"], id="wildcard-suffix"),
        pytest.param(
            "*@*", ["help@account.test", "info@mail.provider"], id="wildcard-both"
        ),
        pytest.param("nobody@account.test", [], id="no-match"),
    ],
)
@pytest.mark.usefixtures("mocked_ldap", "mocked_wikidata")
def test_search_functional_accounts_in_ldap_with_filter(
    client_with_api_key_read_permission: TestClient,
    mail: str,
    expected_emails: list[str],
) -> None:
    response = client_with_api_key_read_permission.post(
        "/v0/ldap/_search",
        json={"entityType": ["ExtractedContactPoint"], "mail": mail},
    )
    assert response.status_code == status.HTTP_200_OK, response.text
    response_json = response.json()
    assert response_json["total"] == len(expected_emails)
    assert (
        sorted(email for item in response_json["items"] for email in item["email"])
        == expected_emails
    )


@pytest.mark.usefixtures("mocked_ldap", "mocked_wikidata")
def test_ldap_functional_accounts_pagination(
    client_with_api_key_read_permission: TestClient,
) -> None:
    all_response = client_with_api_key_read_permission.post(
        "/v0/ldap/_search", json={"entityType": ["ExtractedContactPoint"]}
    )
    assert all_response.status_code == status.HTTP_200_OK, all_response.text
    all_items = all_response.json()

    paginated_response = client_with_api_key_read_permission.post(
        "/v0/ldap/_search",
        json={"entityType": ["ExtractedContactPoint"], "skip": 1, "limit": 1},
    )
    assert paginated_response.status_code == status.HTTP_200_OK, paginated_response.text
    paginated_items = paginated_response.json()

    assert paginated_items["total"] == all_items["total"]
    assert paginated_items["items"] == all_items["items"][1:2]


@pytest.mark.parametrize("skip", [2, 3], ids=["skip-at-total", "skip-past-total"])
@pytest.mark.usefixtures("mocked_ldap", "mocked_wikidata")
def test_ldap_functional_accounts_skip_past_last_result(
    client_with_api_key_read_permission: TestClient, skip: int
) -> None:
    response = client_with_api_key_read_permission.post(
        "/v0/ldap/_search",
        json={"entityType": ["ExtractedContactPoint"], "skip": skip, "limit": 1},
    )
    assert response.status_code == status.HTTP_200_OK, response.text
    assert response.json() == {"items": [], "total": 2}


@pytest.mark.parametrize(
    "body",
    [
        pytest.param(
            {"entityType": ["ExtractedContactPoint"], "surname": "Mueller"},
            id="person-field-in-functional-account-lookup",
        ),
        pytest.param(
            {"entityType": ["ExtractedContactPoint"], "displayName": "*"},
            id="person-field-with-wildcard-in-functional-account-lookup",
        ),
        pytest.param(
            {"entityType": ["ExtractedContactPoint"], "email": "help@account.test"},
            id="unknown-field",
        ),
        pytest.param(
            {"entityType": ["ExtractedContactPoint", "ExtractedPerson"]},
            id="both-entity-types",
        ),
        pytest.param(
            {"entityType": ["ExtractedContactPoint"], "mail": "x" * 1001},
            id="value-too-long",
        ),
        pytest.param(
            {"entityType": ["ExtractedContactPoint"], "limit": 0}, id="limit-0"
        ),
        pytest.param(
            {"entityType": ["ExtractedContactPoint"], "limit": 101}, id="limit-101"
        ),
        pytest.param(
            {"entityType": ["ExtractedContactPoint"], "skip": -1}, id="skip-neg"
        ),
    ],
)
def test_search_functional_accounts_in_ldap_invalid_body(
    client_with_api_key_read_permission: TestClient, body: dict[str, Any]
) -> None:
    response = client_with_api_key_read_permission.post("/v0/ldap/_search", json=body)
    assert response.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT, response.text


def test_search_in_ldap_documents_both_lookups() -> None:
    schema = app.openapi()
    operation = schema["paths"]["/v0/ldap/_search"]["post"]
    body_schema = operation["requestBody"]["content"]["application/json"]["schema"]
    shapes = body_schema.get("oneOf") or body_schema.get("anyOf")
    assert sorted(shape["$ref"].rsplit("/", 1)[-1] for shape in shapes) == [
        "LDAPFunctionalAccountLookup",
        "LDAPPersonLookup",
    ]
