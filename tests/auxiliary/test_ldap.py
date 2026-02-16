from typing import TYPE_CHECKING

import pytest
from starlette import status

from mex.backend.auxiliary.primary_source import extracted_primary_source_ldap
from mex.backend.auxiliary.wikidata import extracted_organization_rki
from mex.backend.extracted.helpers import search_extracted_items_in_graph
from mex.common.models import ExtractedPrimarySource
from mex.common.types import Identifier, TextLanguage
from tests.conftest import get_graph

if TYPE_CHECKING:
    from fastapi.testclient import TestClient


@pytest.mark.usefixtures("mocked_ldap", "mocked_wikidata")
def test_search_persons_in_ldap_mocked(
    client_with_api_key_read_permission: TestClient,
) -> None:
    response = client_with_api_key_read_permission.get("/v0/ldap", params={"q": "test"})
    rki_organization = extracted_organization_rki()
    assert response.status_code == status.HTTP_200_OK, response.text
    assert response.json() == {
        "items": [
            {
                "hadPrimarySource": "ebs5siX85RkdrhBRlsYgRP",
                "identifierInPrimarySource": "00000000-0000-4000-8000-00000000007b",
                "email": ["help@account.test"],
                "$type": "ExtractedContactPoint",
                "identifier": "gocAoWR0iUHruUOBoA1qdx",
                "stableTargetId": "ccBhefRZ7rI04yfpqZDG27",
            },
            {
                "hadPrimarySource": "ebs5siX85RkdrhBRlsYgRP",
                "identifierInPrimarySource": "00000000-0000-4000-8000-000000000141",
                "affiliation": [rki_organization.stableTargetId],
                "email": [],
                "familyName": ["Mueller"],
                "fullName": [],
                "givenName": ["Moritz"],
                "isniId": [],
                "memberOf": ["cjna2jitPngp6yIV63cdi9"],
                "orcidId": [],
                "$type": "ExtractedPerson",
                "identifier": "c6OEuSCqKQYzHNHPC96hEF",
                "stableTargetId": "e9KbtvmWDHuiNRgo3KhiBv",
            },
            {
                "hadPrimarySource": "ebs5siX85RkdrhBRlsYgRP",
                "identifierInPrimarySource": "00000000-0000-4000-8000-0000000001b0",
                "affiliation": [rki_organization.stableTargetId],
                "email": [],
                "familyName": ["Mueller"],
                "fullName": [],
                "givenName": ["Max"],
                "isniId": [],
                "memberOf": ["cjna2jitPngp6yIV63cdi9"],
                "orcidId": [],
                "$type": "ExtractedPerson",
                "identifier": "cg6JHD4TPjZMRtOHqORWKv",
                "stableTargetId": "DJx2H14ViIUC7mHPt4oAj",
            },
            {
                "hadPrimarySource": "ebs5siX85RkdrhBRlsYgRP",
                "identifierInPrimarySource": "00000000-0000-4000-8000-00000000021f",
                "email": ["info@mail.provider"],
                "$type": "ExtractedContactPoint",
                "identifier": "bjkwd4TcGgHzpHAJjGO3xe",
                "stableTargetId": "d9CQ7fQoLrAannxqFdksXR",
            },
            {
                "hadPrimarySource": "ebs5siX85RkdrhBRlsYgRP",
                "identifierInPrimarySource": "00000000-0000-4000-8000-000000000315",
                "affiliation": [rki_organization.stableTargetId],
                "email": [],
                "familyName": ["Example"],
                "fullName": [],
                "givenName": ["Moritz"],
                "isniId": [],
                "memberOf": ["cjna2jitPngp6yIV63cdi9"],
                "orcidId": [],
                "$type": "ExtractedPerson",
                "identifier": "hfhFIAgNwmVDXNvZLz9kBu",
                "stableTargetId": "dh9jSmVfRXmSCa74KIIHGh",
            },
        ],
        "total": 5,
    }


def test_extracted_primary_source_ldap() -> None:
    result = extracted_primary_source_ldap()
    assert isinstance(result, ExtractedPrimarySource)
    assert result.model_dump() == {
        "hadPrimarySource": "00000000000000",
        "identifierInPrimarySource": "ldap",
        "version": None,
        "alternativeTitle": [],
        "contact": [],
        "description": [],
        "documentation": [],
        "locatedAt": [],
        "title": [{"value": "Active Directory", "language": TextLanguage.EN}],
        "unitInCharge": [],
        "entityType": "ExtractedPrimarySource",
        "identifier": "cmiaN880A6fm1Ggno4kl7m",
        "stableTargetId": "ebs5siX85RkdrhBRlsYgRP",
    }


@pytest.mark.integration
def test_extracted_primary_source_ldap_ingest() -> None:
    # verify the primary source ldap has been stored in the database
    result = extracted_primary_source_ldap()

    ingested = search_extracted_items_in_graph(
        query_string="Active Directory",
        stable_target_id=Identifier(result.stableTargetId),
        entity_type=["ExtractedPrimarySource"],
    )

    assert ingested.total == 1, get_graph()
