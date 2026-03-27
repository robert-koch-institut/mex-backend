from typing import TYPE_CHECKING
from unittest.mock import Mock, patch

import pytest
from starlette import status

from mex.backend.auxiliary.ldap import search_persons_or_contact_points_in_ldap
from mex.backend.auxiliary.primary_source import extracted_primary_source_ldap
from mex.backend.auxiliary.wikidata import extracted_organization_rki
from mex.backend.extracted.helpers import search_extracted_items_in_graph
from mex.common.ldap.connector import LDAPConnector
from mex.common.ldap.models import AnyLDAPActor
from mex.common.models import (
    ExtractedContactPoint,
    ExtractedOrganization,
    ExtractedOrganizationalUnit,
    ExtractedPrimarySource,
)
from mex.common.models.base.container import PaginatedItemsContainer
from mex.common.types import Identifier, MergedPrimarySourceIdentifier, TextLanguage
from tests.conftest import get_graph

if TYPE_CHECKING:  # pragma: no cover
    from fastapi.testclient import TestClient


@pytest.mark.usefixtures("mocked_ldap", "mocked_wikidata")
def test_search_persons_or_contact_points_in_ldap(
    client_with_api_key_read_permission: TestClient,
) -> None:
    response = client_with_api_key_read_permission.get("/v0/ldap", params={"q": "*"})
    rki_organization = extracted_organization_rki()
    assert response.status_code == status.HTTP_200_OK, response.text
    response_json = response.json()
    assert response_json["total"] == 5
    assert {
        "hadPrimarySource": "ebs5siX85RkdrhBRlsYgRP",
        "identifierInPrimarySource": "00000000-0000-4000-8000-000000000141",
        "affiliation": [rki_organization.stableTargetId],
        "email": [],
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


@pytest.mark.integration
def test_ldap_pagination() -> None:

    with (
        patch("mex.common.ldap.connector.LDAPConnector.get") as mock_ldap_connector_get,
        patch(
            "mex.backend.auxiliary.organigram.extracted_organizational_units"
        ) as mock_ext_org_units,
        patch(
            "mex.backend.auxiliary.primary_source.extracted_primary_source_ldap"
        ) as mock_ext_prim_src_ldap,
        patch(
            "mex.backend.auxiliary.wikidata.extracted_organization_rki"
        ) as mock_ext_org_rki,
        patch(
            "mex.common.ldap.transform.transform_any_ldap_actor_to_extracted_persons_or_contact_points"
        ) as mock_transform_ldap,
    ):
        mocked_ldap_connector = Mock(spec=LDAPConnector)
        mocked_ldap_connector.get_persons_or_functional_accounts.return_value = (
            PaginatedItemsContainer[AnyLDAPActor](items=[], total=0)
        )
        mock_ldap_connector_get.return_value = mocked_ldap_connector

        my_primary_source = MergedPrimarySourceIdentifier.generate()
        mock_ext_org_units.return_value = [
            ExtractedOrganizationalUnit(
                name="RKI",
                hadPrimarySource=my_primary_source,
                identifierInPrimarySource="RKI",
            )
        ]
        mock_ext_prim_src_ldap.return_value = ExtractedPrimarySource(
            hadPrimarySource=my_primary_source, identifierInPrimarySource="LDAP"
        )
        mock_ext_org_rki.return_value = ExtractedOrganization(
            officialName="RKI",
            identifierInPrimarySource="RKI",
            hadPrimarySource=my_primary_source,
        )
        mock_transform_ldap.return_value = [
            ExtractedContactPoint(
                identifierInPrimarySource="RKI",
                hadPrimarySource=my_primary_source,
                email=["rki@rki.de"],
            )
        ]

        search_persons_or_contact_points_in_ldap("query-string", 10, 10)
        mocked_ldap_connector.get_persons_or_functional_accounts.assert_called_once_with(
            query="query-string", offset=10, limit=10
        )
