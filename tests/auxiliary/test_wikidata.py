import pytest
from fastapi.testclient import TestClient
from pytest import MonkeyPatch

from mex.backend.auxiliary import wikidata
from mex.backend.auxiliary.wikidata import extracted_primary_source_wikidata
from mex.backend.graph.connector import GraphConnector
from mex.common.models import ExtractedPrimarySource
from mex.common.types import Text


@pytest.mark.integration
def test_extracted_primary_source_wikidata() -> None:
    primary_source = extracted_primary_source_wikidata()
    assert primary_source.identifierInPrimarySource == "wikidata"

    # verify the primary source wikidata has been stored in the database
    graph = GraphConnector.get()
    result = graph.fetch_extracted_items(
        "wikidata",
        str(primary_source.stableTargetId),
        ["ExtractedPrimarySource"],
        0,
        100,
    )
    assert result["total"] == 1


@pytest.mark.usefixtures("mocked_wikidata")
def test_search_organization_in_wikidata_mocked(
    client_with_api_key_read_permission: TestClient, monkeypatch: MonkeyPatch
) -> None:
    def extracted_primary_source_wikidata() -> ExtractedPrimarySource:
        return ExtractedPrimarySource(
            hadPrimarySource="00000000000000",
            identifierInPrimarySource="wikidata",
            title=[Text(value="Wikidata", language=None)],
            entityType="ExtractedPrimarySource",
        )

    monkeypatch.setattr(
        wikidata, "extracted_primary_source_wikidata", extracted_primary_source_wikidata
    )

    expected_total = 3
    expected_organization_identifier = "Q679041"
    expected_organization_official_name = [
        {"value": "Robert Koch Institute", "language": "en"},
        {"value": "Robert Koch-Institut", "language": "de"},
    ]
    organizations = client_with_api_key_read_permission.get(
        "/v0/wikidata", params={"q": "rki"}
    ).json()

    assert organizations["total"] == expected_total
    assert (
        organizations["items"][0]["identifierInPrimarySource"]
        == expected_organization_identifier
    )
    assert (
        organizations["items"][0]["officialName"] == expected_organization_official_name
    )
