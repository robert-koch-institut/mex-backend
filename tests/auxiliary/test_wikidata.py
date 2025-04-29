import pytest
from fastapi.testclient import TestClient
from pytest import MonkeyPatch

from mex.backend.auxiliary import wikidata
from mex.common.models import ExtractedPrimarySource
from mex.common.primary_source.helpers import get_all_extracted_primary_sources
from mex.common.testing import Joker


@pytest.mark.usefixtures("mocked_wikidata")
def test_search_organization_in_wikidata_mocked(
    client_with_api_key_read_permission: TestClient, monkeypatch: MonkeyPatch
) -> None:
    def extracted_primary_source_wikidata() -> ExtractedPrimarySource:
        return get_all_extracted_primary_sources()["wikidata"]

    monkeypatch.setattr(
        wikidata, "extracted_primary_source_wikidata", extracted_primary_source_wikidata
    )

    response = client_with_api_key_read_permission.get(
        "/v0/wikidata", params={"q": "Q679041"}
    )
    assert response.status_code == 200
    assert response.json() == {
        "items": [
            {
                "$type": "ExtractedOrganization",
                "alternativeName": Joker(),
                "geprisId": [],
                "gndId": [],
                "hadPrimarySource": "djbNGb5fLgYHFyMh3fZE2g",
                "identifier": "bfsbcyvNoo2ocmmqXiusmf",
                "identifierInPrimarySource": "Q679041",
                "isniId": ["https://isni.org/isni/0000000109403744"],
                "officialName": [
                    {"language": "en", "value": "Robert Koch Institute"},
                    {"language": "de", "value": "Robert Koch-Institut"},
                ],
                "rorId": ["https://ror.org/01k5qnb77"],
                "shortName": [
                    {"language": "de", "value": "RKI"},
                    {"language": "en", "value": "RKI"},
                ],
                "stableTargetId": "ga6xh6pgMwgq7DC7r6Wjqg",
                "viafId": [],
                "wikidataId": ["https://www.wikidata.org/entity/Q679041"],
            }
        ],
        "total": 1,
    }
