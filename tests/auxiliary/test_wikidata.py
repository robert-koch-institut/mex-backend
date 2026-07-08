from typing import TYPE_CHECKING

import pytest
from starlette import status

if TYPE_CHECKING:  # pragma: no cover
    from fastapi.testclient import TestClient


@pytest.mark.usefixtures("mocked_wikidata")
def test_search_organization_in_wikidata_mocked(
    client_with_api_key_read_permission: TestClient,
) -> None:
    response = client_with_api_key_read_permission.get(
        "/v0/wikidata", params={"q": "Q679041"}
    )
    assert response.status_code == status.HTTP_200_OK, response.text
    assert response.json() == {
        "items": [
            {
                "$type": "ExtractedOrganization",
                "alternativeName": [
                    {
                        "language": None,
                        "value": "alias_en_1",
                    },
                    {
                        "language": None,
                        "value": "alias_en_2",
                    },
                    {
                        "language": None,
                        "value": "alias_en_3",
                    },
                    {
                        "language": None,
                        "value": "alias_en_4",
                    },
                    {
                        "language": None,
                        "value": "alias_de_1",
                    },
                    {
                        "language": None,
                        "value": "alias_de_2",
                    },
                    {
                        "language": None,
                        "value": "alias_de_3",
                    },
                ],
                "geprisId": [],
                "gndId": [],
                "hadPrimarySource": "djbNGb5fLgYHFyMh3fZE2g",
                "identifier": "bfsbcyvNoo2ocmmqXiusmf",
                "identifierInPrimarySource": "Q679041",
                "isniId": ["https://isni.org/isni/0000000109403744"],
                "officialName": [
                    {"language": "de", "value": "Robert Koch-Institut"},
                ],
                "rorId": ["https://ror.org/01k5qnb77"],
                "shortName": [
                    {"language": "en", "value": "RKI"},
                    {"language": "de", "value": "RKI"},
                ],
                "stableTargetId": "ga6xh6pgMwgq7DC7r6Wjqg",
                "viafId": [],
                "wikidataId": ["http://www.wikidata.org/entity/Q679041"],
            }
        ],
        "total": 1,
    }
