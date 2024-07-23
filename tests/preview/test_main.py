import pytest
from fastapi.testclient import TestClient

from mex.common.models import AnyExtractedModel


@pytest.mark.integration
def test_preview(
    client_with_api_key_read_permission: TestClient,
    load_dummy_data: list[AnyExtractedModel],
) -> None:
    response = client_with_api_key_read_permission.post(
        f"/v0/preview-item/{load_dummy_data[-1].stableTargetId}",
        json={
            "$type": "AdditiveActivity",
            "start": ["2025"],
            "title": [{"value": "A new beginning", "language": "en"}],
        },
    )
    assert response.status_code == 200, response.text

    assert response.json() == {
        "$type": "MergedActivity",
        "contact": ["bFQoRhcVH5DHUv", "bFQoRhcVH5DHUx", "bFQoRhcVH5DHUz"],
        "responsibleUnit": ["bFQoRhcVH5DHUz"],
        "title": [
            {"value": "Aktivität 1", "language": "de"},
            {"value": "A new beginning", "language": "en"},
        ],
        "abstract": [
            {"value": "An active activity.", "language": "en"},
            {"value": "Une activité active.", "language": None},
        ],
        "activityType": [],
        "alternativeTitle": [],
        "documentation": [],
        "end": [],
        "externalAssociate": [],
        "funderOrCommissioner": [],
        "fundingProgram": [],
        "involvedPerson": [],
        "involvedUnit": [],
        "isPartOfActivity": [],
        "publication": [],
        "shortName": [],
        "start": ["2025"],
        "succeeds": [],
        "theme": ["https://mex.rki.de/item/theme-3"],
        "website": [
            {
                "language": None,
                "title": "Activity Homepage",
                "url": "https://activity-1",
            }
        ],
        "identifier": "bFQoRhcVH5DHUB",
    }
