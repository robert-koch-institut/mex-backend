from typing import Any

import pytest
from fastapi.testclient import TestClient

from mex.common.models import AnyExtractedModel


@pytest.mark.parametrize(
    ("json_body", "expected"),
    [
        (
            {
                "additive": {
                    "$type": "AdditiveActivity",
                    "start": ["2025"],
                    "title": [{"value": "A new beginning", "language": "en"}],
                },
                "preventive": {
                    "$type": "PreventiveActivity",
                },
                "subtractive": {
                    "$type": "SubtractiveActivity",
                },
            },
            {
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
            },
        ),
        (
            {
                "additive": {"$type": "AdditiveActivity"},
                "preventive": {
                    "$type": "PreventiveActivity",
                },
                "subtractive": {
                    "$type": "SubtractiveActivity",
                    "start": ["2025"],
                    "contact": ["bFQoRhcVH5DHUv"],
                    "abstract": [{"value": "Une activité active.", "language": None}],
                },
            },
            {
                "$type": "MergedActivity",
                "contact": ["bFQoRhcVH5DHUx", "bFQoRhcVH5DHUz"],
                "responsibleUnit": ["bFQoRhcVH5DHUz"],
                "title": [
                    {"value": "Aktivität 1", "language": "de"},
                ],
                "abstract": [
                    {"value": "An active activity.", "language": "en"},
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
                "start": [],
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
            },
        ),
        (
            {
                "additive": {"$type": "AdditiveActivity"},
                "preventive": {
                    "$type": "PreventiveActivity",
                    "theme": ["bFQoRhcVH5DHUr"],
                },
                "subtractive": {
                    "$type": "SubtractiveActivity",
                },
            },
            {
                "$type": "MergedActivity",
                "contact": ["bFQoRhcVH5DHUv", "bFQoRhcVH5DHUx", "bFQoRhcVH5DHUz"],
                "responsibleUnit": ["bFQoRhcVH5DHUz"],
                "title": [
                    {"value": "Aktivität 1", "language": "de"},
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
                "start": [],
                "succeeds": [],
                "theme": [],
                "website": [
                    {
                        "language": None,
                        "title": "Activity Homepage",
                        "url": "https://activity-1",
                    }
                ],
                "identifier": "bFQoRhcVH5DHUB",
            },
        ),
    ],
    ids=["additive", "subtractive", "preventive"],
)
@pytest.mark.integration
def test_preview(
    client_with_api_key_read_permission: TestClient,
    load_dummy_data: list[AnyExtractedModel],
    json_body: dict[str, Any],
    expected: dict[str, Any],
) -> None:
    response = client_with_api_key_read_permission.post(
        f"/v0/preview-item/{load_dummy_data[-1].stableTargetId}", json=json_body
    )
    assert response.status_code == 200, response.text
    assert response.json() == expected


@pytest.mark.integration
def test_preview_not_found(
    client_with_api_key_read_permission: TestClient,
) -> None:
    response = client_with_api_key_read_permission.post(
        "/v0/preview-item/thisIdDoesNotExist",
        json={
            "additive": {"$type": "AdditiveActivity"},
            "preventive": {
                "$type": "PreventiveActivity",
            },
            "subtractive": {
                "$type": "SubtractiveActivity",
            },
        },
    )
    assert response.status_code == 404, response.text