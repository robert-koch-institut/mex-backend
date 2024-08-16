from typing import Any

import pytest
from fastapi.testclient import TestClient


@pytest.mark.parametrize(
    ("stable_target_id", "json_body", "expected"),
    [
        (
            "bFQoRhcVH5DHUB",
            {
                "additive": {
                    "$type": "AdditiveActivity",
                    "start": ["2025"],
                    "title": [{"value": "A new beginning", "language": "en"}],
                },
                "preventive": {"$type": "PreventiveActivity"},
                "subtractive": {"$type": "SubtractiveActivity"},
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
                "start": ["2025"],
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
            "bFQoRhcVH5DHUB",
            {
                "additive": {"$type": "AdditiveActivity"},
                "preventive": {"$type": "PreventiveActivity"},
                "subtractive": {
                    "$type": "SubtractiveActivity",
                    "start": ["2025"],
                    "contact": ["bFQoRhcVH5DHUv"],
                    "abstract": [
                        {"value": "Une activité active.", "language": None},
                    ],
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
            "bFQoRhcVH5DHUB",
            {
                "additive": {
                    "$type": "AdditiveActivity",
                },
                "preventive": {
                    "$type": "PreventiveActivity",
                    "theme": ["bFQoRhcVH5DHUr"],
                    "title": ["bFQoRhcVH5DHUt"],
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
            "unknownStableTargetId",
            {
                "additive": {
                    "$type": "AdditiveContactPoint",
                    "email": ["test@test.local"],
                },
                "preventive": {"$type": "PreventiveContactPoint"},
                "subtractive": {"$type": "SubtractiveContactPoint"},
            },
            {
                "$type": "MergedContactPoint",
                "email": ["test@test.local"],
                "identifier": "unknownStableTargetId",
            },
        ),
    ],
    ids=[
        "additive",
        "subtractive",
        "preventive",
        "unknown-id",
    ],
)
@pytest.mark.integration
@pytest.mark.usefixtures("load_dummy_data")
def test_preview(
    stable_target_id: str,
    client_with_api_key_read_permission: TestClient,
    json_body: dict[str, Any],
    expected: dict[str, Any],
) -> None:
    response = client_with_api_key_read_permission.post(
        f"/v0/preview-item/{stable_target_id}", json=json_body
    )
    assert response.status_code == 200, response.text
    cleaned_response = {k: v for k, v in response.json().items() if v}
    assert cleaned_response == expected
