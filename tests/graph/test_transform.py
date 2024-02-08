from mex.backend.graph.transform import transform_search_result_to_model


def test_transform_search_result_to_model() -> None:
    node_dict = {
        "_refs": [
            {"label": "responsibleUnit", "position": 0, "value": "bFQoRhcVH5DHUz"},
            {"label": "contact", "position": 2, "value": "bFQoRhcVH5DHUz"},
            {"label": "contact", "position": 0, "value": "bFQoRhcVH5DHUv"},
            {"label": "contact", "position": 1, "value": "bFQoRhcVH5DHUx"},
            {"label": "hadPrimarySource", "position": 0, "value": "bFQoRhcVH5DHUr"},
            {"label": "stableTargetId", "position": 0, "value": "bFQoRhcVH5DHUB"},
            {
                "label": "website",
                "position": 0,
                "value": {"title": "Activity Homepage", "url": "https://activity-1"},
            },
            {
                "label": "abstract",
                "position": 1,
                "value": {"value": "Une activité active."},
            },
            {
                "label": "title",
                "position": 0,
                "value": {"language": "de", "value": "Aktivität 1"},
            },
            {
                "label": "abstract",
                "position": 0,
                "value": {"language": "en", "value": "An active activity."},
            },
        ],
        "activityType": [],
        "end": [],
        "entityType": "ExtractedActivity",
        "fundingProgram": [],
        "identifier": "bFQoRhcVH5DHUA",
        "identifierInPrimarySource": "a-1",
        "start": [],
        "theme": ["https://mex.rki.de/item/theme-3"],
    }

    transform_search_result_to_model(node_dict)

    assert node_dict == {
        "activityType": [],
        "end": [],
        "entityType": "ExtractedActivity",
        "fundingProgram": [],
        "identifier": "bFQoRhcVH5DHUA",
        "identifierInPrimarySource": "a-1",
        "start": [],
        "theme": ["https://mex.rki.de/item/theme-3"],
        "responsibleUnit": ["bFQoRhcVH5DHUz"],
        "contact": ["bFQoRhcVH5DHUv", "bFQoRhcVH5DHUx", "bFQoRhcVH5DHUz"],
        "hadPrimarySource": ["bFQoRhcVH5DHUr"],
        "stableTargetId": ["bFQoRhcVH5DHUB"],
        "website": [{"title": "Activity Homepage", "url": "https://activity-1"}],
        "abstract": [
            {"language": "en", "value": "An active activity."},
            {"value": "Une activité active."},
        ],
        "title": [{"language": "de", "value": "Aktivität 1"}],
    }
