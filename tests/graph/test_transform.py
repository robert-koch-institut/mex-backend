from mex.backend.graph.transform import expand_references_in_search_result


def test_expand_references_in_search_result() -> None:
    refs = [
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
    ]

    expanded = expand_references_in_search_result(refs)

    assert expanded == {
        "abstract": [
            {"language": "en", "value": "An active activity."},
            {"value": "Une activité active."},
        ],
        "contact": ["bFQoRhcVH5DHUv", "bFQoRhcVH5DHUx", "bFQoRhcVH5DHUz"],
        "hadPrimarySource": ["bFQoRhcVH5DHUr"],
        "responsibleUnit": ["bFQoRhcVH5DHUz"],
        "stableTargetId": ["bFQoRhcVH5DHUB"],
        "title": [{"language": "de", "value": "Aktivität 1"}],
        "website": [{"title": "Activity Homepage", "url": "https://activity-1"}],
    }
