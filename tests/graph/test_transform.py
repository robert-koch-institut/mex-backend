from mex.backend.graph.transform import (
    _SearchResultReference,
    expand_references_in_search_result,
    transform_edges_into_expectations_by_edge_locator,
)


def test_expand_references_in_search_result() -> None:
    refs = [
        _SearchResultReference(
            {"label": "responsibleUnit", "position": 0, "value": "bFQoRhcVH5DHUz"}
        ),
        _SearchResultReference(
            {"label": "contact", "position": 2, "value": "bFQoRhcVH5DHUz"}
        ),
        _SearchResultReference(
            {"label": "contact", "position": 0, "value": "bFQoRhcVH5DHUv"}
        ),
        _SearchResultReference(
            {"label": "contact", "position": 1, "value": "bFQoRhcVH5DHUx"}
        ),
        _SearchResultReference(
            {"label": "hadPrimarySource", "position": 0, "value": "bFQoRhcVH5DHUr"}
        ),
        _SearchResultReference(
            {"label": "stableTargetId", "position": 0, "value": "bFQoRhcVH5DHUB"}
        ),
        _SearchResultReference(
            {
                "label": "website",
                "position": 0,
                "value": {"title": "Activity Homepage", "url": "https://activity-1"},
            }
        ),
        _SearchResultReference(
            {
                "label": "abstract",
                "position": 1,
                "value": {"value": "Une activité active."},
            }
        ),
        _SearchResultReference(
            {
                "label": "title",
                "position": 0,
                "value": {"language": "de", "value": "Aktivität 1"},
            }
        ),
        _SearchResultReference(
            {
                "label": "abstract",
                "position": 0,
                "value": {"language": "en", "value": "An active activity."},
            }
        ),
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


def test_transform_edges_into_expectations_by_edge_locator() -> None:
    expectations = transform_edges_into_expectations_by_edge_locator(
        "NodeLabel",
        ["edgeLabelFoo", "edgeLabelBar"],
        ["fooID", "barID"],
        [0, 73],
    )
    assert expectations == {
        "edgeLabelFoo {position: 0}": '(:NodeLabel)-[:edgeLabelFoo {position: 0}]->({identifier: "fooID"})',
        "edgeLabelBar {position: 73}": '(:NodeLabel)-[:edgeLabelBar {position: 73}]->({identifier: "barID"})',
    }
