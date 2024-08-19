from pydantic import BaseModel

from mex.backend.graph.transform import expand_references_in_search_result, to_primitive
from mex.common.types import APIType, MergedActivityIdentifier, YearMonth


def test_expand_references_in_search_result() -> None:
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

    expand_references_in_search_result(node_dict)

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


def test_to_primitive() -> None:
    class Test(BaseModel):
        api: list[APIType] = [APIType["RPC"]]
        activity: MergedActivityIdentifier = MergedActivityIdentifier.generate(seed=99)
        month: YearMonth = YearMonth(2005, 11)

    assert to_primitive(Test()) == {
        "api": ["https://mex.rki.de/item/api-type-5"],
        "activity": "bFQoRhcVH5DHV1",
        "month": "2005-11",
    }
