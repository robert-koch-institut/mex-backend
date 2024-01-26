import pytest

from mex.backend.graph.connector import GraphConnector
from mex.backend.graph.transform import (
    transform_search_result_to_model,
)
from mex.common.models import (
    AnyExtractedModel,
)
from mex.common.types import TextLanguage


@pytest.mark.integration
def test_transform_search_result_to_model(
    load_dummy_data: list[AnyExtractedModel],
) -> None:
    connector = GraphConnector.get()
    result = connector.query_nodes(None, None, None, 0, 100)
    models = [transform_search_result_to_model(i) for i in result.data[0]["items"]]
    assert [m.model_dump() for m in models] == [
        {
            "identifier": "00000000000000",
            "hadPrimarySource": "00000000000000",
            "identifierInPrimarySource": "mex",
            "stableTargetId": "00000000000000",
            "alternativeTitle": [],
            "contact": [],
            "description": [],
            "documentation": [],
            "locatedAt": [],
            "title": [{"value": "Metadata Exchange", "language": TextLanguage.EN}],
            "unitInCharge": [],
            "version": None,
            "entityType": "ExtractedPrimarySource",
        },
        *[m.model_dump() for m in sorted(load_dummy_data, key=lambda x: x.identifier)],
    ]
