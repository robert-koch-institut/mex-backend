from typing import Annotated

import pytest
from pydantic import Field, TypeAdapter

from mex.backend.graph.connector import MEX_EXTRACTED_PRIMARY_SOURCE, GraphConnector
from mex.common.models import AnyExtractedModel


@pytest.mark.integration
def test_graph_ingest_and_query_roundtrip(
    load_dummy_data: list[AnyExtractedModel],
) -> None:
    seeded_models = [*load_dummy_data, MEX_EXTRACTED_PRIMARY_SOURCE]

    connector = GraphConnector.get()
    result = connector.fetch_extracted_data(None, None, None, 0, len(seeded_models))

    extracted_model_adapter = TypeAdapter(
        list[Annotated[AnyExtractedModel, Field(discriminator="entityType")]]
    )

    expected = sorted(seeded_models, key=lambda x: x.identifier)
    fetched = extracted_model_adapter.validate_python(result["items"])

    assert fetched == expected
