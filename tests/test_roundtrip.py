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
    result = connector.query_nodes(None, None, None, 0, len(seeded_models))

    extracted_model_parser = TypeAdapter(
        list[Annotated[AnyExtractedModel, Field(discriminator="entityType")]]
    ).validate_python

    assert extracted_model_parser(result["items"]) == list(
        sorted(seeded_models, key=lambda x: x.identifier)
    )
