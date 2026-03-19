from unittest.mock import Mock

import pytest

from mex.backend.graph.models import GraphRel, IngestData
from mex.backend.graph.transform import (
    _SearchResultReference,
    expand_references_in_search_result,
    get_error_details_from_neo4j_error,
    validate_ingested_data,
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
                "value": {"value": "Eng aktiv Aktivitéit."},
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
            {"value": "Eng aktiv Aktivitéit."},
        ],
        "contact": ["bFQoRhcVH5DHUv", "bFQoRhcVH5DHUx", "bFQoRhcVH5DHUz"],
        "hadPrimarySource": ["bFQoRhcVH5DHUr"],
        "responsibleUnit": ["bFQoRhcVH5DHUz"],
        "stableTargetId": ["bFQoRhcVH5DHUB"],
        "title": [{"language": "de", "value": "Aktivität 1"}],
        "website": [{"title": "Activity Homepage", "url": "https://activity-1"}],
    }


def _make_ingest_data(**overrides: object) -> IngestData:
    """Create an IngestData with sensible defaults, applying overrides."""
    defaults: dict[str, object] = {
        "stableTargetId": "stid-1",
        "identifier": "id-1",
        "entityType": "ExtractedActivity",
        "nodeProps": {"title": "t"},
        "linkRels": [],
        "createRels": [],
    }
    defaults.update(overrides)
    return IngestData(**defaults)  # type: ignore[arg-type]


def test_validate_ingested_data_happy_path() -> None:
    data = _make_ingest_data()
    assert validate_ingested_data(data, data) == []


@pytest.mark.parametrize(
    "field",
    ["stableTargetId", "identifier", "entityType", "nodeProps"],
)
def test_validate_ingested_data_field_mismatch(field: str) -> None:
    data_in = _make_ingest_data()
    different_values = {
        "stableTargetId": "stid-other",
        "identifier": "id-other",
        "entityType": "ExtractedOther",
        "nodeProps": {"title": "other"},
    }
    data_out = _make_ingest_data(**{field: different_values[field]})

    errors = validate_ingested_data(data_in, data_out)
    assert len(errors) == 1
    assert errors[0]["loc"] == (field,)
    assert errors[0]["msg"] == "ingested data did not match expectation"


def test_validate_ingested_data_unexpected_relation() -> None:
    data_in = _make_ingest_data()
    extra_rel = GraphRel(
        edgeLabel="hadPrimarySource",
        edgeProps={"position": 0},
        nodeLabels=["MergedPrimarySource"],
        nodeProps={"identifier": "ps-1"},
    )
    data_out = _make_ingest_data(linkRels=[extra_rel])

    errors = validate_ingested_data(data_in, data_out)
    assert len(errors) == 1
    assert errors[0]["msg"] == "ingestion would have created unexpected relation"


def test_validate_ingested_data_missing_expected_relation() -> None:
    expected_rel = GraphRel(
        edgeLabel="hadPrimarySource",
        edgeProps={"position": 0},
        nodeLabels=["MergedPrimarySource"],
        nodeProps={"identifier": "ps-1"},
    )
    data_in = _make_ingest_data(linkRels=[expected_rel])
    data_out = _make_ingest_data()

    errors = validate_ingested_data(data_in, data_out)
    assert len(errors) == 1
    assert errors[0]["msg"] == "ingestion failed to create expected relation"


def test_validate_ingested_data_mismatched_node_labels() -> None:
    rel_in = GraphRel(
        edgeLabel="title",
        edgeProps={"position": 0},
        nodeLabels=["Text"],
        nodeProps={"value": "t"},
    )
    rel_out = GraphRel(
        edgeLabel="title",
        edgeProps={"position": 0},
        nodeLabels=["Link"],
        nodeProps={"value": "t"},
    )
    data_in = _make_ingest_data(createRels=[rel_in])
    data_out = _make_ingest_data(createRels=[rel_out])

    errors = validate_ingested_data(data_in, data_out)
    assert len(errors) == 1
    assert errors[0]["msg"] == "referenced node has unexpected labels"
    assert errors[0]["loc"] == ("createRels", "title", 0, "nodeLabels")


def test_validate_ingested_data_mismatched_node_props() -> None:
    rel_in = GraphRel(
        edgeLabel="title",
        edgeProps={"position": 0},
        nodeLabels=["Text"],
        nodeProps={"value": "expected"},
    )
    rel_out = GraphRel(
        edgeLabel="title",
        edgeProps={"position": 0},
        nodeLabels=["Text"],
        nodeProps={"value": "actual"},
    )
    data_in = _make_ingest_data(createRels=[rel_in])
    data_out = _make_ingest_data(createRels=[rel_out])

    errors = validate_ingested_data(data_in, data_out)
    assert len(errors) == 1
    assert errors[0]["msg"] == "referenced node has unexpected properties"
    assert errors[0]["loc"] == ("createRels", "title", 0, "nodeProps")


def test_get_error_details_from_neo4j_error() -> None:
    error = Mock()
    error.code = "Neo.ClientError.Schema.ConstraintValidationFailed"
    error.message = "Node already exists"
    error.metadata = {"some": "info"}

    result = get_error_details_from_neo4j_error("some-input", error)

    assert len(result) == 1
    assert result[0]["type"] == "Neo.ClientError.Schema.ConstraintValidationFailed"
    assert result[0]["msg"] == "Node already exists"
    assert result[0]["input"] == "some-input"
    assert result[0]["ctx"] == {"meta": {"some": "info"}}
