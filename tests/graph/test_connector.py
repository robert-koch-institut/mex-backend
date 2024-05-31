from collections.abc import Callable

import pytest
from black import Mode, format_str
from pytest import MonkeyPatch

from mex.backend.graph import connector as connector_module
from mex.backend.graph.connector import MEX_EXTRACTED_PRIMARY_SOURCE, GraphConnector
from mex.backend.graph.query import QueryBuilder
from mex.common.models import AnyExtractedModel
from mex.common.types import (
    ExtractedPrimarySourceIdentifier,
    Identifier,
    MergedPrimarySourceIdentifier,
)
from tests.conftest import MockedGraph


@pytest.fixture
def mocked_query_builder(monkeypatch: MonkeyPatch) -> None:
    def __getattr__(_: QueryBuilder, query: str) -> Callable[..., str]:
        return lambda **parameters: format_str(
            f"{query}({','.join(f'{k}={v!r}' for k, v in parameters.items())})",
            mode=Mode(line_length=78),
        ).strip()

    monkeypatch.setattr(QueryBuilder, "__getattr__", __getattr__)


@pytest.mark.usefixtures("mocked_query_builder")
def test_mocked_graph_seed_constraints(mocked_graph: MockedGraph) -> None:
    graph = GraphConnector.get()
    graph._seed_constraints()

    assert mocked_graph.call_args_list[-1].args == (
        'create_identifier_uniqueness_constraint(node_label="MergedVariableGroup")',
        {},
    )


@pytest.mark.usefixtures("mocked_query_builder")
def test_mocked_graph_seed_indices(
    mocked_graph: MockedGraph, monkeypatch: MonkeyPatch
) -> None:
    monkeypatch.setattr(
        connector_module, "SEARCHABLE_CLASSES", ["ExtractedThis", "ExtractedThat"]
    )
    monkeypatch.setattr(
        connector_module,
        "SEARCHABLE_FIELDS",
        ["title", "name", "keyword", "description"],
    )
    graph = GraphConnector.get()
    graph._seed_indices()

    assert mocked_graph.call_args_list[-1].args == (
        """\
create_full_text_search_index(
    node_labels=["ExtractedThis", "ExtractedThat"],
    search_fields=["title", "name", "keyword", "description"],
)""",
        {
            "index_config": {
                "fulltext.eventually_consistent": True,
                "fulltext.analyzer": "german",
            }
        },
    )

    mocked_graph.return_value = [
        {
            "node_labels": ["ExtractedThis", "ExtractedThat"],
            "search_fields": ["title", "name", "keyword", "description"],
        }
    ]
    monkeypatch.setattr(
        connector_module,
        "SEARCHABLE_CLASSES",
        ["ExtractedThis", "ExtractedThat", "ExtractedOther"],
    )

    graph._seed_indices()

    assert mocked_graph.call_args_list[-2].args == ("drop_full_text_search_index()", {})
    assert mocked_graph.call_args_list[-1].args == (
        """\
create_full_text_search_index(
    node_labels=["ExtractedThis", "ExtractedThat", "ExtractedOther"],
    search_fields=["title", "name", "keyword", "description"],
)""",
        {
            "index_config": {
                "fulltext.eventually_consistent": True,
                "fulltext.analyzer": "german",
            }
        },
    )


@pytest.mark.usefixtures("mocked_query_builder")
def test_mocked_graph_seed_data(mocked_graph: MockedGraph) -> None:
    graph = GraphConnector.get()
    graph._seed_data()

    assert mocked_graph.call_args_list[-2].args == (
        """\
merge_extracted_node(
    current_label="ExtractedPrimarySource",
    current_constraints=["identifier"],
    merged_label="MergedPrimarySource",
    nested_edge_labels=[],
    nested_node_labels=[],
)""",
        {
            "identifier": ExtractedPrimarySourceIdentifier("00000000000001"),
            "stable_target_id": MergedPrimarySourceIdentifier("00000000000000"),
            "on_match": {"version": None},
            "on_create": {
                "version": None,
                "identifier": "00000000000001",
                "identifierInPrimarySource": "mex",
            },
            "nested_positions": [],
            "nested_values": [],
        },
    )
    assert mocked_graph.call_args_list[-1].args == (
        """\
merge_extracted_edges(
    current_label="ExtractedPrimarySource",
    ref_labels=["hadPrimarySource", "stableTargetId"],
)""",
        {
            "identifier": "00000000000001",
            "ref_identifiers": ["00000000000000", "00000000000000"],
            "ref_positions": [0, 0],
            "stable_target_id": "00000000000000",
        },
    )


@pytest.mark.usefixtures("mocked_query_builder")
def test_mocked_graph_fetch_extracted_data(mocked_graph: MockedGraph) -> None:
    mocked_graph.return_value = [
        {
            "items": [
                {
                    "entityType": "ExtractedThis",
                    "inlineProperty": ["foo", "bar"],
                    "_refs": [
                        {"value": "second", "position": 1, "label": "nestedProperty"},
                        {"value": "first", "position": 0, "label": "nestedProperty"},
                    ],
                }
            ],
            "total": 1,
        }
    ]
    graph = GraphConnector.get()
    result = graph.fetch_extracted_data(
        query_string="my-query",
        stable_target_id=Identifier.generate(99),
        entity_type=[],
        skip=10,
        limit=100,
    )

    assert mocked_graph.call_args_list[-1].args == (
        """\
fetch_extracted_data(
    filter_by_query_string=True,
    filter_by_stable_target_id=True,
    filter_by_labels=False,
)""",
        {
            "labels": [],
            "limit": 100,
            "query_string": "my-query",
            "skip": 10,
            "stable_target_id": "bFQoRhcVH5DHV1",
        },
    )

    assert result.one() == {
        "items": [
            {
                "entityType": "ExtractedThis",
                "inlineProperty": ["foo", "bar"],
                "nestedProperty": ["first", "second"],
            }
        ],
        "total": 1,
    }


@pytest.mark.usefixtures("mocked_query_builder")
def test_mocked_graph_fetch_identities(mocked_graph: MockedGraph) -> None:
    graph = GraphConnector.get()
    graph.fetch_identities(stable_target_id=Identifier.generate(99))

    assert mocked_graph.call_args_list[-1].args == (
        """\
fetch_identities(
    filter_by_had_primary_source=False,
    filter_by_identifier_in_primary_source=False,
    filter_by_stable_target_id=True,
)""",
        {
            "had_primary_source": None,
            "identifier_in_primary_source": None,
            "stable_target_id": Identifier.generate(99),
            "limit": 1000,
        },
    )

    graph.fetch_identities(
        had_primary_source=Identifier.generate(101), identifier_in_primary_source="one"
    )

    assert mocked_graph.call_args_list[-1].args == (
        """\
fetch_identities(
    filter_by_had_primary_source=True,
    filter_by_identifier_in_primary_source=True,
    filter_by_stable_target_id=False,
)""",
        {
            "had_primary_source": Identifier.generate(101),
            "identifier_in_primary_source": "one",
            "stable_target_id": None,
            "limit": 1000,
        },
    )

    graph.fetch_identities(identifier_in_primary_source="two")

    assert mocked_graph.call_args_list[-1].args == (
        """\
fetch_identities(
    filter_by_had_primary_source=False,
    filter_by_identifier_in_primary_source=True,
    filter_by_stable_target_id=False,
)""",
        {
            "had_primary_source": None,
            "identifier_in_primary_source": "two",
            "stable_target_id": None,
            "limit": 1000,
        },
    )


@pytest.mark.usefixtures("mocked_query_builder")
def test_mocked_graph_merge_extracted_node(
    mocked_graph: MockedGraph, dummy_data: list[AnyExtractedModel]
) -> None:
    extracted_organizational_unit = dummy_data[4]
    graph = GraphConnector.get()
    graph._merge_extracted_node(extracted_organizational_unit)

    assert mocked_graph.call_args_list[-1].args == (
        """\
merge_extracted_node(
    current_label="ExtractedOrganizationalUnit",
    current_constraints=["identifier"],
    merged_label="MergedOrganizationalUnit",
    nested_edge_labels=["name"],
    nested_node_labels=["Text"],
)""",
        {
            "identifier": extracted_organizational_unit.identifier,
            "nested_positions": [0],
            "nested_values": [{"language": "en", "value": "Unit 1"}],
            "on_create": {
                "email": [],
                "identifier": extracted_organizational_unit.identifier,
                "identifierInPrimarySource": "ou-1",
            },
            "on_match": {"email": []},
            "stable_target_id": extracted_organizational_unit.stableTargetId,
        },
    )


@pytest.mark.usefixtures("mocked_query_builder")
def test_mocked_graph_merge_extracted_edges(
    mocked_graph: MockedGraph, dummy_data: list[AnyExtractedModel]
) -> None:
    extracted_activity = dummy_data[4]
    graph = GraphConnector.get()
    graph._merge_extracted_edges(extracted_activity)

    assert mocked_graph.call_args_list[-1].args == (
        """\
merge_extracted_edges(
    current_label="ExtractedOrganizationalUnit",
    ref_labels=["hadPrimarySource", "stableTargetId"],
)""",
        {
            "identifier": extracted_activity.identifier,
            "ref_identifiers": [
                extracted_activity.hadPrimarySource,
                extracted_activity.stableTargetId,
            ],
            "ref_positions": [0, 0],
            "stable_target_id": "cWWm02l1c6cucKjIhkFqY4",
        },
    )


@pytest.mark.usefixtures("mocked_graph")
def test_mocked_graph_ingests_models(dummy_data: list[AnyExtractedModel]) -> None:
    graph = GraphConnector.get()
    identifiers = graph.ingest(dummy_data)

    assert identifiers == [d.identifier for d in dummy_data]


@pytest.mark.usefixtures("load_dummy_data")
@pytest.mark.integration
def test_fetch_extracted_data() -> None:
    connector = GraphConnector.get()

    result = connector.fetch_extracted_data(None, None, None, 0, 1)

    assert result.all() == [
        {
            "items": [
                {
                    "entityType": MEX_EXTRACTED_PRIMARY_SOURCE.entityType,
                    "hadPrimarySource": [MEX_EXTRACTED_PRIMARY_SOURCE.hadPrimarySource],
                    "identifier": MEX_EXTRACTED_PRIMARY_SOURCE.identifier,
                    "identifierInPrimarySource": MEX_EXTRACTED_PRIMARY_SOURCE.identifierInPrimarySource,
                    "stableTargetId": [MEX_EXTRACTED_PRIMARY_SOURCE.stableTargetId],
                }
            ],
            "total": 7,
        }
    ]
