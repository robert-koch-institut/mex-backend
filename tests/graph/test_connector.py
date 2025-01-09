from typing import cast
from unittest.mock import MagicMock, Mock, call

import pytest
from backoff.types import Details as BackoffDetails
from black import DEFAULT_LINE_LENGTH
from jinja2 import Environment
from neo4j.exceptions import IncompleteCommit, SessionExpired
from pytest import LogCaptureFixture, MonkeyPatch

from mex.backend.graph import connector as connector_module
from mex.backend.graph.connector import MEX_EXTRACTED_PRIMARY_SOURCE, GraphConnector
from mex.backend.graph.exceptions import InconsistentGraphError
from mex.backend.graph.query import Query
from mex.backend.settings import BackendSettings
from mex.common.exceptions import MExError
from mex.common.models import (
    MEX_PRIMARY_SOURCE_IDENTIFIER,
    MEX_PRIMARY_SOURCE_IDENTIFIER_IN_PRIMARY_SOURCE,
    MEX_PRIMARY_SOURCE_STABLE_TARGET_ID,
    AnyExtractedModel,
    ExtractedOrganizationalUnit,
    OrganizationalUnitRuleSetRequest,
    OrganizationalUnitRuleSetResponse,
)
from mex.common.types import Identifier
from tests.conftest import MockedGraph, get_graph


@pytest.fixture
def mocked_query_class(monkeypatch: MonkeyPatch) -> None:
    monkeypatch.setattr(Query, "__str__", Query.__repr__)
    monkeypatch.setattr(Query.REPR_MODE, "line_length", DEFAULT_LINE_LENGTH)


@pytest.mark.usefixtures("mocked_query_class")
def test_check_connectivity_and_authentication(mocked_graph: MockedGraph) -> None:
    mocked_graph.return_value = [{"currentStatus": "online"}]
    graph = GraphConnector.get()
    graph._check_connectivity_and_authentication()

    assert mocked_graph.call_args_list[-1].args == ("fetch_database_status()", {})


def test_check_connectivity_and_authentication_error(mocked_graph: MockedGraph) -> None:
    mocked_graph.return_value = [{"currentStatus": "offline"}]
    graph = GraphConnector.get()
    with pytest.raises(MExError, match="Database is offline"):
        graph._check_connectivity_and_authentication()


@pytest.mark.usefixtures("mocked_query_class")
def test_mocked_graph_seed_constraints(mocked_graph: MockedGraph) -> None:
    graph = GraphConnector.get()
    graph._seed_constraints()

    assert mocked_graph.call_args_list[-1].args == (
        'create_identifier_uniqueness_constraint(node_label="MergedVariableGroup")',
        {},
    )


@pytest.mark.usefixtures("mocked_query_class")
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


@pytest.mark.usefixtures("mocked_query_class")
def test_mocked_graph_seed_data(mocked_graph: MockedGraph) -> None:
    mocked_graph.return_value = [{"edges": ["hadPrimarySource", "stableTargetId"]}]
    graph = GraphConnector.get()
    graph._seed_data()

    assert mocked_graph.call_args_list[-2].args == (
        """\
merge_item(
    current_label="ExtractedPrimarySource",
    current_constraints=["identifier"],
    merged_label="MergedPrimarySource",
    nested_edge_labels=[],
    nested_node_labels=[],
)""",
        {
            "identifier": MEX_PRIMARY_SOURCE_IDENTIFIER,
            "stable_target_id": MEX_PRIMARY_SOURCE_STABLE_TARGET_ID,
            "on_match": {"version": None},
            "on_create": {
                "version": None,
                "identifier": MEX_PRIMARY_SOURCE_IDENTIFIER,
                "identifierInPrimarySource": (
                    MEX_PRIMARY_SOURCE_IDENTIFIER_IN_PRIMARY_SOURCE
                ),
            },
            "nested_positions": [],
            "nested_values": [],
        },
    )
    assert mocked_graph.call_args_list[-1].args == (
        """\
merge_edges(
    current_label="ExtractedPrimarySource",
    current_constraints=["identifier"],
    merged_label="MergedPrimarySource",
    ref_labels=["hadPrimarySource", "stableTargetId"],
)""",
        {
            "identifier": MEX_PRIMARY_SOURCE_IDENTIFIER,
            "ref_identifiers": [
                MEX_PRIMARY_SOURCE_STABLE_TARGET_ID,
                MEX_PRIMARY_SOURCE_STABLE_TARGET_ID,
            ],
            "ref_positions": [0, 0],
            "stable_target_id": MEX_PRIMARY_SOURCE_STABLE_TARGET_ID,
        },
    )


def test_should_giveup_commit() -> None:
    retryable_error = SessionExpired("session is dead")
    assert GraphConnector._should_giveup_commit(retryable_error) is False

    terminal_error = IncompleteCommit("commit broke midway")
    assert GraphConnector._should_giveup_commit(terminal_error) is True


@pytest.mark.usefixtures("mocked_graph")
def test_on_commit_backoff(monkeypatch: MonkeyPatch) -> None:
    init_driver = MagicMock()
    check_connectivity_and_authentication = MagicMock()
    monkeypatch.setattr(GraphConnector, "_seed_constraints", MagicMock())
    monkeypatch.setattr(GraphConnector, "_init_driver", init_driver)
    monkeypatch.setattr(
        GraphConnector,
        "_check_connectivity_and_authentication",
        check_connectivity_and_authentication,
    )

    connector = GraphConnector.get()
    event = BackoffDetails(args=(connector,))
    GraphConnector._on_commit_backoff(event)
    assert init_driver.call_args_list == [call()]
    assert check_connectivity_and_authentication.call_args_list == [call()]


@pytest.mark.parametrize(
    ("debug", "expected"),
    [
        (True, 'error committing query\nMATCH jjj RETURN "ccc";'),
        (False, 'error committing query: match_something(jinja_var="jjj")'),
    ],
)
@pytest.mark.usefixtures("mocked_graph")
def test_on_commit_giveup(
    caplog: LogCaptureFixture, monkeypatch: MonkeyPatch, debug: bool, expected: str
) -> None:
    settings = BackendSettings.get()
    monkeypatch.setattr(settings, "debug", debug)
    template = Environment(autoescape=True).from_string(
        r"MATCH {{jinja_var}} RETURN $cypher_var;"
    )

    connector = GraphConnector.get()
    query = Query("match_something", template, {"jinja_var": "jjj"})
    event = BackoffDetails(args=(connector, query), kwargs={"cypher_var": "ccc"})
    GraphConnector._on_commit_giveup(event)

    assert caplog.messages[-1] == expected


def test_mocked_graph_commit_raises_error(mocked_graph: MockedGraph) -> None:
    mocked_graph.run.side_effect = Exception("query failed")
    connector = GraphConnector.get()
    with pytest.raises(Exception, match="query failed"):
        connector.commit("RETURN 1;")


@pytest.mark.usefixtures("mocked_query_class")
def test_mocked_graph_fetch_extracted_items(mocked_graph: MockedGraph) -> None:
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
    result = graph.fetch_extracted_items(
        query_string="my-query",
        stable_target_id=Identifier.generate(99),
        entity_type=["ExtractedFoo", "ExtractedBar", "ExtractedBatz"],
        skip=10,
        limit=100,
    )

    assert mocked_graph.call_args_list[-1].args == (
        """\
fetch_extracted_or_rule_items(
    filter_by_query_string=True, filter_by_stable_target_id=True
)""",
        {
            "labels": [
                "ExtractedFoo",
                "ExtractedBar",
                "ExtractedBatz",
            ],
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


@pytest.mark.usefixtures("load_dummy_data")
@pytest.mark.integration
def test_fetch_extracted_items() -> None:
    connector = GraphConnector.get()

    result = connector.fetch_extracted_items(None, None, None, 0, 1)

    assert result.one() == {
        "items": [
            {
                "entityType": MEX_EXTRACTED_PRIMARY_SOURCE.entityType,
                "hadPrimarySource": [MEX_EXTRACTED_PRIMARY_SOURCE.hadPrimarySource],
                "identifier": MEX_EXTRACTED_PRIMARY_SOURCE.identifier,
                "identifierInPrimarySource": MEX_EXTRACTED_PRIMARY_SOURCE.identifierInPrimarySource,
                "stableTargetId": [MEX_EXTRACTED_PRIMARY_SOURCE.stableTargetId],
            }
        ],
        "total": 10,
    }


@pytest.mark.integration
def test_fetch_extracted_items_empty() -> None:
    connector = GraphConnector.get()

    result = connector.fetch_extracted_items(None, "thisIdDoesNotExist", None, 0, 1)

    assert result.one() == {"items": [], "total": 0}


@pytest.mark.usefixtures("mocked_query_class")
def test_mocked_graph_fetch_rule_items(mocked_graph: MockedGraph) -> None:
    mocked_graph.return_value = [
        {
            "items": [
                {
                    "entityType": "AdditiveThis",
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
    result = graph.fetch_rule_items(
        query_string="my-query",
        stable_target_id=Identifier.generate(99),
        entity_type=["AdditiveFoo", "SubtractiveBar", "PreventiveBatz"],
        skip=10,
        limit=100,
    )

    assert mocked_graph.call_args_list[-1].args == (
        """\
fetch_extracted_or_rule_items(
    filter_by_query_string=True, filter_by_stable_target_id=True
)""",
        {
            "labels": [
                "AdditiveFoo",
                "SubtractiveBar",
                "PreventiveBatz",
            ],
            "limit": 100,
            "query_string": "my-query",
            "skip": 10,
            "stable_target_id": "bFQoRhcVH5DHV1",
        },
    )

    assert result.one() == {
        "items": [
            {
                "entityType": "AdditiveThis",
                "inlineProperty": ["foo", "bar"],
                "nestedProperty": ["first", "second"],
            }
        ],
        "total": 1,
    }


@pytest.mark.integration
def test_fetch_rule_items(
    load_dummy_rule_set: OrganizationalUnitRuleSetResponse,
) -> None:
    connector = GraphConnector.get()

    result = connector.fetch_rule_items(None, None, None, 0, 1)

    assert result.one() == {
        "items": [
            {
                "email": [],
                "entityType": "AdditiveOrganizationalUnit",
                "name": [{"value": "Unit 1.7", "language": "en"}],
                "website": [{"title": "Unit Homepage", "url": "https://unit-1-7"}],
                "parentUnit": [load_dummy_rule_set.additive.parentUnit],
                "stableTargetId": [load_dummy_rule_set.stableTargetId],
            }
        ],
        "total": 3,
    }


@pytest.mark.integration
def test_fetch_rule_items_empty() -> None:
    connector = GraphConnector.get()

    result = connector.fetch_rule_items(None, "thisIdDoesNotExist", None, 0, 1)

    assert result.one() == {"items": [], "total": 0}


@pytest.mark.usefixtures("mocked_query_class")
def test_mocked_graph_fetch_merged_items(mocked_graph: MockedGraph) -> None:
    mocked_graph.return_value = [
        {
            "items": [
                {
                    "components": [
                        {
                            "inlineProperty": "foo",
                            "entityType": "ExtractedThis",
                            "_refs": [
                                {
                                    "label": "nestedProperty",
                                    "position": 0,
                                    "value": "first",
                                },
                                {
                                    "label": "nestedProperty",
                                    "position": 1,
                                    "value": "second",
                                },
                            ],
                        },
                        {
                            "entityType": "PreventiveThis",
                            "_refs": [
                                {
                                    "label": "stableTargetId",
                                    "position": 0,
                                    "value": "bFQoRhcVH5DHUB",
                                }
                            ],
                        },
                    ],
                    "entityType": "MergedThis",
                    "identifier": "bFQoRhcVH5DHV1",
                }
            ],
            "total": 1,
        }
    ]
    graph = GraphConnector.get()
    result = graph.fetch_merged_items(
        query_string="my-query",
        identifier=Identifier.generate(99),
        entity_type=["MergedFoo", "MergedBar", "MergedBatz"],
        skip=10,
        limit=100,
    )

    assert mocked_graph.call_args_list[-1].args == (
        """\
fetch_merged_items(filter_by_query_string=True, filter_by_identifier=True)""",
        {
            "labels": [
                "MergedFoo",
                "MergedBar",
                "MergedBatz",
            ],
            "limit": 100,
            "query_string": "my-query",
            "skip": 10,
            "identifier": "bFQoRhcVH5DHV1",
        },
    )

    assert result.one() == {
        "items": [
            {
                "components": [
                    {
                        "entityType": "ExtractedThis",
                        "inlineProperty": "foo",
                        "nestedProperty": ["first", "second"],
                    },
                    {
                        "entityType": "PreventiveThis",
                        "stableTargetId": ["bFQoRhcVH5DHUB"],
                    },
                ],
                "entityType": "MergedThis",
                "identifier": "bFQoRhcVH5DHV1",
            }
        ],
        "total": 1,
    }


@pytest.mark.usefixtures("load_dummy_data", "load_dummy_rule_set")
@pytest.mark.integration
def test_fetch_merged_items() -> None:
    connector = GraphConnector.get()

    result = connector.fetch_merged_items(
        query_string=None,
        identifier=None,
        entity_type=["MergedOrganizationalUnit"],
        skip=1,
        limit=1,
    )

    assert result.one() == {
        "items": [
            {
                "components": [
                    {
                        "identifierInPrimarySource": "ou-1",
                        "email": [],
                        "entityType": "ExtractedOrganizationalUnit",
                        "identifier": "bFQoRhcVH5DHUw",
                        "stableTargetId": ["bFQoRhcVH5DHUx"],
                        "hadPrimarySource": ["bFQoRhcVH5DHUt"],
                        "unitOf": ["bFQoRhcVH5DHUv"],
                        "name": [{"value": "Unit 1", "language": "en"}],
                    }
                ],
                "entityType": "MergedOrganizationalUnit",
                "identifier": "bFQoRhcVH5DHUx",
            }
        ],
        "total": 2,
    }


@pytest.mark.integration
def test_fetch_merged_items_empty() -> None:
    connector = GraphConnector.get()

    result = connector.fetch_merged_items(
        query_string=None,
        identifier="thisIdDoesNotExist",
        entity_type=None,
        skip=0,
        limit=1,
    )

    assert result.one() == {"items": [], "total": 0}


@pytest.mark.usefixtures("mocked_query_class")
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
        had_primary_source=Identifier.generate(101),
        identifier_in_primary_source="one",
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


@pytest.mark.usefixtures("mocked_query_class")
def test_mocked_graph_exists_merged_item(
    mocked_graph: MockedGraph, monkeypatch: MonkeyPatch
) -> None:
    monkeypatch.setattr(
        connector_module,
        "MERGED_MODEL_CLASSES_BY_NAME",
        {"MergedFoo": Mock(), "MergedBar": Mock(), "MergedBatz": Mock()},
    )
    mocked_graph.return_value = [{"exists": True}]

    graph = GraphConnector.get()
    graph.exists_merged_item(
        stable_target_id=Identifier.generate(99),
        stem_types=["Foo", "Bar"],
    )

    assert mocked_graph.call_args_list[-1].args == (
        """\
exists_merged_item(node_labels=["MergedFoo", "MergedBar"])""",
        {"identifier": Identifier.generate(99)},
    )

    graph.exists_merged_item(
        stable_target_id=Identifier.generate(99),
    )

    assert mocked_graph.call_args_list[-1].args == (
        """\
exists_merged_item(node_labels=["MergedFoo", "MergedBar", "MergedBatz"])""",
        {"identifier": Identifier.generate(99)},
    )


@pytest.mark.parametrize(
    ("stable_target_id", "stem_types", "exists"),
    [
        ("bFQoRhcVH5DHUB", None, True),
        ("bFQoRhcVH5DHUB", ["ContactPoint"], True),
        ("bFQoRhcVH5DHUB", ["Person", "ContactPoint", "OrganizationalUnit"], True),
        ("bFQoRhcVH5DHUB", ["Activity"], False),
        ("thisIdDoesNotExist", ["Activity"], False),
    ],
    ids=[
        "found without type filter",
        "found with type filter",
        "found with multi-type filter",
        "missed due to filter",
        "missed due to identifier",
    ],
)
@pytest.mark.usefixtures("load_dummy_data")
@pytest.mark.integration
def test_graph_exists_merged_item(
    stable_target_id: Identifier, stem_types: list[str] | None, exists: bool
) -> None:
    connector = GraphConnector.get()

    assert connector.exists_merged_item(stable_target_id, stem_types) == exists


@pytest.mark.usefixtures("mocked_query_class")
def test_mocked_graph_merge_item(
    mocked_graph: MockedGraph, dummy_data: dict[str, AnyExtractedModel]
) -> None:
    extracted_organizational_unit = dummy_data["organizational_unit_1"]
    graph = GraphConnector.get()
    graph._merge_item(
        extracted_organizational_unit,
        extracted_organizational_unit.stableTargetId,
        identifier=extracted_organizational_unit.identifier,
    )

    assert mocked_graph.call_args_list[-1].args == (
        """\
merge_item(
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


@pytest.mark.usefixtures("mocked_query_class")
def test_mocked_graph_merge_edges(
    mocked_graph: MockedGraph, dummy_data: dict[str, AnyExtractedModel]
) -> None:
    mocked_graph.return_value = [
        {"edges": ["hadPrimarySource", "unitOf", "stableTargetId"]},
    ]
    graph = GraphConnector.get()

    extracted_organizational_unit = cast(
        ExtractedOrganizationalUnit, dummy_data["organizational_unit_1"]
    )
    graph._merge_edges(
        extracted_organizational_unit,
        extracted_organizational_unit.stableTargetId,
        identifier=extracted_organizational_unit.identifier,
    )

    assert mocked_graph.call_args_list[-1].args == (
        """\
merge_edges(
    current_label="ExtractedOrganizationalUnit",
    current_constraints=["identifier"],
    merged_label="MergedOrganizationalUnit",
    ref_labels=["hadPrimarySource", "unitOf", "stableTargetId"],
)""",
        {
            "identifier": extracted_organizational_unit.identifier,
            "ref_identifiers": [
                extracted_organizational_unit.hadPrimarySource,
                extracted_organizational_unit.unitOf[0],
                extracted_organizational_unit.stableTargetId,
            ],
            "ref_positions": [0, 0, 0],
            "stable_target_id": extracted_organizational_unit.stableTargetId,
        },
    )


@pytest.mark.usefixtures("mocked_query_class")
def test_mocked_graph_merge_edges_fails(
    mocked_graph: MockedGraph, dummy_data: dict[str, AnyExtractedModel]
) -> None:
    mocked_graph.return_value = [
        {"edges": ["stableTargetId"]},  # missing hadPrimarySource
    ]
    graph = GraphConnector.get()

    extracted_organizational_unit = dummy_data["organizational_unit_1"]

    with pytest.raises(InconsistentGraphError, match="could not merge all edges"):
        graph._merge_edges(
            extracted_organizational_unit,
            extracted_organizational_unit.stableTargetId,
            identifier=extracted_organizational_unit.identifier,
        )


@pytest.mark.usefixtures("mocked_query_class")
def test_mocked_graph_creates_rule_set(
    mocked_graph: MockedGraph,
    organizational_unit_rule_set_request: OrganizationalUnitRuleSetRequest,
) -> None:
    mocked_graph.side_effect = [
        [{"current": {}}],  # additive item
        [{"edges": ["parentUnit", "stableTargetId"]}],  # additive edges
        [{"current": {}}],  # subtractive item
        [{"edges": ["stableTargetId"]}],  # subtractive edges
        [{"current": {}}],  # preventive item
        [{"edges": ["stableTargetId"]}],  # preventive edges
    ]
    graph = GraphConnector.get()
    graph.create_rule_set(organizational_unit_rule_set_request, Identifier.generate(42))

    assert len(mocked_graph.call_args_list) == 6
    assert mocked_graph.call_args_list[-2].args == (
        """\
merge_item(
    current_label="PreventiveOrganizationalUnit",
    current_constraints=[],
    merged_label="MergedOrganizationalUnit",
    nested_edge_labels=[],
    nested_node_labels=[],
)""",
        {
            "stable_target_id": "bFQoRhcVH5DHU6",
            "on_match": {},
            "on_create": {},
            "nested_values": [],
            "nested_positions": [],
        },
    )
    assert mocked_graph.call_args_list[-1].args == (
        """\
merge_edges(
    current_label="PreventiveOrganizationalUnit",
    current_constraints=[],
    merged_label="MergedOrganizationalUnit",
    ref_labels=["stableTargetId"],
)""",
        {
            "stable_target_id": "bFQoRhcVH5DHU6",
            "ref_identifiers": ["bFQoRhcVH5DHU6"],
            "ref_positions": [0],
        },
    )


def test_mocked_graph_ingests_models(
    mocked_graph: MockedGraph,
    dummy_data: dict[str, AnyExtractedModel],
) -> None:
    # the `$comment` keys are just for easier debugging
    mocked_graph.side_effect = [
        [{"current": {}, "$comment": "mock response for PrimarySource ps-1 item"}],
        [{"current": {}, "$comment": "mock response for PrimarySource ps-2 item"}],
        [{"current": {}, "$comment": "mock response for ContactPoint cp-1 item"}],
        [{"current": {}, "$comment": "mock response for ContactPoint cp-2 item"}],
        [{"current": {}, "$comment": "mock response for Organization rki item"}],
        [
            {
                "current": {},
                "$comment": "mock response for Organization robert-koch-institute item",
            }
        ],
        [{"current": {}, "$comment": "mock response for OrganizationalUnit ou-1 item"}],
        [
            {
                "current": {},
                "$comment": "mock response for OrganizationalUnit ou-1.6 item",
            }
        ],
        [{"current": {}, "$comment": "mock response for Activity a-1 item"}],
        [
            {
                "edges": ["hadPrimarySource", "stableTargetId"],
                "$comment": "mock response for PrimarySource ps-1 edges",
            },
        ],
        [
            {
                "edges": ["hadPrimarySource", "stableTargetId"],
                "$comment": "mock response for PrimarySource ps-2 edges",
            },
        ],
        [
            {
                "edges": ["hadPrimarySource", "stableTargetId"],
                "$comment": "mock response for ContactPoint cp-1 edges",
            },
        ],
        [
            {
                "edges": ["hadPrimarySource", "stableTargetId"],
                "$comment": "mock response for ContactPoint cp-2 edges",
            },
        ],
        [
            {
                "edges": ["hadPrimarySource", "stableTargetId"],
                "$comment": "mock response for Organization rki edges",
            }
        ],
        [
            {
                "edges": ["hadPrimarySource", "stableTargetId"],
                "$comment": "mock response for Organization robert-koch-institute edges",
            }
        ],
        [
            {
                "edges": ["hadPrimarySource", "unitOf", "stableTargetId"],
                "$comment": "mock response for OrganizationalUnit ou-1 edges",
            }
        ],
        [
            {
                "edges": ["hadPrimarySource", "parentUnit", "unitOf", "stableTargetId"],
                "$comment": "mock response for OrganizationalUnit ou-1.6 edges",
            }
        ],
        [
            {
                "edges": [
                    "hadPrimarySource",
                    "contact",
                    "contact",
                    "contact",
                    "responsibleUnit",
                    "stableTargetId",
                ],
                "$comment": "mock response for Activity a-1 edges",
            },
        ],
    ]

    graph = GraphConnector.get()
    identifiers = graph.ingest(list(dummy_data.values()))

    assert identifiers == [d.identifier for d in dummy_data.values()]


@pytest.mark.integration
def test_connector_flush_fails(monkeypatch: MonkeyPatch) -> None:
    settings = BackendSettings.get()

    monkeypatch.setattr(settings, "debug", False)
    connector = GraphConnector.get()

    with pytest.raises(
        MExError, match="database flush was attempted outside of debug mode"
    ):
        connector.flush()


@pytest.mark.integration
@pytest.mark.usefixtures("load_dummy_data")
def test_connector_flush(monkeypatch: MonkeyPatch) -> None:
    assert len(get_graph()) >= 10

    settings = BackendSettings.get()
    monkeypatch.setattr(settings, "debug", True)
    connector = GraphConnector.get()
    connector.flush()

    assert len(get_graph()) == 0
