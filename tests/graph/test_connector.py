import re
from typing import Any, cast
from unittest.mock import MagicMock, Mock, call

import pytest
from backoff.types import Details as BackoffDetails
from black import DEFAULT_LINE_LENGTH
from jinja2 import Environment
from neo4j.exceptions import IncompleteCommit, SessionExpired
from pytest import LogCaptureFixture, MonkeyPatch

from mex.backend.graph import connector as connector_module
from mex.backend.graph.connector import GraphConnector
from mex.backend.graph.exceptions import InconsistentGraphError
from mex.backend.graph.query import Query
from mex.backend.settings import BackendSettings
from mex.common.exceptions import MExError
from mex.common.models import (
    EXTRACTED_MODEL_CLASSES_BY_NAME,
    MEX_PRIMARY_SOURCE_IDENTIFIER,
    MEX_PRIMARY_SOURCE_IDENTIFIER_IN_PRIMARY_SOURCE,
    MEX_PRIMARY_SOURCE_STABLE_TARGET_ID,
    AnyExtractedModel,
    ExtractedOrganization,
    ExtractedOrganizationalUnit,
    OrganizationalUnitRuleSetResponse,
)
from mex.common.types import Identifier, Text, TextLanguage
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
    mocked_graph.return_value = [
        {"edges": ["hadPrimarySource {position: 0}", "stableTargetId {position: 0}"]}
    ]
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

    graph = GraphConnector.get()
    event = BackoffDetails(args=(graph,))
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

    graph = GraphConnector.get()
    query = Query("match_something", template, {"jinja_var": "jjj"})
    event = BackoffDetails(args=(graph, query), kwargs={"cypher_var": "ccc"})
    GraphConnector._on_commit_giveup(event)

    assert caplog.messages[-1] == expected


def test_mocked_graph_commit_raises_error(mocked_graph: MockedGraph) -> None:
    mocked_graph.run.side_effect = Exception("query failed")
    graph = GraphConnector.get()
    with pytest.raises(Exception, match="query failed"):
        graph.commit("RETURN 1;")


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
        had_primary_source=None,
        skip=10,
        limit=100,
    )

    assert mocked_graph.call_args_list[-1].args == (
        """\
fetch_extracted_or_rule_items(
    filter_by_query_string=True,
    filter_by_stable_target_id=True,
    filter_by_reference_to_merged_item=False,
    reference_field_name="hadPrimarySource",
)""",
        {
            "labels": [
                "ExtractedFoo",
                "ExtractedBar",
                "ExtractedBatz",
            ],
            "limit": 100,
            "query_string": "my-query",
            "referenced_identifiers": None,
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


@pytest.mark.parametrize(
    ("query_string", "stable_target_id", "entity_type", "limit", "expected"),
    [
        (None, "thisIdDoesNotExist", None, 10, {"items": [], "total": 0}),
        ("this_search_term_is_not_findable", None, None, 10, {"items": [], "total": 0}),
        (
            None,
            None,
            None,
            1,
            {
                "items": [
                    {
                        "identifierInPrimarySource": "mex",
                        "entityType": "ExtractedPrimarySource",
                        "identifier": "00000000000001",
                        "stableTargetId": ["00000000000000"],
                        "hadPrimarySource": ["00000000000000"],
                    }
                ],
                "total": 10,
            },
        ),
        (
            None,
            None,
            ["ExtractedOrganization"],
            1,
            {
                "items": [
                    {
                        "rorId": [],
                        "gndId": [],
                        "wikidataId": [],
                        "identifierInPrimarySource": "robert-koch-institute",
                        "viafId": [],
                        "geprisId": [],
                        "isniId": [],
                        "entityType": "ExtractedOrganization",
                        "identifier": "bFQoRhcVH5DHUC",
                        "stableTargetId": ["bFQoRhcVH5DHUv"],
                        "hadPrimarySource": ["bFQoRhcVH5DHUt"],
                        "officialName": [
                            {"value": "RKI", "language": "de"},
                            {"value": "Robert Koch Institute", "language": "en"},
                        ],
                    },
                ],
                "total": 2,
            },
        ),
        (
            # find exact matches. without the quotes this might also match the second
            # contact point's email `help@contact-point.two`
            '"info@contact-point.one"',
            None,
            None,
            10,
            {
                "items": [
                    {
                        "identifierInPrimarySource": "cp-1",
                        "email": ["info@contact-point.one"],
                        "entityType": "ExtractedContactPoint",
                        "identifier": "bFQoRhcVH5DHUy",
                        "stableTargetId": ["bFQoRhcVH5DHUz"],
                        "hadPrimarySource": ["bFQoRhcVH5DHUr"],
                    }
                ],
                "total": 1,
            },
        ),
        (
            "contact point",
            None,
            None,
            10,
            {
                "items": [
                    {
                        "identifierInPrimarySource": "cp-2",
                        "email": ["help@contact-point.two"],
                        "entityType": "ExtractedContactPoint",
                        "identifier": "bFQoRhcVH5DHUA",
                        "stableTargetId": ["bFQoRhcVH5DHUB"],
                        "hadPrimarySource": ["bFQoRhcVH5DHUr"],
                    },
                    {
                        "identifierInPrimarySource": "cp-1",
                        "email": ["info@contact-point.one"],
                        "entityType": "ExtractedContactPoint",
                        "identifier": "bFQoRhcVH5DHUy",
                        "stableTargetId": ["bFQoRhcVH5DHUz"],
                        "hadPrimarySource": ["bFQoRhcVH5DHUr"],
                    },
                ],
                "total": 2,
            },
        ),
        (
            "RKI",
            None,
            None,
            10,
            {
                "items": [
                    {
                        "rorId": [],
                        "gndId": [],
                        "wikidataId": [],
                        "identifierInPrimarySource": "robert-koch-institute",
                        "viafId": [],
                        "geprisId": [],
                        "isniId": [],
                        "entityType": "ExtractedOrganization",
                        "identifier": "bFQoRhcVH5DHUC",
                        "stableTargetId": ["bFQoRhcVH5DHUv"],
                        "hadPrimarySource": ["bFQoRhcVH5DHUt"],
                        "officialName": [
                            {"value": "RKI", "language": "de"},
                            {"value": "Robert Koch Institute", "language": "en"},
                        ],
                    },
                    {
                        "rorId": [],
                        "gndId": [],
                        "wikidataId": [],
                        "identifierInPrimarySource": "rki",
                        "viafId": [],
                        "geprisId": [],
                        "isniId": [],
                        "entityType": "ExtractedOrganization",
                        "identifier": "bFQoRhcVH5DHUu",
                        "stableTargetId": ["bFQoRhcVH5DHUv"],
                        "hadPrimarySource": ["bFQoRhcVH5DHUr"],
                        "officialName": [
                            {"value": "RKI", "language": "de"},
                            {
                                "value": "Robert Koch Institut ist the best",
                                "language": "de",
                            },
                        ],
                    },
                ],
                "total": 2,
            },
        ),
        (
            "Homepage",
            None,
            None,
            10,
            {
                "items": [
                    {
                        "fundingProgram": [],
                        "identifierInPrimarySource": "a-1",
                        "start": ["2014-08-24"],
                        "theme": ["https://mex.rki.de/item/theme-11"],
                        "entityType": "ExtractedActivity",
                        "activityType": [],
                        "identifier": "bFQoRhcVH5DHUG",
                        "end": [],
                        "stableTargetId": ["bFQoRhcVH5DHUH"],
                        "hadPrimarySource": ["bFQoRhcVH5DHUr"],
                        "contact": [
                            "bFQoRhcVH5DHUz",
                            "bFQoRhcVH5DHUB",
                            "bFQoRhcVH5DHUx",
                        ],
                        "responsibleUnit": ["bFQoRhcVH5DHUx"],
                        "title": [{"value": "Aktivität 1", "language": "de"}],
                        "abstract": [
                            {"value": "An active activity.", "language": "en"},
                            {"value": "Une activité active."},
                        ],
                        "website": [
                            {"title": "Activity Homepage", "url": "https://activity-1"}
                        ],
                    }
                ],
                "total": 1,
            },
        ),
    ],
    ids=[
        "id not found",
        "search not found",
        "no filters",
        "entity type filter",
        "find exact",
        "find fuzzy",
        "find Text",
        "find Link",
    ],
)
@pytest.mark.usefixtures("load_dummy_data")
@pytest.mark.integration
def test_fetch_extracted_items(
    query_string: str | None,
    stable_target_id: str | None,
    entity_type: list[str] | None,
    limit: int,
    expected: dict[str, Any],
) -> None:
    graph = GraphConnector.get()

    result = graph.fetch_extracted_items(
        query_string=query_string,
        stable_target_id=stable_target_id,
        entity_type=entity_type,
        had_primary_source=None,
        skip=0,
        limit=limit,
    )

    assert result.one() == expected


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
        "my-query",
        Identifier.generate(99),
        ["AdditiveFoo", "SubtractiveBar", "PreventiveBatz"],
        None,
        10,
        100,
    )

    assert mocked_graph.call_args_list[-1].args == (
        """\
fetch_extracted_or_rule_items(
    filter_by_query_string=True,
    filter_by_stable_target_id=True,
    filter_by_reference_to_merged_item=False,
    reference_field_name="hadPrimarySource",
)""",
        {
            "labels": [
                "AdditiveFoo",
                "SubtractiveBar",
                "PreventiveBatz",
            ],
            "limit": 100,
            "query_string": "my-query",
            "referenced_identifiers": None,
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


@pytest.mark.parametrize(
    ("query_string", "stable_target_id", "expected"),
    [
        (None, "thisIdDoesNotExist", {"items": [], "total": 0}),
        ("this_search_term_is_not_findable", None, {"items": [], "total": 0}),
        (
            None,
            None,
            {
                "items": [
                    {
                        "email": [],
                        "entityType": "AdditiveOrganizationalUnit",
                        "stableTargetId": ["bFQoRhcVH5DHUF"],
                        "parentUnit": ["bFQoRhcVH5DHUx"],
                        "name": [{"value": "Unit 1.7", "language": "en"}],
                        "website": [
                            {"title": "Unit Homepage", "url": "https://unit-1-7"}
                        ],
                    }
                ],
                "total": 3,
            },
        ),
        (
            '"Unit 1.7"',
            None,
            {
                "items": [
                    {
                        "email": [],
                        "entityType": "AdditiveOrganizationalUnit",
                        "stableTargetId": ["bFQoRhcVH5DHUF"],
                        "parentUnit": ["bFQoRhcVH5DHUx"],
                        "name": [{"value": "Unit 1.7", "language": "en"}],
                        "website": [
                            {"title": "Unit Homepage", "url": "https://unit-1-7"}
                        ],
                    }
                ],
                "total": 1,
            },
        ),
    ],
    ids=[
        "id not found",
        "search not found",
        "no filters",
        "find Link",
    ],
)
@pytest.mark.usefixtures("load_dummy_data", "load_dummy_rule_set")
@pytest.mark.integration
def test_fetch_rule_items(
    query_string: str | None,
    stable_target_id: str | None,
    expected: dict[str, Any],
) -> None:
    graph = GraphConnector.get()

    result = graph.fetch_rule_items(query_string, stable_target_id, None, None, 0, 1)

    assert result.one() == expected


@pytest.mark.integration
def test_fetch_rule_items_empty() -> None:
    graph = GraphConnector.get()

    result = graph.fetch_rule_items(None, "thisIdDoesNotExist", None, None, 0, 1)

    assert result.one() == {"items": [], "total": 0}


@pytest.mark.usefixtures("mocked_query_class")
def test_mocked_graph_fetch_merged_items(mocked_graph: MockedGraph) -> None:
    mocked_graph.return_value = [
        {
            "items": [
                {
                    "_components": [
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
        had_primary_source=[Identifier.generate(100)],
        skip=10,
        limit=100,
    )

    assert mocked_graph.call_args_list[-1].args == (
        """\
fetch_merged_items(
    filter_by_query_string=True,
    filter_by_identifier=True,
    filter_by_reference_to_merged_item=True,
    reference_field_name="hadPrimarySource",
)""",
        {
            "labels": [
                "MergedFoo",
                "MergedBar",
                "MergedBatz",
            ],
            "limit": 100,
            "query_string": "my-query",
            "referenced_identifiers": ["bFQoRhcVH5DHV2"],
            "skip": 10,
            "identifier": "bFQoRhcVH5DHV1",
        },
    )

    assert result.one() == {
        "items": [
            {
                "_components": [
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


@pytest.mark.parametrize(
    (
        "query_string",
        "identifier",
        "entity_type",
        "had_primary_source",
        "limit",
        "expected",
    ),
    [
        (None, "thisIdDoesNotExist", None, None, 10, {"items": [], "total": 0}),
        (
            "this_search_term_is_not_findable",
            None,
            None,
            None,
            10,
            {"items": [], "total": 0},
        ),
        (
            None,
            None,
            None,
            None,
            1,
            {
                "items": [
                    {
                        "_components": [
                            {
                                "identifierInPrimarySource": "mex",
                                "entityType": "ExtractedPrimarySource",
                                "identifier": "00000000000001",
                                "stableTargetId": ["00000000000000"],
                                "hadPrimarySource": ["00000000000000"],
                            }
                        ],
                        "entityType": "MergedPrimarySource",
                        "identifier": "00000000000000",
                    }
                ],
                "total": 9,
            },
        ),
        (
            None,
            None,
            ["MergedOrganization"],
            None,
            1,
            {
                "items": [
                    {
                        "_components": [
                            {
                                "rorId": [],
                                "gndId": [],
                                "wikidataId": [],
                                "identifierInPrimarySource": "robert-koch-institute",
                                "viafId": [],
                                "geprisId": [],
                                "isniId": [],
                                "entityType": "ExtractedOrganization",
                                "identifier": "bFQoRhcVH5DHUC",
                                "stableTargetId": ["bFQoRhcVH5DHUv"],
                                "hadPrimarySource": ["bFQoRhcVH5DHUt"],
                                "officialName": [
                                    {"value": "RKI", "language": "de"},
                                    {
                                        "value": "Robert Koch Institute",
                                        "language": "en",
                                    },
                                ],
                            },
                            {
                                "rorId": [],
                                "gndId": [],
                                "wikidataId": [],
                                "identifierInPrimarySource": "rki",
                                "viafId": [],
                                "geprisId": [],
                                "isniId": [],
                                "entityType": "ExtractedOrganization",
                                "identifier": "bFQoRhcVH5DHUu",
                                "stableTargetId": ["bFQoRhcVH5DHUv"],
                                "hadPrimarySource": ["bFQoRhcVH5DHUr"],
                                "officialName": [
                                    {"value": "RKI", "language": "de"},
                                    {
                                        "value": "Robert Koch Institut ist the best",
                                        "language": "de",
                                    },
                                ],
                            },
                        ],
                        "entityType": "MergedOrganization",
                        "identifier": "bFQoRhcVH5DHUv",
                    }
                ],
                "total": 1,
            },
        ),
        (
            None,
            None,
            None,
            ["bFQoRhcVH5DHUt"],
            1,
            {
                "items": [
                    {
                        "_components": [
                            {
                                "email": [],
                                "entityType": "ExtractedOrganizationalUnit",
                                "hadPrimarySource": ["bFQoRhcVH5DHUt"],
                                "identifier": "bFQoRhcVH5DHUE",
                                "identifierInPrimarySource": "ou-1.6",
                                "name": [{"language": "en", "value": "Unit 1.6"}],
                                "parentUnit": ["bFQoRhcVH5DHUx"],
                                "stableTargetId": ["bFQoRhcVH5DHUF"],
                                "unitOf": ["bFQoRhcVH5DHUv"],
                            },
                            {
                                "email": [],
                                "entityType": "AdditiveOrganizationalUnit",
                                "name": [{"language": "en", "value": "Unit 1.7"}],
                                "parentUnit": ["bFQoRhcVH5DHUx"],
                                "stableTargetId": ["bFQoRhcVH5DHUF"],
                                "website": [
                                    {
                                        "title": "Unit Homepage",
                                        "url": "https://unit-1-7",
                                    }
                                ],
                            },
                            {
                                "entityType": "PreventiveOrganizationalUnit",
                                "stableTargetId": ["bFQoRhcVH5DHUF"],
                            },
                            {
                                "email": [],
                                "entityType": "SubtractiveOrganizationalUnit",
                                "stableTargetId": ["bFQoRhcVH5DHUF"],
                            },
                        ],
                        "entityType": "MergedOrganizationalUnit",
                        "identifier": "bFQoRhcVH5DHUF",
                    }
                ],
                "total": 3,
            },
        ),
        (
            "Unit",
            None,
            None,
            ["bFQoRhcVH5DHUt"],
            1,
            {
                "items": [
                    {
                        "_components": [
                            {
                                "email": [],
                                "entityType": "ExtractedOrganizationalUnit",
                                "hadPrimarySource": ["bFQoRhcVH5DHUt"],
                                "identifier": "bFQoRhcVH5DHUE",
                                "identifierInPrimarySource": "ou-1.6",
                                "name": [{"language": "en", "value": "Unit 1.6"}],
                                "parentUnit": ["bFQoRhcVH5DHUx"],
                                "stableTargetId": ["bFQoRhcVH5DHUF"],
                                "unitOf": ["bFQoRhcVH5DHUv"],
                            },
                            {
                                "email": [],
                                "entityType": "AdditiveOrganizationalUnit",
                                "name": [{"language": "en", "value": "Unit 1.7"}],
                                "parentUnit": ["bFQoRhcVH5DHUx"],
                                "stableTargetId": ["bFQoRhcVH5DHUF"],
                                "website": [
                                    {
                                        "title": "Unit Homepage",
                                        "url": "https://unit-1-7",
                                    }
                                ],
                            },
                            {
                                "entityType": "PreventiveOrganizationalUnit",
                                "stableTargetId": ["bFQoRhcVH5DHUF"],
                            },
                            {
                                "email": [],
                                "entityType": "SubtractiveOrganizationalUnit",
                                "stableTargetId": ["bFQoRhcVH5DHUF"],
                            },
                        ],
                        "entityType": "MergedOrganizationalUnit",
                        "identifier": "bFQoRhcVH5DHUF",
                    }
                ],
                "total": 2,
            },
        ),
        (
            # find exact matches. without the quotes this might also match the second
            # contact point's email `help@contact-point.two`
            '"info@contact-point.one"',
            None,
            None,
            None,
            10,
            {
                "items": [
                    {
                        "_components": [
                            {
                                "identifierInPrimarySource": "cp-1",
                                "email": ["info@contact-point.one"],
                                "entityType": "ExtractedContactPoint",
                                "identifier": "bFQoRhcVH5DHUy",
                                "stableTargetId": ["bFQoRhcVH5DHUz"],
                                "hadPrimarySource": ["bFQoRhcVH5DHUr"],
                            }
                        ],
                        "entityType": "MergedContactPoint",
                        "identifier": "bFQoRhcVH5DHUz",
                    }
                ],
                "total": 1,
            },
        ),
        (
            "contact point",
            None,
            None,
            None,
            10,
            {
                "items": [
                    {
                        "_components": [
                            {
                                "identifierInPrimarySource": "cp-2",
                                "email": ["help@contact-point.two"],
                                "entityType": "ExtractedContactPoint",
                                "identifier": "bFQoRhcVH5DHUA",
                                "stableTargetId": ["bFQoRhcVH5DHUB"],
                                "hadPrimarySource": ["bFQoRhcVH5DHUr"],
                            }
                        ],
                        "entityType": "MergedContactPoint",
                        "identifier": "bFQoRhcVH5DHUB",
                    },
                    {
                        "_components": [
                            {
                                "identifierInPrimarySource": "cp-1",
                                "email": ["info@contact-point.one"],
                                "entityType": "ExtractedContactPoint",
                                "identifier": "bFQoRhcVH5DHUy",
                                "stableTargetId": ["bFQoRhcVH5DHUz"],
                                "hadPrimarySource": ["bFQoRhcVH5DHUr"],
                            }
                        ],
                        "entityType": "MergedContactPoint",
                        "identifier": "bFQoRhcVH5DHUz",
                    },
                ],
                "total": 2,
            },
        ),
        (
            "RKI",
            None,
            None,
            None,
            10,
            {
                "items": [
                    {
                        "_components": [
                            {
                                "rorId": [],
                                "gndId": [],
                                "wikidataId": [],
                                "identifierInPrimarySource": "robert-koch-institute",
                                "viafId": [],
                                "geprisId": [],
                                "isniId": [],
                                "entityType": "ExtractedOrganization",
                                "identifier": "bFQoRhcVH5DHUC",
                                "stableTargetId": ["bFQoRhcVH5DHUv"],
                                "hadPrimarySource": ["bFQoRhcVH5DHUt"],
                                "officialName": [
                                    {"value": "RKI", "language": "de"},
                                    {
                                        "value": "Robert Koch Institute",
                                        "language": "en",
                                    },
                                ],
                            },
                            {
                                "rorId": [],
                                "gndId": [],
                                "wikidataId": [],
                                "identifierInPrimarySource": "rki",
                                "viafId": [],
                                "geprisId": [],
                                "isniId": [],
                                "entityType": "ExtractedOrganization",
                                "identifier": "bFQoRhcVH5DHUu",
                                "stableTargetId": ["bFQoRhcVH5DHUv"],
                                "hadPrimarySource": ["bFQoRhcVH5DHUr"],
                                "officialName": [
                                    {"value": "RKI", "language": "de"},
                                    {
                                        "value": "Robert Koch Institut ist the best",
                                        "language": "de",
                                    },
                                ],
                            },
                        ],
                        "entityType": "MergedOrganization",
                        "identifier": "bFQoRhcVH5DHUv",
                    }
                ],
                "total": 1,
            },
        ),
        (
            "Homepage",
            None,
            None,
            None,
            10,
            {
                "items": [
                    {
                        "_components": [
                            {
                                "identifierInPrimarySource": "ou-1.6",
                                "email": [],
                                "entityType": "ExtractedOrganizationalUnit",
                                "identifier": "bFQoRhcVH5DHUE",
                                "stableTargetId": ["bFQoRhcVH5DHUF"],
                                "hadPrimarySource": ["bFQoRhcVH5DHUt"],
                                "parentUnit": ["bFQoRhcVH5DHUx"],
                                "unitOf": ["bFQoRhcVH5DHUv"],
                                "name": [{"value": "Unit 1.6", "language": "en"}],
                            },
                            {
                                "email": [],
                                "entityType": "AdditiveOrganizationalUnit",
                                "stableTargetId": ["bFQoRhcVH5DHUF"],
                                "parentUnit": ["bFQoRhcVH5DHUx"],
                                "name": [{"value": "Unit 1.7", "language": "en"}],
                                "website": [
                                    {
                                        "title": "Unit Homepage",
                                        "url": "https://unit-1-7",
                                    }
                                ],
                            },
                            {
                                "entityType": "PreventiveOrganizationalUnit",
                                "stableTargetId": ["bFQoRhcVH5DHUF"],
                            },
                            {
                                "email": [],
                                "entityType": "SubtractiveOrganizationalUnit",
                                "stableTargetId": ["bFQoRhcVH5DHUF"],
                            },
                        ],
                        "entityType": "MergedOrganizationalUnit",
                        "identifier": "bFQoRhcVH5DHUF",
                    },
                    {
                        "_components": [
                            {
                                "fundingProgram": [],
                                "identifierInPrimarySource": "a-1",
                                "start": ["2014-08-24"],
                                "theme": ["https://mex.rki.de/item/theme-11"],
                                "entityType": "ExtractedActivity",
                                "activityType": [],
                                "identifier": "bFQoRhcVH5DHUG",
                                "end": [],
                                "stableTargetId": ["bFQoRhcVH5DHUH"],
                                "hadPrimarySource": ["bFQoRhcVH5DHUr"],
                                "contact": [
                                    "bFQoRhcVH5DHUz",
                                    "bFQoRhcVH5DHUB",
                                    "bFQoRhcVH5DHUx",
                                ],
                                "responsibleUnit": ["bFQoRhcVH5DHUx"],
                                "title": [{"value": "Aktivität 1", "language": "de"}],
                                "abstract": [
                                    {"value": "An active activity.", "language": "en"},
                                    {"value": "Une activité active."},
                                ],
                                "website": [
                                    {
                                        "title": "Activity Homepage",
                                        "url": "https://activity-1",
                                    }
                                ],
                            }
                        ],
                        "entityType": "MergedActivity",
                        "identifier": "bFQoRhcVH5DHUH",
                    },
                ],
                "total": 2,
            },
        ),
    ],
    ids=[
        "id not found",
        "search not found",
        "no filters",
        "entity type filter",
        "had primary source filter",
        "had primary source filter and filter by query",
        "find exact",
        "find fuzzy",
        "find Text",
        "find Link",
    ],
)
@pytest.mark.usefixtures("load_dummy_data", "load_dummy_rule_set")
@pytest.mark.integration
def test_fetch_merged_items(  # noqa: PLR0913
    query_string: str | None,
    identifier: str | None,
    entity_type: list[str] | None,
    had_primary_source: list[str] | None,
    limit: int,
    expected: dict[str, Any],
) -> None:
    graph = GraphConnector.get()

    result = graph.fetch_merged_items(
        query_string=query_string,
        identifier=identifier,
        entity_type=entity_type,
        had_primary_source=had_primary_source,
        skip=0,
        limit=limit,
    )

    assert result.one() == expected


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
    graph = GraphConnector.get()

    assert graph.exists_merged_item(stable_target_id, stem_types) == exists


@pytest.mark.usefixtures("mocked_query_class")
def test_mocked_graph_merge_item(
    mocked_graph: MockedGraph, dummy_data: dict[str, AnyExtractedModel]
) -> None:
    extracted_organizational_unit = dummy_data["organizational_unit_1"]
    graph = GraphConnector.get()
    with mocked_graph.session as session:
        graph._merge_item(
            session,
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
        {
            "edges": [
                "hadPrimarySource {position: 0}",
                "unitOf {position: 0}",
                "stableTargetId {position: 0}",
            ]
        },
    ]
    graph = GraphConnector.get()

    extracted_organizational_unit = cast(
        "ExtractedOrganizationalUnit", dummy_data["organizational_unit_1"]
    )
    with mocked_graph.session as session:
        graph._merge_edges(
            session,
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
def test_mocked_graph_merge_edges_fails_inconsistent(
    mocked_graph: MockedGraph, dummy_data: dict[str, AnyExtractedModel]
) -> None:
    mocked_graph.return_value = [{"edges": ["stableTargetId {position: 0}"]}]
    graph = GraphConnector.get()
    extracted_organizational_unit = dummy_data["organizational_unit_1"]

    with pytest.raises(  # noqa: SIM117
        InconsistentGraphError,
        match=re.escape("""InconsistentGraphError: failed to merge 2 edges: \
(:ExtractedOrganizationalUnit {identifier: "gIyDlXYbq0JwItPRU0NcFN"})-[:hadPrimarySource {position: 0}]->({identifier: "bbTqJnQc3TA8dBJmLMBimb"}), \
(:ExtractedOrganizationalUnit {identifier: "gIyDlXYbq0JwItPRU0NcFN"})-[:unitOf {position: 0}]->({identifier: "gGsD37g2jyzxedMSHozDZa"})\
"""),
    ):
        with mocked_graph.session as session:
            graph._merge_edges(
                session,
                extracted_organizational_unit,
                extracted_organizational_unit.stableTargetId,
                identifier=extracted_organizational_unit.identifier,
            )


@pytest.mark.integration
@pytest.mark.usefixtures("load_dummy_data")
def test_graph_merge_edges_fails_inconsistent(
    load_dummy_data: dict[str, AnyExtractedModel],
) -> None:
    graph = GraphConnector.get()
    consistent_org = ExtractedOrganization(
        identifierInPrimarySource="10000000",
        hadPrimarySource=load_dummy_data["primary_source_2"].stableTargetId,
        officialName=[Text(value="Consistent Org", language=TextLanguage.EN)],
    )
    inconsistent_unit = ExtractedOrganizationalUnit(
        identifierInPrimarySource="199999",
        hadPrimarySource="whatPrimarySource",  # inconsistent identifier
        name=[Text(value="Inconsistent Unit", language=TextLanguage.EN)],
        unitOf=[
            load_dummy_data["organization_1"].stableTargetId,  # consistent identifier
            "whatOrganizationalUnit",  # inconsistent identifier
            consistent_org.stableTargetId,  # consistent identifier
        ],
    )

    with pytest.raises(
        InconsistentGraphError,
        match=re.escape("""InconsistentGraphError: failed to merge 2 edges: \
(:ExtractedOrganizationalUnit {identifier: "bFQoRhcVH5DHUK"})-[:hadPrimarySource {position: 0}]->({identifier: "whatPrimarySource"}), \
(:ExtractedOrganizationalUnit {identifier: "bFQoRhcVH5DHUK"})-[:unitOf {position: 1}]->({identifier: "whatOrganizationalUnit"})\
"""),
    ):
        graph.ingest([consistent_org, inconsistent_unit])


@pytest.mark.usefixtures("mocked_query_class")
def test_mocked_graph_merge_edges_fails_unexpected(
    mocked_graph: MockedGraph, dummy_data: dict[str, AnyExtractedModel]
) -> None:
    mocked_graph.return_value = [
        {
            "edges": [
                "stableTargetId {position: 0}",
                "unitOf {position: 0}",
                "stableTargetId {position: 0}",
                "hadPrimarySource {position: 0}",
                "newEdgeWhoDis {position: 0}",
            ]
        },
    ]
    graph = GraphConnector.get()
    extracted_organizational_unit = dummy_data["organizational_unit_1"]

    with pytest.raises(  # noqa: SIM117
        RuntimeError,
        match=re.escape(
            "merged 1 edges more than expected: newEdgeWhoDis {position: 0}"
        ),
    ):
        with mocked_graph.session as session:
            graph._merge_edges(
                session,
                extracted_organizational_unit,
                extracted_organizational_unit.stableTargetId,
                identifier=extracted_organizational_unit.identifier,
            )


@pytest.mark.usefixtures("mocked_query_class")
def test_mocked_graph_ingests_rule_set(
    mocked_graph: MockedGraph,
    organizational_unit_rule_set_response: OrganizationalUnitRuleSetResponse,
) -> None:
    mocked_graph.side_effect = [
        [{"current": {}, "$comment": "additive item"}],
        [{"current": {}, "$comment": "subtractive item"}],
        [{"current": {}, "$comment": "preventive item"}],
        [
            {
                "edges": ["parentUnit {position: 0}", "stableTargetId {position: 0}"],
                "$comment": "additive edges",
            }
        ],
        [{"edges": ["stableTargetId {position: 0}"], "$comment": "subtractive edges"}],
        [{"edges": ["stableTargetId {position: 0}"], "$comment": "preventive edges"}],
    ]
    graph = GraphConnector.get()
    graph.ingest([organizational_unit_rule_set_response])

    assert len(mocked_graph.call_args_list) == 6
    assert mocked_graph.call_args_list[2].args == (
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
    assert mocked_graph.call_args_list[5].args == (
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


def test_mocked_graph_ingests_extracted_models(
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
                "edges": [
                    "hadPrimarySource {position: 0}",
                    "stableTargetId {position: 0}",
                ],
                "$comment": "mock response for PrimarySource ps-1 edges",
            },
        ],
        [
            {
                "edges": [
                    "hadPrimarySource {position: 0}",
                    "stableTargetId {position: 0}",
                ],
                "$comment": "mock response for PrimarySource ps-2 edges",
            },
        ],
        [
            {
                "edges": [
                    "hadPrimarySource {position: 0}",
                    "stableTargetId {position: 0}",
                ],
                "$comment": "mock response for ContactPoint cp-1 edges",
            },
        ],
        [
            {
                "edges": [
                    "hadPrimarySource {position: 0}",
                    "stableTargetId {position: 0}",
                ],
                "$comment": "mock response for ContactPoint cp-2 edges",
            },
        ],
        [
            {
                "edges": [
                    "hadPrimarySource {position: 0}",
                    "stableTargetId {position: 0}",
                ],
                "$comment": "mock response for Organization rki edges",
            }
        ],
        [
            {
                "edges": [
                    "hadPrimarySource {position: 0}",
                    "stableTargetId {position: 0}",
                ],
                "$comment": "mock response for Organization robert-koch-institute edges",
            }
        ],
        [
            {
                "edges": [
                    "hadPrimarySource {position: 0}",
                    "unitOf {position: 0}",
                    "stableTargetId {position: 0}",
                ],
                "$comment": "mock response for OrganizationalUnit ou-1 edges",
            }
        ],
        [
            {
                "edges": [
                    "hadPrimarySource {position: 0}",
                    "parentUnit {position: 0}",
                    "unitOf {position: 0}",
                    "stableTargetId {position: 0}",
                ],
                "$comment": "mock response for OrganizationalUnit ou-1.6 edges",
            }
        ],
        [
            {
                "edges": [
                    "hadPrimarySource {position: 0}",
                    "contact {position: 0}",
                    "contact {position: 1}",
                    "contact {position: 2}",
                    "responsibleUnit {position: 0}",
                    "stableTargetId {position: 0}",
                ],
                "$comment": "mock response for Activity a-1 edges",
            },
        ],
    ]

    dummy_items = list(dummy_data.values())
    graph = GraphConnector.get()
    graph.ingest(dummy_items)

    assert len(mocked_graph.call_args_list) == 18


@pytest.mark.integration
def test_connector_flush_fails(monkeypatch: MonkeyPatch) -> None:
    settings = BackendSettings.get()

    monkeypatch.setattr(settings, "debug", False)
    graph = GraphConnector.get()

    with pytest.raises(
        MExError, match="database flush was attempted outside of debug mode"
    ):
        graph.flush()


@pytest.mark.integration
@pytest.mark.usefixtures("load_dummy_data")
def test_connector_flush(monkeypatch: MonkeyPatch) -> None:
    assert len(get_graph()) >= 10

    settings = BackendSettings.get()
    monkeypatch.setattr(settings, "debug", True)
    graph = GraphConnector.get()
    graph.flush()

    assert len(get_graph()) == 0


@pytest.mark.integration
def test_connector_ingest() -> None:
    import time

    from mex.artificial.helpers import generate_artificial_extracted_items
    from mex.common.logging import logger

    items = generate_artificial_extracted_items(
        locale=["de", "en"],
        seed=None,
        count=20,
        chattiness=20,
        stem_types=list(EXTRACTED_MODEL_CLASSES_BY_NAME),
    )
    connector = GraphConnector.get()

    v1_times = []
    v2_times = []

    for _ in range(3):
        t0 = time.time()
        connector.ingest(items)
        t1 = time.time() - t0
        v1_times.append(t1)
        result = connector.commit("MATCH (n) RETURN count(n)")
        logger.info(
            f"v1 finished in {t1} posting {result} items",
        )
        connector.flush()

    for _ in range(3):
        t0 = time.time()
        connector.ingest_v2(items)
        t1 = time.time() - t0
        v2_times.append(t1)
        result = connector.commit("MATCH (n) RETURN count(n)")
        logger.info(
            f"v2 finished in {t1} posting {result} items",
        )
        connector.flush()

    logger.info(v1_times, v2_times)
