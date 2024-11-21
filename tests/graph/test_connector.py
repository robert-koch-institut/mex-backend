from collections.abc import Callable, Iterable
from typing import Any
from unittest.mock import Mock

import pytest
from black import Mode, format_str
from pytest import MonkeyPatch

from mex.backend.graph import connector as connector_module
from mex.backend.graph.connector import MEX_EXTRACTED_PRIMARY_SOURCE, GraphConnector
from mex.backend.graph.query import QueryBuilder
from mex.common.exceptions import MExError
from mex.common.models import (
    MEX_PRIMARY_SOURCE_IDENTIFIER,
    MEX_PRIMARY_SOURCE_IDENTIFIER_IN_PRIMARY_SOURCE,
    MEX_PRIMARY_SOURCE_STABLE_TARGET_ID,
    AnyExtractedModel,
    OrganizationalUnitRuleSetRequest,
    OrganizationalUnitRuleSetResponse,
)
from mex.common.types import Identifier
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


def test_mocked_graph_commit_raises_error(mocked_graph: MockedGraph) -> None:
    mocked_graph.run.side_effect = Exception("query failed")
    connector = GraphConnector.get()
    with pytest.raises(Exception, match="query failed"):
        connector.commit("RETURN 1;")


@pytest.mark.usefixtures("mocked_query_builder")
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


@pytest.mark.parametrize(
    ("args", "expected"),
    [
        (
            ("Unit", None, None, 0, 10),
            {
                "items": [
                    {
                        "identifierInPrimarySource": "ou-1.6",
                        "identifier": "bFQoRhcVH5DHUA",
                        "entityType": "ExtractedOrganizationalUnit",
                        "email": [],
                        "hadPrimarySource": ["bFQoRhcVH5DHUt"],
                        "parentUnit": ["bFQoRhcVH5DHUv"],
                        "stableTargetId": ["bFQoRhcVH5DHUB"],
                        "name": [{"language": "en", "value": "Unit 1.6"}],
                    },
                    {
                        "identifierInPrimarySource": "ou-1",
                        "identifier": "bFQoRhcVH5DHUu",
                        "entityType": "ExtractedOrganizationalUnit",
                        "email": [],
                        "hadPrimarySource": ["bFQoRhcVH5DHUt"],
                        "stableTargetId": ["bFQoRhcVH5DHUv"],
                        "name": [{"language": "en", "value": "Unit 1"}],
                    },
                ],
                "total": 2,
            },
        ),
        (
            (None, None, None, 0, 1),
            {
                "items": [
                    {
                        "entityType": MEX_EXTRACTED_PRIMARY_SOURCE.entityType,
                        "hadPrimarySource": [
                            MEX_EXTRACTED_PRIMARY_SOURCE.hadPrimarySource
                        ],
                        "identifier": MEX_EXTRACTED_PRIMARY_SOURCE.identifier,
                        "identifierInPrimarySource": MEX_EXTRACTED_PRIMARY_SOURCE.identifierInPrimarySource,
                        "stableTargetId": [MEX_EXTRACTED_PRIMARY_SOURCE.stableTargetId],
                    }
                ],
                "total": 10,
            },
        ),
        (
            ("Cool", None, None, 0, 10),
            {
                "items": [
                    {
                        "identifier": "bFQoRhcVH5DHUs",
                        "identifierInPrimarySource": "ps-2",
                        "entityType": "ExtractedPrimarySource",
                        "version": "Cool Version v2.13",
                        "hadPrimarySource": ["00000000000000"],
                        "stableTargetId": ["bFQoRhcVH5DHUt"],
                    }
                ],
                "total": 1,
            },
        ),
        (
            ("1.6", None, None, 0, 10),
            {
                "items": [
                    {
                        "identifierInPrimarySource": "ou-1.6",
                        "identifier": "bFQoRhcVH5DHUA",
                        "entityType": "ExtractedOrganizationalUnit",
                        "email": [],
                        "hadPrimarySource": ["bFQoRhcVH5DHUt"],
                        "parentUnit": ["bFQoRhcVH5DHUv"],
                        "stableTargetId": ["bFQoRhcVH5DHUB"],
                        "name": [{"language": "en", "value": "Unit 1.6"}],
                    },
                ],
                "total": 1,
            },
        ),
        (
            (None, "thisIdDoesNotExist", None, 0, 1),
            {
                "items": [],
                "total": 0,
            },
        ),
        (
            (None, "bFQoRhcVH5DHUB", None, 0, 10),
            {
                "items": [
                    {
                        "identifierInPrimarySource": "ou-1.6",
                        "identifier": "bFQoRhcVH5DHUA",
                        "entityType": "ExtractedOrganizationalUnit",
                        "email": [],
                        "hadPrimarySource": ["bFQoRhcVH5DHUt"],
                        "parentUnit": ["bFQoRhcVH5DHUv"],
                        "stableTargetId": ["bFQoRhcVH5DHUB"],
                        "name": [{"language": "en", "value": "Unit 1.6"}],
                    },
                ],
                "total": 1,
            },
        ),
        (
            (None, None, ["ExtractedActivity"], 0, 10),
            {
                "items": [
                    {
                        "identifier": "bFQoRhcVH5DHUC",
                        "identifierInPrimarySource": "a-1",
                        "fundingProgram": [],
                        "entityType": "ExtractedActivity",
                        "start": ["2014-08-24"],
                        "end": [],
                        "theme": ["https://mex.rki.de/item/theme-3"],
                        "activityType": [],
                        "contact": [
                            "bFQoRhcVH5DHUx",
                            "bFQoRhcVH5DHUz",
                            "bFQoRhcVH5DHUv",
                        ],
                        "responsibleUnit": ["bFQoRhcVH5DHUv"],
                        "hadPrimarySource": ["bFQoRhcVH5DHUr"],
                        "stableTargetId": ["bFQoRhcVH5DHUD"],
                        "abstract": [
                            {"language": "en", "value": "An active activity."},
                            {"value": "Une activité active."},
                        ],
                        "website": [
                            {"title": "Activity Homepage", "url": "https://activity-1"}
                        ],
                        "title": [{"language": "de", "value": "Aktivität 1"}],
                    }
                ],
                "total": 1,
            },
        ),
    ],
    ids=[
        "search in nested nodes",
        "no filter by anything but pagination is 1",
        "search in inline strings",
        "search in nested nodes and inline strings",
        "search for non-existent stableTargetId",
        "search for existing stableTargetId",
        "search for specific entityType",
    ],
)
@pytest.mark.usefixtures("load_dummy_data")
@pytest.mark.integration
def test_fetch_extracted_items(args: Iterable[Any], expected: dict[str, Any]) -> None:
    connector = GraphConnector.get()
    result = connector.fetch_extracted_items(*args)
    assert result.one() == expected


@pytest.mark.usefixtures("mocked_query_builder")
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


@pytest.mark.usefixtures("load_dummy_data")
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
                "stableTargetId": ["bFQoRhcVH5DHUB"],
            }
        ],
        "total": 3,
    }


@pytest.mark.integration
def test_fetch_rule_items_empty() -> None:
    connector = GraphConnector.get()

    result = connector.fetch_rule_items(None, "thisIdDoesNotExist", None, 0, 1)

    assert result.one() == {"items": [], "total": 0}


@pytest.mark.usefixtures("mocked_query_builder")
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
        stable_target_id=Identifier.generate(99),
        entity_type=["MergedFoo", "MergedBar", "MergedBatz"],
        skip=10,
        limit=100,
    )

    assert mocked_graph.call_args_list[-1].args == (
        """\
fetch_merged_items(
    filter_by_query_string=True, filter_by_stable_target_id=True
)""",
        {
            "labels": [
                "MergedFoo",
                "MergedBar",
                "MergedBatz",
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


@pytest.mark.parametrize(
    ("args", "expected"),
    [
        (
            (None, None, None, 1, 1),
            {
                "items": [
                    {
                        "components": [
                            {
                                "email": [],
                                "entityType": "ExtractedOrganizationalUnit",
                                "hadPrimarySource": ["bFQoRhcVH5DHUt"],
                                "identifier": "bFQoRhcVH5DHUA",
                                "identifierInPrimarySource": "ou-1.6",
                                "name": [{"language": "en", "value": "Unit 1.6"}],
                                "parentUnit": ["bFQoRhcVH5DHUv"],
                                "stableTargetId": ["bFQoRhcVH5DHUB"],
                            },
                            {
                                "entityType": "PreventiveOrganizationalUnit",
                                "stableTargetId": ["bFQoRhcVH5DHUB"],
                            },
                            {
                                "email": [],
                                "entityType": "SubtractiveOrganizationalUnit",
                                "stableTargetId": ["bFQoRhcVH5DHUB"],
                            },
                            {
                                "email": [],
                                "entityType": "AdditiveOrganizationalUnit",
                                "name": [{"language": "en", "value": "Unit 1.7"}],
                                "parentUnit": ["bFQoRhcVH5DHUv"],
                                "stableTargetId": ["bFQoRhcVH5DHUB"],
                                "website": [
                                    {
                                        "title": "Unit Homepage",
                                        "url": "https://unit-1-7",
                                    }
                                ],
                            },
                        ],
                        "entityType": "MergedOrganizationalUnit",
                        "identifier": "bFQoRhcVH5DHUB",
                    }
                ],
                "total": 9,
            },
        ),
        (
            ("Robert Koch Institut ist the best", None, None, 0, 10),
            {
                "items": [
                    {
                        "identifier": "bFQoRhcVH5DHUF",
                        "components": [
                            {
                                "geprisId": [],
                                "identifier": "bFQoRhcVH5DHUE",
                                "identifierInPrimarySource": "rki",
                                "viafId": [],
                                "isniId": [],
                                "entityType": "ExtractedOrganization",
                                "gndId": [],
                                "wikidataId": [],
                                "rorId": [],
                                "hadPrimarySource": ["bFQoRhcVH5DHUr"],
                                "stableTargetId": ["bFQoRhcVH5DHUF"],
                                "officialName": [
                                    {"value": "RKI"},
                                    {
                                        "language": "de",
                                        "value": "Robert Koch Institut ist the best",
                                    },
                                ],
                            },
                            {
                                "geprisId": [],
                                "identifier": "bFQoRhcVH5DHUG",
                                "identifierInPrimarySource": "robert-koch-institute",
                                "viafId": [],
                                "isniId": [],
                                "entityType": "ExtractedOrganization",
                                "gndId": [],
                                "wikidataId": [],
                                "rorId": [],
                                "stableTargetId": ["bFQoRhcVH5DHUF"],
                                "hadPrimarySource": ["bFQoRhcVH5DHUt"],
                                "officialName": [
                                    {"value": "RKI"},
                                    {
                                        "language": "en",
                                        "value": "Robert Koch Institute",
                                    },
                                ],
                            },
                        ],
                        "entityType": "MergedOrganization",
                    }
                ],
                "total": 1,
            },
        ),
        ((None, "thisIdDoesNotExist", None, 0, 1), {"items": [], "total": 0}),
        (
            (None, "bFQoRhcVH5DHUF", None, 0, 10),
            {
                "items": [
                    {
                        "identifier": "bFQoRhcVH5DHUF",
                        "components": [
                            {
                                "geprisId": [],
                                "identifier": "bFQoRhcVH5DHUE",
                                "identifierInPrimarySource": "rki",
                                "viafId": [],
                                "isniId": [],
                                "entityType": "ExtractedOrganization",
                                "gndId": [],
                                "wikidataId": [],
                                "rorId": [],
                                "hadPrimarySource": ["bFQoRhcVH5DHUr"],
                                "stableTargetId": ["bFQoRhcVH5DHUF"],
                                "officialName": [
                                    {"value": "RKI"},
                                    {
                                        "language": "de",
                                        "value": "Robert Koch Institut ist the best",
                                    },
                                ],
                            },
                            {
                                "geprisId": [],
                                "identifier": "bFQoRhcVH5DHUG",
                                "identifierInPrimarySource": "robert-koch-institute",
                                "viafId": [],
                                "isniId": [],
                                "entityType": "ExtractedOrganization",
                                "gndId": [],
                                "wikidataId": [],
                                "rorId": [],
                                "stableTargetId": ["bFQoRhcVH5DHUF"],
                                "hadPrimarySource": ["bFQoRhcVH5DHUt"],
                                "officialName": [
                                    {"value": "RKI"},
                                    {
                                        "language": "en",
                                        "value": "Robert Koch Institute",
                                    },
                                ],
                            },
                        ],
                        "entityType": "MergedOrganization",
                    }
                ],
                "total": 1,
            },
        ),
        (
            (None, None, ["MergedOrganizationalUnit"], 0, 10),
            {
                "items": [
                    {
                        "identifier": "bFQoRhcVH5DHUB",
                        "components": [
                            {
                                "identifierInPrimarySource": "ou-1.6",
                                "identifier": "bFQoRhcVH5DHUA",
                                "entityType": "ExtractedOrganizationalUnit",
                                "email": [],
                                "hadPrimarySource": ["bFQoRhcVH5DHUt"],
                                "parentUnit": ["bFQoRhcVH5DHUv"],
                                "stableTargetId": ["bFQoRhcVH5DHUB"],
                                "name": [{"language": "en", "value": "Unit 1.6"}],
                            },
                            {
                                "entityType": "PreventiveOrganizationalUnit",
                                "stableTargetId": ["bFQoRhcVH5DHUB"],
                            },
                            {
                                "entityType": "SubtractiveOrganizationalUnit",
                                "email": [],
                                "stableTargetId": ["bFQoRhcVH5DHUB"],
                            },
                            {
                                "entityType": "AdditiveOrganizationalUnit",
                                "email": [],
                                "parentUnit": ["bFQoRhcVH5DHUv"],
                                "stableTargetId": ["bFQoRhcVH5DHUB"],
                                "name": [{"language": "en", "value": "Unit 1.7"}],
                                "website": [
                                    {
                                        "title": "Unit Homepage",
                                        "url": "https://unit-1-7",
                                    }
                                ],
                            },
                        ],
                        "entityType": "MergedOrganizationalUnit",
                    },
                    {
                        "identifier": "bFQoRhcVH5DHUv",
                        "components": [
                            {
                                "identifierInPrimarySource": "ou-1",
                                "identifier": "bFQoRhcVH5DHUu",
                                "entityType": "ExtractedOrganizationalUnit",
                                "email": [],
                                "hadPrimarySource": ["bFQoRhcVH5DHUt"],
                                "stableTargetId": ["bFQoRhcVH5DHUv"],
                                "name": [{"language": "en", "value": "Unit 1"}],
                            }
                        ],
                        "entityType": "MergedOrganizationalUnit",
                    },
                ],
                "total": 2,
            },
        ),
        (
            ("version", None, None, 0, 10),
            {
                "items": [
                    {
                        "identifier": "bFQoRhcVH5DHUt",
                        "components": [
                            {
                                "identifier": "bFQoRhcVH5DHUs",
                                "identifierInPrimarySource": "ps-2",
                                "entityType": "ExtractedPrimarySource",
                                "version": "Cool Version v2.13",
                                "hadPrimarySource": ["00000000000000"],
                                "stableTargetId": ["bFQoRhcVH5DHUt"],
                            }
                        ],
                        "entityType": "MergedPrimarySource",
                    }
                ],
                "total": 1,
            },
        ),
    ],
    ids=[
        "fetch all merged items",
        "search for nested nodes and inline strings",
        "search for non-existent stableTargetId",
        "search for existent stableTargetId",
        "search for specific entity type",
        "search one inline merged item",
    ],
)
@pytest.mark.usefixtures("load_dummy_data", "load_dummy_rule_set")
@pytest.mark.integration
def test_fetch_merged_items(args: Iterable[Any], expected: dict[str, Any]) -> None:
    connector = GraphConnector.get()
    result = connector.fetch_merged_items(*args)
    assert result.one() == expected


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


@pytest.mark.usefixtures("mocked_query_builder")
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
        ("bFQoRhcVH5DHUv", None, True),
        ("bFQoRhcVH5DHUv", ["Person", "ContactPoint", "OrganizationalUnit"], True),
        ("bFQoRhcVH5DHUv", ["Activity"], False),
        ("thisIdDoesNotExist", ["Activity"], False),
    ],
    ids=[
        "found without type filter",
        "found with type filter",
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


@pytest.mark.usefixtures("mocked_query_builder")
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


@pytest.mark.usefixtures("mocked_query_builder")
def test_mocked_graph_merge_edges(
    mocked_graph: MockedGraph, dummy_data: dict[str, AnyExtractedModel]
) -> None:
    extracted_organizational_unit = dummy_data["organizational_unit_1"]
    graph = GraphConnector.get()
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
    ref_labels=["hadPrimarySource", "stableTargetId"],
)""",
        {
            "identifier": extracted_organizational_unit.identifier,
            "ref_identifiers": [
                extracted_organizational_unit.hadPrimarySource,
                extracted_organizational_unit.stableTargetId,
            ],
            "ref_positions": [0, 0],
            "stable_target_id": "cWWm02l1c6cucKjIhkFqY4",
        },
    )


@pytest.mark.usefixtures("mocked_query_builder")
def test_mocked_graph_creates_rule_set(
    mocked_graph: MockedGraph,
    organizational_unit_rule_set_request: OrganizationalUnitRuleSetRequest,
) -> None:
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


@pytest.mark.usefixtures("mocked_graph")
def test_mocked_graph_ingests_models(dummy_data: dict[str, AnyExtractedModel]) -> None:
    graph = GraphConnector.get()
    identifiers = graph.ingest(list(dummy_data.values()))

    assert identifiers == [d.identifier for d in dummy_data.values()]
