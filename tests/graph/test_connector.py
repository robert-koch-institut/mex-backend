import re
from collections import deque
from typing import Any
from unittest.mock import Mock, call

import pytest
from black.const import DEFAULT_LINE_LENGTH
from pytest import MonkeyPatch

from mex.backend.graph import connector as connector_module
from mex.backend.graph.connector import GraphConnector
from mex.backend.graph.exceptions import InconsistentGraphError, IngestionError
from mex.backend.graph.query import Query
from mex.backend.settings import BackendSettings
from mex.common.exceptions import MExError
from mex.common.models import (
    EXTRACTED_MODEL_CLASSES_BY_NAME,
    MERGED_MODEL_CLASSES_BY_NAME,
    AnyExtractedModel,
    ExtractedOrganization,
    ExtractedOrganizationalUnit,
    OrganizationalUnitRuleSetResponse,
)
from mex.common.types import (
    Identifier,
    MergedOrganizationalUnitIdentifier,
    Text,
    TextLanguage,
)
from tests.conftest import MockedGraph, get_graph


@pytest.fixture
def mocked_query_class(monkeypatch: MonkeyPatch) -> None:
    monkeypatch.setattr(Query, "__str__", Query.__repr__)
    monkeypatch.setattr(Query.REPR_MODE, "line_length", DEFAULT_LINE_LENGTH)


@pytest.mark.usefixtures("mocked_query_class", "mocked_redis")
def test_check_connectivity_and_authentication(mocked_graph: MockedGraph) -> None:
    mocked_graph.return_value = [{"currentStatus": "online"}]
    graph = GraphConnector.get()
    graph._check_connectivity_and_authentication()

    assert mocked_graph.call_args_list[-1].args == ("fetch_database_status()", {})


@pytest.mark.usefixtures("mocked_redis")
def test_check_connectivity_and_authentication_error(mocked_graph: MockedGraph) -> None:
    mocked_graph.return_value = [{"currentStatus": "offline"}]
    graph = GraphConnector.get()
    with pytest.raises(MExError, match="Database is offline"):
        graph._check_connectivity_and_authentication()


@pytest.mark.usefixtures("mocked_query_class", "mocked_redis")
def test_mocked_graph_seed_constraints(mocked_graph: MockedGraph) -> None:
    graph = GraphConnector.get()
    graph._seed_constraints()

    assert len(
        [
            c
            for c in mocked_graph.call_args_list
            if c.args[0].startswith("create_identifier_constraint")
        ]
    ) == len(EXTRACTED_MODEL_CLASSES_BY_NAME) + len(MERGED_MODEL_CLASSES_BY_NAME)

    assert len(
        [
            c
            for c in mocked_graph.call_args_list
            if c.args[0].startswith("create_provenance_constraint")
        ]
    ) == len(EXTRACTED_MODEL_CLASSES_BY_NAME)

    assert mocked_graph.call_args_list[-1].args == (
        'create_provenance_constraint(node_label="ExtractedVariableGroup")',
        {},
    )


@pytest.mark.usefixtures("mocked_query_class", "mocked_redis")
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


@pytest.mark.usefixtures("mocked_query_class", "mocked_redis")
def test_mocked_graph_seed_data(mocked_graph: MockedGraph) -> None:
    mocked_graph.return_value = [
        {
            "identifier": "00000000000001",
            "stableTargetId": "00000000000000",
            "entityType": "ExtractedPrimarySource",
            "linkRels": [
                {
                    "nodeProps": {"identifier": "00000000000000"},
                    "edgeLabel": "hadPrimarySource",
                    "edgeProps": {"position": 0},
                    "nodeLabels": ["MergedPrimarySource"],
                }
            ],
            "createRels": [],
            "nodeProps": {
                "identifierInPrimarySource": "mex",
                "identifier": "00000000000001",
            },
        }
    ]
    graph = GraphConnector.get()
    graph._seed_data()

    assert mocked_graph.call_args_list[-1].args == (
        """merge_extracted_item(
    params=IngestParams(
        merged_label="MergedPrimarySource",
        node_label="ExtractedPrimarySource",
        all_referenced_labels=[
            "MergedContactPoint",
            "MergedOrganizationalUnit",
            "MergedPerson",
            "MergedPrimarySource",
        ],
        all_nested_labels=["Link", "Text"],
        detach_node_edges=["contact", "hadPrimarySource", "unitInCharge"],
        delete_node_edges=[
            "alternativeTitle",
            "description",
            "title",
            "documentation",
            "locatedAt",
        ],
        has_link_rels=True,
        has_create_rels=True,
    )
)""",
    )
    assert mocked_graph.call_args_list[-1].kwargs == {
        "data": {
            "stableTargetId": "00000000000000",
            "identifier": "00000000000001",
            "entityType": "ExtractedPrimarySource",
            "nodeProps": {
                "version": None,
                "identifier": "00000000000001",
                "identifierInPrimarySource": "mex",
            },
            "linkRels": [
                {
                    "nodeLabels": ["MergedPrimarySource"],
                    "nodeProps": {"identifier": "00000000000000"},
                    "edgeLabel": "hadPrimarySource",
                    "edgeProps": {"position": 0},
                }
            ],
            "createRels": [],
        },
    }


@pytest.mark.usefixtures("mocked_redis")
def test_mocked_graph_commit_raises_error(mocked_graph: MockedGraph) -> None:
    mocked_graph.run.side_effect = Exception("query failed")
    graph = GraphConnector.get()
    with pytest.raises(Exception, match="query failed"):
        graph.commit("RETURN 1;")


@pytest.mark.usefixtures("mocked_query_class", "mocked_redis")
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
        identifier=None,
        stable_target_id=Identifier.generate(99),
        entity_type=["ExtractedFoo", "ExtractedBar", "ExtractedBatz"],
        referenced_identifiers=None,
        reference_field=None,
        skip=10,
        limit=100,
    )

    assert mocked_graph.call_args_list[-1].args == (
        """\
fetch_extracted_or_rule_items(
    filter_by_query_string=True,
    filter_by_identifier=False,
    filter_by_stable_target_id=True,
    filter_by_referenced_identifiers=False,
    reference_field=None,
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
            "identifier": None,
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
        identifier=None,
        stable_target_id=stable_target_id,
        entity_type=entity_type,
        referenced_identifiers=None,
        reference_field=None,
        skip=0,
        limit=limit,
    )

    assert result.one() == expected


@pytest.mark.usefixtures("mocked_query_class", "mocked_redis")
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
        identifier=None,
        stable_target_id=Identifier.generate(99),
        entity_type=["AdditiveFoo", "SubtractiveBar", "PreventiveBatz"],
        referenced_identifiers=None,
        reference_field=None,
        skip=10,
        limit=100,
    )

    assert mocked_graph.call_args_list[-1].args == (
        """\
fetch_extracted_or_rule_items(
    filter_by_query_string=True,
    filter_by_identifier=False,
    filter_by_stable_target_id=True,
    filter_by_referenced_identifiers=False,
    reference_field=None,
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
            "identifier": None,
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

    result = graph.fetch_rule_items(
        query_string=query_string,
        identifier=None,
        stable_target_id=stable_target_id,
        entity_type=None,
        referenced_identifiers=None,
        reference_field=None,
        skip=0,
        limit=1,
    )

    assert result.one() == expected


@pytest.mark.integration
def test_fetch_rule_items_empty() -> None:
    graph = GraphConnector.get()

    result = graph.fetch_rule_items(
        query_string=None,
        identifier=None,
        stable_target_id="thisIdDoesNotExist",
        entity_type=None,
        referenced_identifiers=None,
        reference_field=None,
        skip=0,
        limit=1,
    )

    assert result.one() == {"items": [], "total": 0}


@pytest.mark.usefixtures("mocked_query_class", "mocked_redis")
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
        referenced_identifiers=[Identifier.generate(100)],
        reference_field="hadPrimarySource",
        skip=10,
        limit=100,
    )

    assert mocked_graph.call_args_list[-1].args == (
        """\
fetch_merged_items(
    filter_by_query_string=True,
    filter_by_identifier=True,
    filter_by_referenced_identifiers=True,
    reference_field="hadPrimarySource",
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


@pytest.mark.usefixtures("mocked_query_class", "mocked_redis", "mocked_graph")
def test_mocked_graph_fetch_merged_items_invalid_field_name() -> None:
    graph = GraphConnector.get()
    with pytest.raises(ValueError, match="Invalid field name"):
        graph.fetch_merged_items(
            query_string="ok-query",
            identifier=Identifier.generate(99),
            entity_type=["MergedFoo"],
            referenced_identifiers=[Identifier.generate(100)],
            reference_field="Robert'); DROP TABLE Students;--",
            skip=10,
            limit=10,
        )


@pytest.mark.parametrize(
    (
        "query_string",
        "identifier",
        "entity_type",
        "referenced_identifiers",
        "reference_field",
        "limit",
        "expected",
    ),
    [
        (
            None,
            "thisIdDoesNotExist",
            None,
            None,
            None,
            10,
            {"items": [], "total": 0},
        ),
        (
            "this_search_term_is_not_findable",
            None,
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
            "hadPrimarySource",
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
            "hadPrimarySource",
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
    referenced_identifiers: list[str] | None,
    reference_field: str | None,
    limit: int,
    expected: dict[str, Any],
) -> None:
    graph = GraphConnector.get()

    result = graph.fetch_merged_items(
        query_string=query_string,
        identifier=identifier,
        entity_type=entity_type,
        referenced_identifiers=referenced_identifiers,
        reference_field=reference_field,
        skip=0,
        limit=limit,
    )

    assert result.one() == expected


@pytest.mark.usefixtures("mocked_query_class", "mocked_redis")
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


@pytest.mark.usefixtures("mocked_query_class", "mocked_redis")
def test_mocked_graph_exists_item(
    mocked_graph: MockedGraph,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        connector_module,
        "ALL_MODEL_CLASSES_BY_NAME",
        {"MergedFoo": Mock(), "MergedBar": Mock(), "MergedBatz": Mock()},
    )
    mocked_graph.return_value = [{"exists": True}]

    graph = GraphConnector.get()
    graph.exists_item(
        identifier=Identifier.generate(99),
        entity_type="MergedFoo",
    )

    assert mocked_graph.call_args_list[-1].args == (
        """\
exists_item(node_labels=["MergedFoo"])""",
        {"identifier": Identifier.generate(99)},
    )


@pytest.mark.parametrize(
    ("stable_target_id", "entity_type", "exists"),
    [
        ("bFQoRhcVH5DHUB", "MergedContactPoint", True),
        ("bFQoRhcVH5DHUB", "MergedActivity", False),
        ("thisIdDoesNotExist", "MergedActivity", False),
    ],
    ids=[
        "found with type filter",
        "missed due to filter",
        "missed due to identifier",
    ],
)
@pytest.mark.usefixtures("load_dummy_data")
@pytest.mark.integration
def test_graph_exists_item(
    stable_target_id: Identifier,
    entity_type: str,
    exists: bool,  # noqa: FBT001
) -> None:
    graph = GraphConnector.get()

    assert graph.exists_item(stable_target_id, entity_type) == exists


@pytest.mark.usefixtures("mocked_query_class", "mocked_redis")
def test_mocked_graph_merge_rule_item(
    mocked_graph: MockedGraph,
    organizational_unit_rule_set_response: OrganizationalUnitRuleSetResponse,
) -> None:
    graph = GraphConnector.get()
    with mocked_graph.session as session:
        graph._merge_rule_item(
            session,
            organizational_unit_rule_set_response.additive,
            Identifier("orgUnitStableTargetId"),
        )

    assert mocked_graph.call_args_list[-1].args == (
        """merge_rule_item(
    current_label="AdditiveOrganizationalUnit",
    merged_label="MergedOrganizationalUnit",
    nested_edge_labels=["name", "website"],
    nested_node_labels=["Text", "Link"],
)""",
        {
            "stable_target_id": Identifier("orgUnitStableTargetId"),
            "on_match": {"email": []},
            "on_create": {"email": []},
            "nested_values": [
                {"value": "Unit 1.7", "language": TextLanguage.EN},
                {"language": None, "title": "Unit Homepage", "url": "https://unit-1-7"},
            ],
            "nested_positions": [0, 0],
        },
    )


@pytest.mark.usefixtures("mocked_query_class", "mocked_redis")
def test_mocked_graph_merge_rule_edges(
    mocked_graph: MockedGraph,
    organizational_unit_rule_set_response: OrganizationalUnitRuleSetResponse,
) -> None:
    mocked_graph.return_value = [
        {"edges": ["parentUnit {position: 0}", "stableTargetId {position: 0}"]},
    ]
    graph = GraphConnector.get()
    with mocked_graph.session as session:
        graph._merge_rule_edges(
            session,
            organizational_unit_rule_set_response.additive,
            Identifier("orgUnitStableTargetId"),
        )

    assert mocked_graph.call_args_list[-1].args == (
        """\
merge_rule_edges(
    current_label="AdditiveOrganizationalUnit",
    merged_label="MergedOrganizationalUnit",
    ref_labels=["parentUnit", "stableTargetId"],
)""",
        {
            "stable_target_id": Identifier("orgUnitStableTargetId"),
            "ref_identifiers": [
                "cWWm02l1c6cucKjIhkFqY4",
                Identifier("orgUnitStableTargetId"),
            ],
            "ref_positions": [0, 0],
        },
    )


@pytest.mark.usefixtures("mocked_query_class", "mocked_redis")
def test_mocked_graph_merge_rule_edges_fails_inconsistent(
    mocked_graph: MockedGraph,
    organizational_unit_rule_set_response: OrganizationalUnitRuleSetResponse,
) -> None:
    mocked_graph.return_value = [{"edges": ["stableTargetId {position: 0}"]}]
    graph = GraphConnector.get()

    with pytest.raises(  # noqa: SIM117
        InconsistentGraphError,
        match=re.escape(
            "InconsistentGraphError: failed to merge 1 edges: "
            "(:AdditiveOrganizationalUnit)-[:parentUnit {position: 0}]->"
            '({identifier: "cWWm02l1c6cucKjIhkFqY4"})'
        ),
    ):
        with mocked_graph.session as session:
            graph._merge_rule_edges(
                session,
                organizational_unit_rule_set_response.additive,
                Identifier("orgUnitStableTargetId"),
            )


@pytest.mark.integration
@pytest.mark.usefixtures("load_dummy_data")
def test_graph_merge_rule_edges_fails_inconsistent(
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
        IngestionError,
        match=re.escape(
            "IngestionError: Could not merge "
            "ExtractedOrganizationalUnit(stableTargetId='bFQoRhcVH5DHUL', ...)"
        ),
    ):
        deque(graph.ingest_extracted_items([consistent_org, inconsistent_unit]))


@pytest.mark.usefixtures("mocked_query_class", "mocked_redis")
def test_mocked_graph_merge_rule_edges_fails_unexpected(
    mocked_graph: MockedGraph,
    organizational_unit_rule_set_response: OrganizationalUnitRuleSetResponse,
) -> None:
    mocked_graph.return_value = [
        {
            "edges": [
                "stableTargetId {position: 0}",
                "parentUnit {position: 0}",
                "newEdgeWhoDis {position: 0}",
            ]
        }
    ]
    graph = GraphConnector.get()

    with pytest.raises(  # noqa: SIM117
        RuntimeError,
        match=re.escape(
            "merged 1 edges more than expected: newEdgeWhoDis {position: 0}"
        ),
    ):
        with mocked_graph.session as session:
            graph._merge_rule_edges(
                session,
                organizational_unit_rule_set_response.additive,
                Identifier("orgUnitStableTargetId"),
            )


@pytest.mark.usefixtures("mocked_query_class", "mocked_redis")
def test_mocked_graph_ingests_rule_set(
    mocked_graph: MockedGraph,
    organizational_unit_rule_set_response: OrganizationalUnitRuleSetResponse,
) -> None:
    mocked_graph.side_effect = [
        [{"current": {}, "$comment": "additive item"}],
        [
            {
                "edges": ["parentUnit {position: 0}", "stableTargetId {position: 0}"],
                "$comment": "additive edges",
            }
        ],
        [{"current": {}, "$comment": "subtractive item"}],
        [{"edges": ["stableTargetId {position: 0}"], "$comment": "subtractive edges"}],
        [{"current": {}, "$comment": "preventive item"}],
        [{"edges": ["stableTargetId {position: 0}"], "$comment": "preventive edges"}],
    ]
    graph = GraphConnector.get()
    graph.ingest_rule_set(organizational_unit_rule_set_response)

    assert mocked_graph.call_args_list == [
        call(
            """merge_rule_item(
    current_label="AdditiveOrganizationalUnit",
    merged_label="MergedOrganizationalUnit",
    nested_edge_labels=["name", "website"],
    nested_node_labels=["Text", "Link"],
)""",
            {
                "stable_target_id": MergedOrganizationalUnitIdentifier(
                    "bFQoRhcVH5DHU6"
                ),
                "on_match": {"email": []},
                "on_create": {"email": []},
                "nested_values": [
                    {"value": "Unit 1.7", "language": TextLanguage.EN},
                    {
                        "language": None,
                        "title": "Unit Homepage",
                        "url": "https://unit-1-7",
                    },
                ],
                "nested_positions": [0, 0],
            },
        ),
        call(
            """merge_rule_edges(
    current_label="AdditiveOrganizationalUnit",
    merged_label="MergedOrganizationalUnit",
    ref_labels=["parentUnit", "stableTargetId"],
)""",
            {
                "stable_target_id": MergedOrganizationalUnitIdentifier(
                    "bFQoRhcVH5DHU6"
                ),
                "ref_identifiers": [
                    "cWWm02l1c6cucKjIhkFqY4",
                    MergedOrganizationalUnitIdentifier("bFQoRhcVH5DHU6"),
                ],
                "ref_positions": [0, 0],
            },
        ),
        call(
            """merge_rule_item(
    current_label="SubtractiveOrganizationalUnit",
    merged_label="MergedOrganizationalUnit",
    nested_edge_labels=[],
    nested_node_labels=[],
)""",
            {
                "stable_target_id": MergedOrganizationalUnitIdentifier(
                    "bFQoRhcVH5DHU6"
                ),
                "on_match": {"email": []},
                "on_create": {"email": []},
                "nested_values": [],
                "nested_positions": [],
            },
        ),
        call(
            """merge_rule_edges(
    current_label="SubtractiveOrganizationalUnit",
    merged_label="MergedOrganizationalUnit",
    ref_labels=["stableTargetId"],
)""",
            {
                "stable_target_id": MergedOrganizationalUnitIdentifier(
                    "bFQoRhcVH5DHU6"
                ),
                "ref_identifiers": [
                    MergedOrganizationalUnitIdentifier("bFQoRhcVH5DHU6")
                ],
                "ref_positions": [0],
            },
        ),
        call(
            """merge_rule_item(
    current_label="PreventiveOrganizationalUnit",
    merged_label="MergedOrganizationalUnit",
    nested_edge_labels=[],
    nested_node_labels=[],
)""",
            {
                "stable_target_id": MergedOrganizationalUnitIdentifier(
                    "bFQoRhcVH5DHU6"
                ),
                "on_match": {},
                "on_create": {},
                "nested_values": [],
                "nested_positions": [],
            },
        ),
        call(
            """merge_rule_edges(
    current_label="PreventiveOrganizationalUnit",
    merged_label="MergedOrganizationalUnit",
    ref_labels=["stableTargetId"],
)""",
            {
                "stable_target_id": MergedOrganizationalUnitIdentifier(
                    "bFQoRhcVH5DHU6"
                ),
                "ref_identifiers": [
                    MergedOrganizationalUnitIdentifier("bFQoRhcVH5DHU6")
                ],
                "ref_positions": [0],
            },
        ),
    ]


@pytest.mark.usefixtures("mocked_redis")
def test_mocked_graph_ingests_extracted_models(
    mocked_graph: MockedGraph,
    dummy_data: dict[str, AnyExtractedModel],
) -> None:
    mocked_graph.side_effect = [
        [
            {
                "identifier": str(dummy_data["primary_source_1"].identifier),
                "stableTargetId": str(dummy_data["primary_source_1"].stableTargetId),
                "entityType": "ExtractedPrimarySource",
                "linkRels": [
                    {
                        "nodeProps": {"identifier": "00000000000000"},
                        "edgeLabel": "hadPrimarySource",
                        "edgeProps": {"position": 0},
                        "nodeLabels": ["MergedPrimarySource"],
                    }
                ],
                "createRels": [],
                "nodeProps": {
                    "identifierInPrimarySource": "ps-1",
                    "identifier": str(dummy_data["primary_source_1"].identifier),
                },
            }
        ],
        [
            {
                "identifier": str(dummy_data["primary_source_2"].identifier),
                "stableTargetId": str(dummy_data["primary_source_2"].stableTargetId),
                "entityType": "ExtractedPrimarySource",
                "linkRels": [
                    {
                        "nodeProps": {"identifier": "00000000000000"},
                        "edgeLabel": "hadPrimarySource",
                        "edgeProps": {"position": 0},
                        "nodeLabels": ["MergedPrimarySource"],
                    }
                ],
                "createRels": [],
                "nodeProps": {
                    "identifierInPrimarySource": "ps-2",
                    "identifier": str(dummy_data["primary_source_2"].identifier),
                    "version": "Cool Version v2.13",
                },
            }
        ],
        [
            {
                "identifier": str(dummy_data["contact_point_1"].identifier),
                "stableTargetId": str(dummy_data["contact_point_1"].stableTargetId),
                "entityType": "ExtractedContactPoint",
                "linkRels": [
                    {
                        "nodeProps": {
                            "identifier": str(
                                dummy_data["primary_source_1"].stableTargetId
                            )
                        },
                        "edgeLabel": "hadPrimarySource",
                        "edgeProps": {"position": 0},
                        "nodeLabels": ["MergedPrimarySource"],
                    }
                ],
                "nodeProps": {
                    "identifierInPrimarySource": "cp-1",
                    "email": ["info@contact-point.one"],
                    "identifier": str(dummy_data["contact_point_1"].identifier),
                },
            }
        ],
        [
            {
                "identifier": str(dummy_data["contact_point_2"].identifier),
                "stableTargetId": str(dummy_data["contact_point_2"].stableTargetId),
                "entityType": "ExtractedContactPoint",
                "linkRels": [
                    {
                        "nodeProps": {
                            "identifier": str(
                                dummy_data["primary_source_1"].stableTargetId
                            )
                        },
                        "edgeLabel": "hadPrimarySource",
                        "edgeProps": {"position": 0},
                        "nodeLabels": ["MergedPrimarySource"],
                    }
                ],
                "nodeProps": {
                    "identifierInPrimarySource": "cp-2",
                    "email": ["help@contact-point.two"],
                    "identifier": str(dummy_data["contact_point_2"].identifier),
                },
            }
        ],
        [
            {
                "identifier": str(dummy_data["organization_1"].identifier),
                "stableTargetId": str(dummy_data["organization_1"].stableTargetId),
                "entityType": "ExtractedOrganization",
                "linkRels": [
                    {
                        "nodeProps": {
                            "identifier": str(
                                dummy_data["primary_source_1"].stableTargetId
                            )
                        },
                        "edgeLabel": "hadPrimarySource",
                        "edgeProps": {"position": 0},
                        "nodeLabels": ["MergedPrimarySource"],
                    }
                ],
                "createRels": [
                    {
                        "nodeProps": {"value": "RKI", "language": "de"},
                        "edgeLabel": "officialName",
                        "edgeProps": {"position": 0},
                        "nodeLabels": ["Text"],
                    },
                    {
                        "nodeProps": {
                            "value": "Robert Koch Institut ist the best",
                            "language": "de",
                        },
                        "edgeLabel": "officialName",
                        "edgeProps": {"position": 1},
                        "nodeLabels": ["Text"],
                    },
                ],
                "nodeProps": {
                    "rorId": [],
                    "identifierInPrimarySource": "rki",
                    "gndId": [],
                    "wikidataId": [],
                    "geprisId": [],
                    "viafId": [],
                    "isniId": [],
                    "identifier": str(dummy_data["organization_1"].identifier),
                },
            }
        ],
        [
            {
                "identifier": str(dummy_data["organization_2"].identifier),
                "stableTargetId": str(dummy_data["organization_2"].stableTargetId),
                "entityType": "ExtractedOrganization",
                "linkRels": [
                    {
                        "nodeProps": {
                            "identifier": str(
                                dummy_data["primary_source_2"].stableTargetId
                            )
                        },
                        "edgeLabel": "hadPrimarySource",
                        "edgeProps": {"position": 0},
                        "nodeLabels": ["MergedPrimarySource"],
                    }
                ],
                "createRels": [
                    {
                        "nodeProps": {"value": "RKI", "language": "de"},
                        "edgeLabel": "officialName",
                        "edgeProps": {"position": 0},
                        "nodeLabels": ["Text"],
                    },
                    {
                        "nodeProps": {
                            "value": "Robert Koch Institute",
                            "language": "en",
                        },
                        "edgeLabel": "officialName",
                        "edgeProps": {"position": 1},
                        "nodeLabels": ["Text"],
                    },
                ],
                "nodeProps": {
                    "rorId": [],
                    "identifierInPrimarySource": "robert-koch-institute",
                    "gndId": [],
                    "wikidataId": [],
                    "geprisId": [],
                    "viafId": [],
                    "isniId": [],
                    "identifier": str(dummy_data["organization_2"].identifier),
                },
            }
        ],
        [
            {
                "identifier": str(dummy_data["organizational_unit_1"].identifier),
                "stableTargetId": str(
                    dummy_data["organizational_unit_1"].stableTargetId
                ),
                "entityType": "ExtractedOrganizationalUnit",
                "linkRels": [
                    {
                        "nodeProps": {
                            "identifier": str(
                                dummy_data["primary_source_2"].stableTargetId
                            )
                        },
                        "edgeLabel": "hadPrimarySource",
                        "edgeProps": {"position": 0},
                        "nodeLabels": ["MergedPrimarySource"],
                    },
                    {
                        "nodeProps": {
                            "identifier": str(
                                dummy_data["organization_1"].stableTargetId
                            )
                        },
                        "edgeLabel": "unitOf",
                        "edgeProps": {"position": 0},
                        "nodeLabels": ["MergedOrganization"],
                    },
                ],
                "createRels": [
                    {
                        "nodeProps": {"value": "Unit 1", "language": "en"},
                        "edgeLabel": "name",
                        "edgeProps": {"position": 0},
                        "nodeLabels": ["Text"],
                    }
                ],
                "nodeProps": {
                    "identifierInPrimarySource": "ou-1",
                    "email": [],
                    "identifier": str(dummy_data["organizational_unit_1"].identifier),
                },
            }
        ],
        [
            {
                "identifier": str(dummy_data["organizational_unit_2"].identifier),
                "stableTargetId": str(
                    dummy_data["organizational_unit_2"].stableTargetId
                ),
                "entityType": "ExtractedOrganizationalUnit",
                "linkRels": [
                    {
                        "nodeProps": {
                            "identifier": str(
                                dummy_data["primary_source_2"].stableTargetId
                            )
                        },
                        "edgeLabel": "hadPrimarySource",
                        "edgeProps": {"position": 0},
                        "nodeLabels": ["MergedPrimarySource"],
                    },
                    {
                        "nodeProps": {
                            "identifier": str(
                                dummy_data["organizational_unit_1"].stableTargetId
                            )
                        },
                        "edgeLabel": "parentUnit",
                        "edgeProps": {"position": 0},
                        "nodeLabels": ["MergedOrganizationalUnit"],
                    },
                    {
                        "nodeProps": {
                            "identifier": str(
                                dummy_data["organization_1"].stableTargetId
                            )
                        },
                        "edgeLabel": "unitOf",
                        "edgeProps": {"position": 0},
                        "nodeLabels": ["MergedOrganization"],
                    },
                ],
                "createRels": [
                    {
                        "nodeProps": {"value": "Unit 1.6", "language": "en"},
                        "edgeLabel": "name",
                        "edgeProps": {"position": 0},
                        "nodeLabels": ["Text"],
                    }
                ],
                "nodeProps": {
                    "identifierInPrimarySource": "ou-1.6",
                    "email": [],
                    "identifier": str(dummy_data["organizational_unit_2"].identifier),
                },
            }
        ],
        [
            {
                "identifier": str(dummy_data["activity_1"].identifier),
                "stableTargetId": str(dummy_data["activity_1"].stableTargetId),
                "entityType": "ExtractedActivity",
                "linkRels": [
                    {
                        "nodeProps": {
                            "identifier": str(
                                dummy_data["contact_point_1"].stableTargetId
                            )
                        },
                        "edgeLabel": "contact",
                        "edgeProps": {"position": 0},
                        "nodeLabels": ["MergedContactPoint"],
                    },
                    {
                        "nodeProps": {
                            "identifier": str(
                                dummy_data["contact_point_2"].stableTargetId
                            )
                        },
                        "edgeLabel": "contact",
                        "edgeProps": {"position": 1},
                        "nodeLabels": ["MergedContactPoint"],
                    },
                    {
                        "nodeProps": {
                            "identifier": str(
                                dummy_data["organizational_unit_1"].stableTargetId
                            )
                        },
                        "edgeLabel": "contact",
                        "edgeProps": {"position": 2},
                        "nodeLabels": ["MergedOrganizationalUnit"],
                    },
                    {
                        "nodeProps": {
                            "identifier": str(
                                dummy_data["primary_source_1"].stableTargetId
                            )
                        },
                        "edgeLabel": "hadPrimarySource",
                        "edgeProps": {"position": 0},
                        "nodeLabels": ["MergedPrimarySource"],
                    },
                    {
                        "nodeProps": {
                            "identifier": str(
                                dummy_data["organizational_unit_1"].stableTargetId
                            )
                        },
                        "edgeLabel": "responsibleUnit",
                        "edgeProps": {"position": 0},
                        "nodeLabels": ["MergedOrganizationalUnit"],
                    },
                ],
                "createRels": [
                    {
                        "nodeProps": {"value": "An active activity.", "language": "en"},
                        "edgeLabel": "abstract",
                        "edgeProps": {"position": 0},
                        "nodeLabels": ["Text"],
                    },
                    {
                        "nodeProps": {"value": "Une activité active."},
                        "edgeLabel": "abstract",
                        "edgeProps": {"position": 1},
                        "nodeLabels": ["Text"],
                    },
                    {
                        "nodeProps": {"value": "Aktivität 1", "language": "de"},
                        "edgeLabel": "title",
                        "edgeProps": {"position": 0},
                        "nodeLabels": ["Text"],
                    },
                    {
                        "nodeProps": {
                            "title": "Activity Homepage",
                            "url": "https://activity-1",
                        },
                        "edgeLabel": "website",
                        "edgeProps": {"position": 0},
                        "nodeLabels": ["Link"],
                    },
                ],
                "nodeProps": {
                    "fundingProgram": [],
                    "identifierInPrimarySource": "a-1",
                    "start": ["2014-08-24"],
                    "theme": ["https://mex.rki.de/item/theme-11"],
                    "activityType": [],
                    "identifier": str(dummy_data["activity_1"].identifier),
                    "end": [],
                },
            }
        ],
    ]
    graph = GraphConnector.get()
    deque(graph.ingest_extracted_items(dummy_data.values()))
    assert len(mocked_graph.call_args_list) == len(dummy_data)


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
