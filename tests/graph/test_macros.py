from typing import TYPE_CHECKING

import pytest
from jinja2 import ChoiceLoader, FileSystemLoader
from pytest import MonkeyPatch

from mex.backend.graph.connector import GraphConnector
from mex.backend.graph.constants import NO_REFERENCE_SENTINEL
from mex.backend.graph.models import MEX_EDITOR_PRIMARY_SOURCE, MEX_PRIMARY_SOURCE
from mex.backend.graph.query import QueryBuilder
from mex.common.models.base.rules import RuleSet

if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Callable

    from tests.conftest import DummyData


@pytest.fixture
def query_builder(monkeypatch: MonkeyPatch) -> QueryBuilder:
    builder = QueryBuilder.get()
    assert builder._env.loader
    monkeypatch.setattr(
        builder._env,
        "loader",
        ChoiceLoader(
            [
                FileSystemLoader("tests/graph/cypher"),
                FileSystemLoader("mex/backend/graph/cypher"),
            ]
        ),
    )
    monkeypatch.setitem(
        builder._env.globals,
        "any_extracted_or_rule_label",
        "ExtractedThis|AdditiveThis",
    )
    monkeypatch.setitem(builder._env.globals, "any_merged_label", "MergedThis")
    monkeypatch.setitem(builder._env.globals, "any_nested_label", "Link|Text")
    return builder


@pytest.mark.parametrize(
    ("filter_by_query_string", "expected"),
    [
        pytest.param(
            True,
            """\
OPTIONAL CALL db.index.fulltext.queryNodes("search_index", $query_string)
YIELD node AS hit, score
CALL (hit) {
    MATCH (hit:ExtractedThis|AdditiveThis)-[:stableTargetId]->(merged_node:MergedThis)
    RETURN hit as extracted_or_rule_node, merged_node
UNION
    MATCH (hit:Link|Text)<-[]-(extracted_or_rule_node:ExtractedThis|AdditiveThis)-[:stableTargetId]->(merged_node:MergedThis)
    RETURN extracted_or_rule_node, merged_node
}
WITH DISTINCT extracted_or_rule_node, merged_node
ORDER BY merged_node.identifier, extracted_or_rule_node.identifier, head(labels(extracted_or_rule_node)) ASC
RETURN extracted_or_rule_node, merged_node;""",
            id="search",
        ),
        pytest.param(
            False,
            """\
OPTIONAL MATCH (extracted_or_rule_node:ExtractedThis|AdditiveThis)-[:stableTargetId]->(merged_node:MergedThis)
ORDER BY merged_node.identifier, extracted_or_rule_node.identifier, head(labels(extracted_or_rule_node)) ASC
RETURN extracted_or_rule_node, merged_node;""",
            id="match",
        ),
    ],
)
def test_render_match_or_search_nodes(
    query_builder: QueryBuilder,
    filter_by_query_string: bool,  # noqa: FBT001
    expected: str,
) -> None:
    query = query_builder.test_match_or_search_nodes(
        filter_by_query_string=filter_by_query_string,
    )
    assert query.render() == expected


def test_render_collect_references_and_nested(
    query_builder: QueryBuilder,
) -> None:
    query = query_builder.test_collect_references_and_nested()
    assert (
        query.render()
        == """\
MATCH (extracted_or_rule_node:ExtractedThis|AdditiveThis)
WHERE extracted_or_rule_node.identifier = $identifier
RETURN [
    (extracted_or_rule_node)-[r]->(referenced_merged_node:MergedThis) |
    {value: referenced_merged_node.identifier, position:r.position, label: type(r)}
] + [
    (extracted_or_rule_node)-[r]->(referenced_nested_node:Link|Text) |
    {value: properties(referenced_nested_node), position:r.position , label: type(r)}
] AS refs;"""
    )


def test_render_check_reference_filters(
    query_builder: QueryBuilder,
) -> None:
    query = query_builder.test_check_reference_filters(
        reference_fields=["hadPrimarySource", "unitOf"],
    )
    assert (
        query.render()
        == """\
MATCH (extracted_or_rule_node:ExtractedThis|AdditiveThis)-[:stableTargetId]->(merged_node:MergedThis)
OPTIONAL MATCH (extracted_or_rule_node)-[reference:hadPrimarySource|unitOf]->(referenced_merged_node:MergedThis)
WITH
    merged_node,
    extracted_or_rule_node,
    collect(CASE WHEN reference IS NOT NULL THEN type(reference) END) AS found_fields,
    collect(CASE WHEN reference IS NOT NULL
        THEN {field: type(reference), identifier: referenced_merged_node.identifier} END
    ) AS existing_refs
WITH
    merged_node,
    extracted_or_rule_node,
    existing_refs +
    [f IN $reference_fields WHERE NOT f IN found_fields | {field: f, identifier: "__NO_REF__"}]
    AS ref_matches
WHERE ALL(rf IN $reference_filters WHERE
    ANY(m IN ref_matches WHERE m.field = rf.field AND m.identifier IN rf.identifiers)
)
RETURN extracted_or_rule_node, merged_node;"""
    )


@pytest.fixture
def integration_query_builder(monkeypatch: MonkeyPatch) -> QueryBuilder:
    builder = QueryBuilder.get()
    monkeypatch.setattr(
        builder._env,
        "loader",
        ChoiceLoader(
            [
                FileSystemLoader("tests/graph/cypher"),
                FileSystemLoader("mex/backend/graph/cypher"),
            ]
        ),
    )
    return builder


@pytest.mark.integration
def test_match_or_search_nodes_match(
    integration_query_builder: QueryBuilder, loaded_dummy_data: DummyData
) -> None:
    query = integration_query_builder.test_match_or_search_nodes(
        filter_by_query_string=False,
    )
    connector = GraphConnector.get()
    result = connector.commit(query)
    rows = result.all()

    seeded_extracted_or_rule_item_count = sum(
        3 if isinstance(v, RuleSet) else 1
        for v in [
            *loaded_dummy_data.values(),
            MEX_PRIMARY_SOURCE,
            MEX_EDITOR_PRIMARY_SOURCE,
        ]
    )

    assert len(rows) == seeded_extracted_or_rule_item_count
    assert rows[0] == {
        "extracted_or_rule_node": {
            "identifier": "00000000000001",
            "identifierInPrimarySource": "mex",
        },
        "merged_node": {"identifier": "00000000000000"},
    }


@pytest.mark.integration
@pytest.mark.usefixtures("loaded_dummy_data")
def test_match_or_search_nodes_search(
    integration_query_builder: QueryBuilder,
) -> None:
    query = integration_query_builder.test_match_or_search_nodes(
        filter_by_query_string=True,
    )
    connector = GraphConnector.get()
    result = connector.commit(query, query_string="Aktivität")
    rows = result.all()

    assert len(rows) == 1
    assert rows[0] == {
        "extracted_or_rule_node": {
            "identifier": "bFQoRhcVH5DHUG",
            "identifierInPrimarySource": "a-1",
            "fundingProgram": [],
            "start": ["2014-08-24"],
            "end": [],
            "theme": ["https://mex.rki.de/item/theme-11"],
            "activityType": [],
        },
        "merged_node": {"identifier": "bFQoRhcVH5DHUH"},
    }


@pytest.mark.integration
@pytest.mark.usefixtures("loaded_dummy_data")
def test_collect_references_and_nested(
    integration_query_builder: QueryBuilder,
    dummy_data: DummyData,
) -> None:
    query = integration_query_builder.test_collect_references_and_nested()
    connector = GraphConnector.get()
    result = connector.commit(
        query,
        identifier=str(dummy_data["activity_1"].identifier),
    )
    row = result.one()
    refs = row["refs"]
    assert refs == [
        {"label": "stableTargetId", "position": 0, "value": "bFQoRhcVH5DHUH"},
        {"label": "hadPrimarySource", "position": 0, "value": "bFQoRhcVH5DHUr"},
        {"label": "contact", "position": 0, "value": "bFQoRhcVH5DHUB"},
        {"label": "contact", "position": 1, "value": "bFQoRhcVH5DHUD"},
        {"label": "contact", "position": 2, "value": "bFQoRhcVH5DHUx"},
        {"label": "responsibleUnit", "position": 0, "value": "bFQoRhcVH5DHUx"},
        {
            "label": "abstract",
            "position": 0,
            "value": {"language": "en", "value": "An active activity."},
        },
        {
            "label": "abstract",
            "position": 1,
            "value": {"value": "Eng aktiv Aktivitéit."},
        },
        {
            "label": "title",
            "position": 0,
            "value": {"language": "de", "value": "Aktivität 1"},
        },
        {
            "label": "website",
            "position": 0,
            "value": {"title": "Activity Homepage", "url": "https://activity-1"},
        },
    ]


@pytest.mark.integration
@pytest.mark.usefixtures("loaded_dummy_data")
@pytest.mark.parametrize(
    ("reference_fields", "make_filters", "expected_count"),
    [
        pytest.param(
            ["hadPrimarySource"],
            lambda d: [
                {
                    "field": "hadPrimarySource",
                    "identifiers": [d["primary_source_1"].stableTargetId],
                },
            ],
            4,
            id="single field valid reference",
        ),
        pytest.param(
            ["hadPrimarySource"],
            lambda _: [
                {
                    "field": "hadPrimarySource",
                    "identifiers": ["nonExistentIdentifier"],
                },
            ],
            0,
            id="single field nonexistent reference",
        ),
        pytest.param(
            ["hadPrimarySource"],
            lambda _: [
                {
                    "field": "hadPrimarySource",
                    "identifiers": [NO_REFERENCE_SENTINEL],
                },
            ],
            6,
            id="single field no_reference_sentinel",
        ),
        pytest.param(
            ["hadPrimarySource"],
            lambda d: [
                {
                    "field": "hadPrimarySource",
                    "identifiers": [
                        NO_REFERENCE_SENTINEL,
                        d["primary_source_1"].stableTargetId,
                    ],
                },
            ],
            10,
            id="single field no_reference_sentinel or valid",
        ),
        pytest.param(
            ["hadPrimarySource"],
            lambda d: [
                {
                    "field": "hadPrimarySource",
                    "identifiers": [
                        d["primary_source_1"].stableTargetId,
                        d["primary_source_2"].stableTargetId,
                    ],
                },
            ],
            7,
            id="single field two valid identifiers",
        ),
        pytest.param(
            ["hadPrimarySource", "unitOf"],
            lambda d: [
                {
                    "field": "hadPrimarySource",
                    "identifiers": [d["primary_source_1"].stableTargetId],
                },
                {
                    "field": "unitOf",
                    "identifiers": [NO_REFERENCE_SENTINEL],
                },
            ],
            4,
            id="multiple fields valid and no_reference_sentinel",
        ),
        pytest.param(
            ["hadPrimarySource", "unitOf"],
            lambda d: [
                {
                    "field": "hadPrimarySource",
                    "identifiers": [d["primary_source_1"].stableTargetId],
                },
                {
                    "field": "unitOf",
                    "identifiers": ["nonExistentIdentifier"],
                },
            ],
            0,
            id="multiple fields valid and nonexistent",
        ),
        pytest.param(
            ["hadPrimarySource", "unitOf", "contact"],
            lambda d: [
                {
                    "field": "hadPrimarySource",
                    "identifiers": [d["primary_source_1"].stableTargetId],
                },
                {
                    "field": "unitOf",
                    "identifiers": ["nonExistentIdentifier"],
                },
                {
                    "field": "contact",
                    "identifiers": [NO_REFERENCE_SENTINEL],
                },
            ],
            0,
            id="multiple fields valid nonexistent and no_reference_sentinel",
        ),
    ],
)
def test_check_reference_filters(
    integration_query_builder: QueryBuilder,
    dummy_data: DummyData,
    reference_fields: list[str],
    make_filters: Callable[[DummyData], list[dict[str, object]]],
    expected_count: int,
) -> None:
    query = integration_query_builder.test_check_reference_filters(
        reference_fields=reference_fields,
    )
    connector = GraphConnector.get()
    result = connector.commit(
        query,
        reference_fields=reference_fields,
        reference_filters=make_filters(dummy_data),
    )
    rows = result.all()
    assert len(rows) == expected_count
