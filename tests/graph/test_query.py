import pytest

from mex.backend.graph.query import QueryBuilder


@pytest.fixture
def query_builder() -> QueryBuilder:
    builder = QueryBuilder.get()
    builder._env.globals.update(
        extracted_labels=["ExtractedThis", "ExtractedThat", "ExtractedOther"],
        merged_labels=["MergedThis", "MergedThat", "MergedOther"],
        nested_labels=["Link", "Text", "Location"],
    )
    return builder


def test_create_full_text_search_index(query_builder: QueryBuilder) -> None:
    query = query_builder.create_full_text_search_index(
        node_labels=["Apple", "Orange"],
        search_fields=["texture", "sugarContent", "color"],
    )
    assert (
        query
        == """\
CREATE FULLTEXT INDEX search_index IF NOT EXISTS
FOR (n:Apple|Orange)
ON EACH [n.texture, n.sugarContent, n.color]
OPTIONS {indexConfig: $index_config};"""
    )


def test_create_identifier_uniqueness_constraint(query_builder: QueryBuilder) -> None:
    query = query_builder.create_identifier_uniqueness_constraint(
        node_label="BlueBerryPie"
    )
    assert (
        query
        == """\
CREATE CONSTRAINT blue_berry_pie_identifier_uniqueness IF NOT EXISTS
FOR (n:BlueBerryPie)
REQUIRE n.identifier IS UNIQUE;"""
    )


def test_fetch_database_status(query_builder: QueryBuilder) -> None:
    query = query_builder.fetch_database_status()
    assert (
        query
        == """\
SHOW DEFAULT DATABASE
YIELD currentStatus;"""
    )


@pytest.mark.parametrize(
    ("query_string", "stable_target_id", "labels", "expected"),
    [
        (
            True,
            True,
            True,
            """\
CALL {
    CALL db.index.fulltext.queryNodes("search_index", $query_string)
    YIELD node AS hit, score
    MATCH (n:ExtractedThis|ExtractedThat|ExtractedOther)-[:stableTargetId]->(m:MergedThis|MergedThat|MergedOther)
    WHERE
        elementId(hit) = elementId(n)
        AND m.identifier = $stable_target_id
        AND ANY(label IN labels(n) WHERE label IN $labels)
    RETURN COUNT(n) AS total
}
CALL {
    CALL db.index.fulltext.queryNodes("search_index", $query_string)
    YIELD node AS hit, score
    MATCH (n:ExtractedThis|ExtractedThat|ExtractedOther)-[:stableTargetId]->(m:MergedThis|MergedThat|MergedOther)
    WHERE
        elementId(hit) = elementId(n)
        AND m.identifier = $stable_target_id
        AND ANY(label IN labels(n) WHERE label IN $labels)
    CALL {
        WITH n
        MATCH (n:ExtractedThis|ExtractedThat|ExtractedOther)-[r]->(t:MergedThis|MergedThat|MergedOther)
        RETURN type(r) as label, r.position as position, t.identifier as value
    UNION
        WITH n
        MATCH (n:ExtractedThis|ExtractedThat|ExtractedOther)-[r]->(t:Link|Text|Location)
        RETURN type(r) as label, r.position as position, properties(t) as value
    }
    WITH n, collect({label: label, position: position, value: value}) as refs
    RETURN n{.*, entityType: head(labels(n)), _refs: refs}
    ORDER BY n.identifier ASC
    SKIP $skip
    LIMIT $limit
}
RETURN collect(n) AS items, total;""",
        ),
        (
            False,
            False,
            False,
            """\
CALL {
    MATCH (n:ExtractedThis|ExtractedThat|ExtractedOther)
    RETURN COUNT(n) AS total
}
CALL {
    MATCH (n:ExtractedThis|ExtractedThat|ExtractedOther)
    CALL {
        WITH n
        MATCH (n:ExtractedThis|ExtractedThat|ExtractedOther)-[r]->(t:MergedThis|MergedThat|MergedOther)
        RETURN type(r) as label, r.position as position, t.identifier as value
    UNION
        WITH n
        MATCH (n:ExtractedThis|ExtractedThat|ExtractedOther)-[r]->(t:Link|Text|Location)
        RETURN type(r) as label, r.position as position, properties(t) as value
    }
    WITH n, collect({label: label, position: position, value: value}) as refs
    RETURN n{.*, entityType: head(labels(n)), _refs: refs}
    ORDER BY n.identifier ASC
    SKIP $skip
    LIMIT $limit
}
RETURN collect(n) AS items, total;""",
        ),
        (
            False,
            False,
            True,
            """\
CALL {
    MATCH (n:ExtractedThis|ExtractedThat|ExtractedOther)
    WHERE
        ANY(label IN labels(n) WHERE label IN $labels)
    RETURN COUNT(n) AS total
}
CALL {
    MATCH (n:ExtractedThis|ExtractedThat|ExtractedOther)
    WHERE
        ANY(label IN labels(n) WHERE label IN $labels)
    CALL {
        WITH n
        MATCH (n:ExtractedThis|ExtractedThat|ExtractedOther)-[r]->(t:MergedThis|MergedThat|MergedOther)
        RETURN type(r) as label, r.position as position, t.identifier as value
    UNION
        WITH n
        MATCH (n:ExtractedThis|ExtractedThat|ExtractedOther)-[r]->(t:Link|Text|Location)
        RETURN type(r) as label, r.position as position, properties(t) as value
    }
    WITH n, collect({label: label, position: position, value: value}) as refs
    RETURN n{.*, entityType: head(labels(n)), _refs: refs}
    ORDER BY n.identifier ASC
    SKIP $skip
    LIMIT $limit
}
RETURN collect(n) AS items, total;""",
        ),
    ],
    ids=["all-filters", "no-filters", "label-filter"],
)
def test_fetch_extracted_data(
    query_builder: QueryBuilder,
    query_string: bool,
    stable_target_id: bool,
    labels: bool,
    expected: str,
) -> None:
    query = query_builder.fetch_extracted_data(
        query_string=query_string,
        stable_target_id=stable_target_id,
        labels=labels,
    )
    assert query == expected


@pytest.mark.parametrize(
    (
        "had_primary_source",
        "identifier_in_primary_source",
        "stable_target_id",
        "expected",
    ),
    [
        (
            True,
            True,
            True,
            """\
MATCH (n:ExtractedThis|ExtractedThat|ExtractedOther)-[:stableTargetId]->(m:MergedThis|MergedThat|MergedOther)
MATCH (n:ExtractedThis|ExtractedThat|ExtractedOther)-[:hadPrimarySource]->(p:MergedPrimarySource)
WHERE
    p.identifier = $had_primary_source
    AND n.identifierInPrimarySource = $identifier_in_primary_source
    AND m.identifier = $stable_target_id
RETURN
    m.identifier as stableTargetId,
    p.identifier as hadPrimarySource,
    n.identifierInPrimarySource as identifierInPrimarySource,
    n.identifier as identifier
ORDER BY n.identifier ASC
LIMIT $limit;""",
        ),
        (
            False,
            False,
            False,
            """\
MATCH (n:ExtractedThis|ExtractedThat|ExtractedOther)-[:stableTargetId]->(m:MergedThis|MergedThat|MergedOther)
MATCH (n:ExtractedThis|ExtractedThat|ExtractedOther)-[:hadPrimarySource]->(p:MergedPrimarySource)
RETURN
    m.identifier as stableTargetId,
    p.identifier as hadPrimarySource,
    n.identifierInPrimarySource as identifierInPrimarySource,
    n.identifier as identifier
ORDER BY n.identifier ASC
LIMIT $limit;""",
        ),
        (
            False,
            False,
            True,
            """\
MATCH (n:ExtractedThis|ExtractedThat|ExtractedOther)-[:stableTargetId]->(m:MergedThis|MergedThat|MergedOther)
MATCH (n:ExtractedThis|ExtractedThat|ExtractedOther)-[:hadPrimarySource]->(p:MergedPrimarySource)
WHERE
    m.identifier = $stable_target_id
RETURN
    m.identifier as stableTargetId,
    p.identifier as hadPrimarySource,
    n.identifierInPrimarySource as identifierInPrimarySource,
    n.identifier as identifier
ORDER BY n.identifier ASC
LIMIT $limit;""",
        ),
    ],
    ids=["all-filters", "no-filters", "id-filter"],
)
def test_fetch_identities(
    query_builder: QueryBuilder,
    had_primary_source: bool,
    identifier_in_primary_source: bool,
    stable_target_id: bool,
    expected: str,
) -> None:
    query = query_builder.fetch_identities(
        had_primary_source=had_primary_source,
        identifier_in_primary_source=identifier_in_primary_source,
        stable_target_id=stable_target_id,
    )
    assert query == expected


@pytest.mark.parametrize(
    (
        "ref_labels",
        "expected",
    ),
    [
        (
            ["personInCharge", "meetingScheduledBy", "agendaSignedOff"],
            """\
MATCH (source:ExtractedThat {identifier: $identifier})
CALL {
    WITH source
    MATCH (target_0 {identifier: $ref_identifiers[0]})
    MERGE (source)-[edge:personInCharge {position: $ref_positions[0]}]->(target_0)
    RETURN edge
    UNION
    MATCH (target_1 {identifier: $ref_identifiers[1]})
    MERGE (source)-[edge:meetingScheduledBy {position: $ref_positions[1]}]->(target_1)
    RETURN edge
    UNION
    MATCH (target_2 {identifier: $ref_identifiers[2]})
    MERGE (source)-[edge:agendaSignedOff {position: $ref_positions[2]}]->(target_2)
    RETURN edge
}
WITH source, collect(edge) as edges
CALL {
    WITH source, edges
    MATCH (source)-[gc]->(:MergedThis|MergedThat|MergedOther)
    WHERE NOT gc IN edges
    DELETE gc
    RETURN count(gc) as pruned
}
RETURN count(edges) as merged, pruned, edges;""",
        ),
        (
            [],
            """\
MATCH (source:ExtractedThat {identifier: $identifier})
CALL {
    RETURN null as edge
}
WITH source, collect(edge) as edges
CALL {
    WITH source, edges
    MATCH (source)-[gc]->(:MergedThis|MergedThat|MergedOther)
    WHERE NOT gc IN edges
    DELETE gc
    RETURN count(gc) as pruned
}
RETURN count(edges) as merged, pruned, edges;""",
        ),
    ],
    ids=["has-ref-labels", "no-ref-labels"],
)
def test_merge_edges(
    query_builder: QueryBuilder, ref_labels: list[str], expected: str
) -> None:
    query = query_builder.merge_edges(
        extracted_label="ExtractedThat", ref_labels=ref_labels
    )
    assert query == expected


@pytest.mark.parametrize(
    ("nested_edge_labels", "nested_node_labels", "expected"),
    [
        (
            ["description", "homepage", "geoLocation"],
            ["Text", "Link", "Location"],
            """\
MERGE (merged:MergedThat {identifier: $stable_target_id})
MERGE (extracted:ExtractedThat {identifier: $identifier})-[stableTargetId:stableTargetId {position: 0}]->(merged)
ON CREATE SET extracted = $on_create
ON MATCH SET extracted += $on_match
MERGE (extracted)-[edge_0:description {position: $nested_positions[0]}]->(value_0:Text)
ON CREATE SET value_0 = $nested_values[0]
ON MATCH SET value_0 += $nested_values[0]
MERGE (extracted)-[edge_1:homepage {position: $nested_positions[1]}]->(value_1:Link)
ON CREATE SET value_1 = $nested_values[1]
ON MATCH SET value_1 += $nested_values[1]
MERGE (extracted)-[edge_2:geoLocation {position: $nested_positions[2]}]->(value_2:Location)
ON CREATE SET value_2 = $nested_values[2]
ON MATCH SET value_2 += $nested_values[2]
WITH extracted,
    [edge_0, edge_1, edge_2] as edges,
    [value_0, value_1, value_2] as values
CALL {
    WITH values
    MATCH (:ExtractedThat {identifier: $identifier})-[]->(gc:Link|Text|Location)
    WHERE NOT gc IN values
    DETACH DELETE gc
    RETURN count(gc) as pruned
}
RETURN extracted, edges, values, pruned;""",
        ),
        (
            [],
            [],
            """\
MERGE (merged:MergedThat {identifier: $stable_target_id})
MERGE (extracted:ExtractedThat {identifier: $identifier})-[stableTargetId:stableTargetId {position: 0}]->(merged)
ON CREATE SET extracted = $on_create
ON MATCH SET extracted += $on_match
WITH extracted,
    [] as edges,
    [] as values
CALL {
    WITH values
    MATCH (:ExtractedThat {identifier: $identifier})-[]->(gc:Link|Text|Location)
    WHERE NOT gc IN values
    DETACH DELETE gc
    RETURN count(gc) as pruned
}
RETURN extracted, edges, values, pruned;""",
        ),
    ],
    ids=["has-nested-labels", "no-nested-labels"],
)
def test_merge_node(
    query_builder: QueryBuilder,
    nested_edge_labels: list[str],
    nested_node_labels: list[str],
    expected: str,
) -> None:
    query = query_builder.merge_node(
        extracted_label="ExtractedThat",
        merged_label="MergedThat",
        nested_edge_labels=nested_edge_labels,
        nested_node_labels=nested_node_labels,
    )
    assert query == expected
