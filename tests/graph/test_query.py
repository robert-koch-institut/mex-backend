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
    (
        "filter_by_query_string",
        "filter_by_stable_target_id",
        "filter_by_labels",
        "expected",
    ),
    [
        (
            True,
            True,
            True,
            """\
CALL {
    CALL db.index.fulltext.queryNodes("search_index", $query_string)
    YIELD node AS hit, score
    MATCH (n:ExtractedThis|ExtractedThat|ExtractedOther)-[:stableTargetId]->(merged:MergedThis|MergedThat|MergedOther)
    WHERE
        elementId(hit) = elementId(n)
        AND merged.identifier = $stable_target_id
        AND ANY(label IN labels(n) WHERE label IN $labels)
    RETURN COUNT(n) AS total
}
CALL {
    CALL db.index.fulltext.queryNodes("search_index", $query_string)
    YIELD node AS hit, score
    MATCH (n:ExtractedThis|ExtractedThat|ExtractedOther)-[:stableTargetId]->(merged:MergedThis|MergedThat|MergedOther)
    WHERE
        elementId(hit) = elementId(n)
        AND merged.identifier = $stable_target_id
        AND ANY(label IN labels(n) WHERE label IN $labels)
    CALL {
        WITH n
        MATCH (n)-[r]->(merged:MergedThis|MergedThat|MergedOther)
        RETURN type(r) as label, r.position as position, merged.identifier as value
    UNION
        WITH n
        MATCH (n)-[r]->(nested:Link|Text|Location)
        RETURN type(r) as label, r.position as position, properties(nested) as value
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
        MATCH (n)-[r]->(merged:MergedThis|MergedThat|MergedOther)
        RETURN type(r) as label, r.position as position, merged.identifier as value
    UNION
        WITH n
        MATCH (n)-[r]->(nested:Link|Text|Location)
        RETURN type(r) as label, r.position as position, properties(nested) as value
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
        MATCH (n)-[r]->(merged:MergedThis|MergedThat|MergedOther)
        RETURN type(r) as label, r.position as position, merged.identifier as value
    UNION
        WITH n
        MATCH (n)-[r]->(nested:Link|Text|Location)
        RETURN type(r) as label, r.position as position, properties(nested) as value
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
    filter_by_query_string: bool,
    filter_by_stable_target_id: bool,
    filter_by_labels: bool,
    expected: str,
) -> None:
    query = query_builder.fetch_extracted_data(
        filter_by_query_string=filter_by_query_string,
        filter_by_stable_target_id=filter_by_stable_target_id,
        filter_by_labels=filter_by_labels,
    )
    assert query == expected


@pytest.mark.parametrize(
    (
        "filter_by_had_primary_source",
        "filter_by_identifier_in_primary_source",
        "filter_by_stable_target_id",
        "expected",
    ),
    [
        (
            True,
            True,
            True,
            """\
MATCH (n:ExtractedThis|ExtractedThat|ExtractedOther)-[:stableTargetId]->(merged:MergedThis|MergedThat|MergedOther)
MATCH (n)-[:hadPrimarySource]->(primary_source:MergedPrimarySource)
WHERE
    primary_source.identifier = $had_primary_source
    AND n.identifierInPrimarySource = $identifier_in_primary_source
    AND merged.identifier = $stable_target_id
RETURN
    merged.identifier as stableTargetId,
    primary_source.identifier as hadPrimarySource,
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
MATCH (n:ExtractedThis|ExtractedThat|ExtractedOther)-[:stableTargetId]->(merged:MergedThis|MergedThat|MergedOther)
MATCH (n)-[:hadPrimarySource]->(primary_source:MergedPrimarySource)
RETURN
    merged.identifier as stableTargetId,
    primary_source.identifier as hadPrimarySource,
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
MATCH (n:ExtractedThis|ExtractedThat|ExtractedOther)-[:stableTargetId]->(merged:MergedThis|MergedThat|MergedOther)
MATCH (n)-[:hadPrimarySource]->(primary_source:MergedPrimarySource)
WHERE
    merged.identifier = $stable_target_id
RETURN
    merged.identifier as stableTargetId,
    primary_source.identifier as hadPrimarySource,
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
    filter_by_had_primary_source: bool,
    filter_by_identifier_in_primary_source: bool,
    filter_by_stable_target_id: bool,
    expected: str,
) -> None:
    query = query_builder.fetch_identities(
        filter_by_had_primary_source=filter_by_had_primary_source,
        filter_by_identifier_in_primary_source=filter_by_identifier_in_primary_source,
        filter_by_stable_target_id=filter_by_stable_target_id,
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
MATCH (source:ExtractedThat {identifier: $identifier})-[stableTargetId:stableTargetId]->({identifier: $stable_target_id})
CALL {
    WITH source
    MATCH (target_0 {identifier: $ref_identifiers[0]})
    MERGE (source)-[edge:personInCharge {position: $ref_positions[0]}]->(target_0)
    RETURN edge
    UNION
    WITH source
    MATCH (target_1 {identifier: $ref_identifiers[1]})
    MERGE (source)-[edge:meetingScheduledBy {position: $ref_positions[1]}]->(target_1)
    RETURN edge
    UNION
    WITH source
    MATCH (target_2 {identifier: $ref_identifiers[2]})
    MERGE (source)-[edge:agendaSignedOff {position: $ref_positions[2]}]->(target_2)
    RETURN edge
}
WITH source, collect(edge) as edges
CALL {
    WITH source, edges
    MATCH (source)-[outdated_edge]->(:MergedThis|MergedThat|MergedOther)
    WHERE NOT outdated_edge IN edges
    DELETE outdated_edge
    RETURN count(outdated_edge) as pruned
}
RETURN count(edges) as merged, pruned, edges;""",
        ),
        (
            [],
            """\
MATCH (source:ExtractedThat {identifier: $identifier})-[stableTargetId:stableTargetId]->({identifier: $stable_target_id})
CALL {
    RETURN null as edge
}
WITH source, collect(edge) as edges
CALL {
    WITH source, edges
    MATCH (source)-[outdated_edge]->(:MergedThis|MergedThat|MergedOther)
    WHERE NOT outdated_edge IN edges
    DELETE outdated_edge
    RETURN count(outdated_edge) as pruned
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
        current_label="ExtractedThat",
        current_constraints=["identifier"],
        ref_labels=ref_labels,
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
MERGE (current:ExtractedThat {identifier: $identifier})-[stableTargetId:stableTargetId {position: 0}]->(merged)
ON CREATE SET current = $on_create
ON MATCH SET current += $on_match
MERGE (current)-[edge_0:description {position: $nested_positions[0]}]->(value_0:Text)
ON CREATE SET value_0 = $nested_values[0]
ON MATCH SET value_0 += $nested_values[0]
MERGE (current)-[edge_1:homepage {position: $nested_positions[1]}]->(value_1:Link)
ON CREATE SET value_1 = $nested_values[1]
ON MATCH SET value_1 += $nested_values[1]
MERGE (current)-[edge_2:geoLocation {position: $nested_positions[2]}]->(value_2:Location)
ON CREATE SET value_2 = $nested_values[2]
ON MATCH SET value_2 += $nested_values[2]
WITH current,
    [edge_0, edge_1, edge_2] as edges,
    [value_0, value_1, value_2] as values
CALL {
    WITH current, values
    MATCH (current)-[]->(outdated_node:Link|Text|Location)
    WHERE NOT outdated_node IN values
    DETACH DELETE outdated_node
    RETURN count(outdated_node) as pruned
}
RETURN current, edges, values, pruned;""",
        ),
        (
            [],
            [],
            """\
MERGE (merged:MergedThat {identifier: $stable_target_id})
MERGE (current:ExtractedThat {identifier: $identifier})-[stableTargetId:stableTargetId {position: 0}]->(merged)
ON CREATE SET current = $on_create
ON MATCH SET current += $on_match
WITH current,
    [] as edges,
    [] as values
CALL {
    WITH current, values
    MATCH (current)-[]->(outdated_node:Link|Text|Location)
    WHERE NOT outdated_node IN values
    DETACH DELETE outdated_node
    RETURN count(outdated_node) as pruned
}
RETURN current, edges, values, pruned;""",
        ),
    ],
    ids=["has-nested-labels", "no-nested-labels"],
)
def test_merge_item(
    query_builder: QueryBuilder,
    nested_edge_labels: list[str],
    nested_node_labels: list[str],
    expected: str,
) -> None:
    query = query_builder.merge_item(
        current_label="ExtractedThat",
        current_constraints=["identifier"],
        merged_label="MergedThat",
        nested_edge_labels=nested_edge_labels,
        nested_node_labels=nested_node_labels,
    )
    assert query == expected
