import pytest
from pydantic import ValidationError

from mex.backend.graph.query import QueryBuilder, render_constraints


@pytest.fixture
def query_builder() -> QueryBuilder:
    builder = QueryBuilder.get()
    builder._env.globals.update(
        extracted_labels=["ExtractedThis", "ExtractedThat", "ExtractedOther"],
        merged_labels=["MergedThis", "MergedThat", "MergedOther"],
        nested_labels=["Link", "Text", "Location"],
        rule_labels=["AdditiveThis", "AdditiveThat", "AdditiveOther"],
        extracted_or_rule_labels=[
            "ExtractedThis",
            "ExtractedThat",
            "ExtractedOther",
            "AdditiveThis",
            "AdditiveThat",
            "AdditiveOther",
        ],
    )
    return builder


def test_render_constraints() -> None:
    with pytest.raises(ValidationError):
        render_constraints(["this-fi5ld doesn't match the p4tt3rn!", "thisIsOk"])

    assert render_constraints(["someField", "another"]) == (
        "someField: $someField, another: $another"
    )


def test_create_full_text_search_index(query_builder: QueryBuilder) -> None:
    query = query_builder.create_full_text_search_index(
        node_labels=["Apple", "Orange"],
        search_fields=["texture", "sugarContent", "color"],
    )
    assert (
        str(query)
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
        str(query)
        == """\
CREATE CONSTRAINT blue_berry_pie_identifier_uniqueness IF NOT EXISTS
FOR (n:BlueBerryPie)
REQUIRE n.identifier IS UNIQUE;"""
    )


def test_fetch_database_status(query_builder: QueryBuilder) -> None:
    query = query_builder.fetch_database_status()
    assert (
        str(query)
        == """\
SHOW DEFAULT DATABASE
YIELD currentStatus;"""
    )


@pytest.mark.parametrize(
    (
        "filter_by_query_string",
        "filter_by_stable_target_id",
        "expected",
    ),
    [
        (
            True,
            True,
            r"""CALL () {
    OPTIONAL CALL db.index.fulltext.queryNodes("search_index", $query_string)
    YIELD node AS hit, score
    CALL (hit) {
        MATCH (extracted_or_rule_node:ExtractedThis|ExtractedThat|ExtractedOther|AdditiveThis|AdditiveThat|AdditiveOther)-[:stableTargetId]->(merged_node:MergedThis|MergedThat|MergedOther)
        WHERE
            elementId(hit) = elementId(extracted_or_rule_node)
        RETURN extracted_or_rule_node, merged_node
    UNION
        MATCH (nested_node:Link|Text|Location)<-[]-(extracted_or_rule_node:ExtractedThis|ExtractedThat|ExtractedOther|AdditiveThis|AdditiveThat|AdditiveOther)-[:stableTargetId]->(merged_node:MergedThis|MergedThat|MergedOther)
        WHERE
            elementId(hit) = elementId(nested_node)
        RETURN extracted_or_rule_node, merged_node
    }
    WITH DISTINCT extracted_or_rule_node, merged_node
    WHERE
        ANY(label IN labels(extracted_or_rule_node) WHERE label IN $labels)
        AND merged_node.identifier = $stable_target_id
    RETURN COUNT(extracted_or_rule_node) AS total
}
CALL () {
    OPTIONAL CALL db.index.fulltext.queryNodes("search_index", $query_string)
    YIELD node AS hit, score
    CALL (hit) {
        MATCH (extracted_or_rule_node:ExtractedThis|ExtractedThat|ExtractedOther|AdditiveThis|AdditiveThat|AdditiveOther)-[:stableTargetId]->(merged_node:MergedThis|MergedThat|MergedOther)
        WHERE
            elementId(hit) = elementId(extracted_or_rule_node)
        RETURN extracted_or_rule_node, merged_node
    UNION
        MATCH (nested_node:Link|Text|Location)<-[]-(extracted_or_rule_node:ExtractedThis|ExtractedThat|ExtractedOther|AdditiveThis|AdditiveThat|AdditiveOther)-[:stableTargetId]->(merged_node:MergedThis|MergedThat|MergedOther)
        WHERE
            elementId(hit) = elementId(nested_node)
        RETURN extracted_or_rule_node, merged_node
    }
    WITH DISTINCT extracted_or_rule_node, merged_node
    WHERE
        ANY(label IN labels(extracted_or_rule_node) WHERE label IN $labels)
        AND merged_node.identifier = $stable_target_id
    ORDER BY extracted_or_rule_node.identifier, head(labels(extracted_or_rule_node)) ASC
    SKIP $skip
    LIMIT $limit
    WITH
        extracted_or_rule_node,
        [
            (extracted_or_rule_node)-[r]->(referenced_merged_node:MergedThis|MergedThat|MergedOther) |
            {value: referenced_merged_node.identifier, position:r.position, label: type(r)}
        ] + [
            (extracted_or_rule_node)-[r]->(referenced_nested_node:Link|Text|Location) |
            {value: properties(referenced_nested_node), position:r.position , label: type(r)}
        ] AS refs
    WITH
        collect(
            extracted_or_rule_node{.*, entityType: head(labels(extracted_or_rule_node)), _refs: refs}
        ) AS items
    RETURN items
}
RETURN items, total;""",
        ),
        (
            False,
            False,
            r"""CALL () {
    OPTIONAL MATCH (extracted_or_rule_node:ExtractedThis|ExtractedThat|ExtractedOther|AdditiveThis|AdditiveThat|AdditiveOther)-[:stableTargetId]->(merged_node:MergedThis|MergedThat|MergedOther)
    WHERE
        ANY(label IN labels(extracted_or_rule_node) WHERE label IN $labels)
    RETURN COUNT(extracted_or_rule_node) AS total
}
CALL () {
    OPTIONAL MATCH (extracted_or_rule_node:ExtractedThis|ExtractedThat|ExtractedOther|AdditiveThis|AdditiveThat|AdditiveOther)-[:stableTargetId]->(merged_node:MergedThis|MergedThat|MergedOther)
    WHERE
        ANY(label IN labels(extracted_or_rule_node) WHERE label IN $labels)
    ORDER BY extracted_or_rule_node.identifier, head(labels(extracted_or_rule_node)) ASC
    SKIP $skip
    LIMIT $limit
    WITH
        extracted_or_rule_node,
        [
            (extracted_or_rule_node)-[r]->(referenced_merged_node:MergedThis|MergedThat|MergedOther) |
            {value: referenced_merged_node.identifier, position:r.position, label: type(r)}
        ] + [
            (extracted_or_rule_node)-[r]->(referenced_nested_node:Link|Text|Location) |
            {value: properties(referenced_nested_node), position:r.position , label: type(r)}
        ] AS refs
    WITH
        collect(
            extracted_or_rule_node{.*, entityType: head(labels(extracted_or_rule_node)), _refs: refs}
        ) AS items
    RETURN items
}
RETURN items, total;""",
        ),
    ],
    ids=["all-filters", "no-filters"],
)
def test_fetch_extracted_or_rule_items(
    query_builder: QueryBuilder,
    filter_by_query_string: bool,
    filter_by_stable_target_id: bool,
    expected: str,
) -> None:
    query = query_builder.fetch_extracted_or_rule_items(
        filter_by_query_string=filter_by_query_string,
        filter_by_stable_target_id=filter_by_stable_target_id,
    )
    assert str(query) == expected


@pytest.mark.parametrize(
    (
        "filter_by_query_string",
        "filter_by_identifier",
        "filter_by_reference_to_merged_item",
        "expected",
    ),
    [
        (
            True,
            True,
            True,
            r"""CALL () {
    OPTIONAL CALL db.index.fulltext.queryNodes("search_index", $query_string)
    YIELD node AS hit, score
    CALL (hit) {
        MATCH (extracted_or_rule_node:ExtractedThis|ExtractedThat|ExtractedOther|AdditiveThis|AdditiveThat|AdditiveOther)-[:stableTargetId]->(merged_node:MergedThis|MergedThat|MergedOther)
        WHERE
            elementId(hit) = elementId(extracted_or_rule_node)
        RETURN merged_node
    UNION
        MATCH (nested_node:Link|Text|Location)<-[]-(:ExtractedThis|ExtractedThat|ExtractedOther|AdditiveThis|AdditiveThat|AdditiveOther)-[:stableTargetId]->(merged_node:MergedThis|MergedThat|MergedOther)
        WHERE
            elementId(hit) = elementId(nested_node)
        RETURN merged_node
    }
    WITH DISTINCT merged_node AS merged_node
    MATCH (merged_node)<-[:stableTargetId]-(:ExtractedThis|ExtractedThat|ExtractedOther)-[:hadPrimarySource]->(referenced_merged_node_to_filter_by)
    WHERE
        ANY(label IN labels(merged_node) WHERE label IN $labels)
        AND merged_node.identifier = $identifier
        AND referenced_merged_node_to_filter_by.identifier IN $referenced_identifiers
    RETURN COUNT(merged_node) AS total
}
CALL () {
    OPTIONAL CALL db.index.fulltext.queryNodes("search_index", $query_string)
    YIELD node AS hit, score
    CALL (hit) {
        MATCH (extracted_or_rule_node:ExtractedThis|ExtractedThat|ExtractedOther|AdditiveThis|AdditiveThat|AdditiveOther)-[:stableTargetId]->(merged_node:MergedThis|MergedThat|MergedOther)
        WHERE
            elementId(hit) = elementId(extracted_or_rule_node)
        RETURN merged_node
    UNION
        MATCH (nested_node:Link|Text|Location)<-[]-(:ExtractedThis|ExtractedThat|ExtractedOther|AdditiveThis|AdditiveThat|AdditiveOther)-[:stableTargetId]->(merged_node:MergedThis|MergedThat|MergedOther)
        WHERE
            elementId(hit) = elementId(nested_node)
        RETURN merged_node
    }
    WITH DISTINCT merged_node AS merged_node
    MATCH (merged_node)<-[:stableTargetId]-(:ExtractedThis|ExtractedThat|ExtractedOther)-[:hadPrimarySource]->(referenced_merged_node_to_filter_by)
    WHERE
        ANY(label IN labels(merged_node) WHERE label IN $labels)
        AND merged_node.identifier = $identifier
        AND referenced_merged_node_to_filter_by.identifier IN $referenced_identifiers
    ORDER BY merged_node.identifier, head(labels(merged_node)) ASC
    SKIP $skip
    LIMIT $limit
    OPTIONAL MATCH (extracted_or_rule_node)-[:stableTargetId]->(merged_node)
    ORDER BY extracted_or_rule_node.identifier, head(labels(extracted_or_rule_node)) ASC
    WITH
        extracted_or_rule_node,
        merged_node,
        [
            (extracted_or_rule_node)-[r]->(referenced_merged_node:MergedThis|MergedThat|MergedOther) |
            {value: referenced_merged_node.identifier, position:r.position, label: type(r)}
        ] + [
            (extracted_or_rule_node)-[r]->(referenced_nested_node:Link|Text|Location) |
            {value: properties(referenced_nested_node), position:r.position , label: type(r)}
        ] AS refs
    WITH
        merged_node,
        collect(
            extracted_or_rule_node{.*, entityType: head(labels(extracted_or_rule_node)), _refs: refs}
        ) AS components
    WITH
        collect(
            merged_node{.*, entityType: head(labels(merged_node)), _components: components}
        ) AS items
    RETURN items
}
RETURN items, total;""",
        ),
        (
            False,
            False,
            False,
            r"""CALL () {
    OPTIONAL MATCH (merged_node:MergedThis|MergedThat|MergedOther)
    WHERE
        ANY(label IN labels(merged_node) WHERE label IN $labels)
    RETURN COUNT(merged_node) AS total
}
CALL () {
    OPTIONAL MATCH (merged_node:MergedThis|MergedThat|MergedOther)
    WHERE
        ANY(label IN labels(merged_node) WHERE label IN $labels)
    ORDER BY merged_node.identifier, head(labels(merged_node)) ASC
    SKIP $skip
    LIMIT $limit
    OPTIONAL MATCH (extracted_or_rule_node)-[:stableTargetId]->(merged_node)
    ORDER BY extracted_or_rule_node.identifier, head(labels(extracted_or_rule_node)) ASC
    WITH
        extracted_or_rule_node,
        merged_node,
        [
            (extracted_or_rule_node)-[r]->(referenced_merged_node:MergedThis|MergedThat|MergedOther) |
            {value: referenced_merged_node.identifier, position:r.position, label: type(r)}
        ] + [
            (extracted_or_rule_node)-[r]->(referenced_nested_node:Link|Text|Location) |
            {value: properties(referenced_nested_node), position:r.position , label: type(r)}
        ] AS refs
    WITH
        merged_node,
        collect(
            extracted_or_rule_node{.*, entityType: head(labels(extracted_or_rule_node)), _refs: refs}
        ) AS components
    WITH
        collect(
            merged_node{.*, entityType: head(labels(merged_node)), _components: components}
        ) AS items
    RETURN items
}
RETURN items, total;""",
        ),
    ],
    ids=["all-filters", "no-filters"],
)
def test_fetch_merged_items(
    query_builder: QueryBuilder,
    filter_by_query_string: bool,
    filter_by_identifier: bool,
    filter_by_reference_to_merged_item: bool,
    expected: str,
) -> None:
    query = query_builder.fetch_merged_items(
        filter_by_query_string=filter_by_query_string,
        filter_by_identifier=filter_by_identifier,
        filter_by_reference_to_merged_item=filter_by_reference_to_merged_item,
        reference_field_name="hadPrimarySource",
    )
    assert str(query) == expected


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
    merged.identifier AS stableTargetId,
    primary_source.identifier AS hadPrimarySource,
    n.identifierInPrimarySource AS identifierInPrimarySource,
    n.identifier AS identifier
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
    merged.identifier AS stableTargetId,
    primary_source.identifier AS hadPrimarySource,
    n.identifierInPrimarySource AS identifierInPrimarySource,
    n.identifier AS identifier
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
    merged.identifier AS stableTargetId,
    primary_source.identifier AS hadPrimarySource,
    n.identifierInPrimarySource AS identifierInPrimarySource,
    n.identifier AS identifier
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
    assert str(query) == expected


@pytest.mark.parametrize(
    (
        "ref_labels",
        "expected",
    ),
    [
        (
            ["personInCharge", "meetingScheduledBy", "agendaSignedOff"],
            """\
MATCH (source:ExtractedThat {identifier: $identifier})-[:stableTargetId]->({identifier: $stable_target_id})
CALL (source) {
    WITH source
    MATCH (target_0 {identifier: $ref_identifiers[0]})
    MERGE (source)-[edge:personInCharge {position: $ref_positions[0]}]->(target_0)
    RETURN edge
    UNION ALL
    WITH source
    MATCH (target_1 {identifier: $ref_identifiers[1]})
    MERGE (source)-[edge:meetingScheduledBy {position: $ref_positions[1]}]->(target_1)
    RETURN edge
    UNION ALL
    WITH source
    MATCH (target_2 {identifier: $ref_identifiers[2]})
    MERGE (source)-[edge:agendaSignedOff {position: $ref_positions[2]}]->(target_2)
    RETURN edge
}
WITH source, count(edge) AS merged, collect(edge) AS edges
CALL (source, edges) {
    MATCH (source)-[outdated_edge]->(:MergedThis|MergedThat|MergedOther)
    WHERE NOT outdated_edge IN edges
    DELETE outdated_edge
    RETURN count(outdated_edge) AS pruned
}
RETURN merged, pruned, edges;""",
        ),
        (
            [],
            """\
MATCH (source:ExtractedThat {identifier: $identifier})-[:stableTargetId]->({identifier: $stable_target_id})
CALL (source) {
    RETURN null AS edge
}
WITH source, count(edge) AS merged, collect(edge) AS edges
CALL (source, edges) {
    MATCH (source)-[outdated_edge]->(:MergedThis|MergedThat|MergedOther)
    WHERE NOT outdated_edge IN edges
    DELETE outdated_edge
    RETURN count(outdated_edge) AS pruned
}
RETURN merged, pruned, edges;""",
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
    assert str(query) == expected


@pytest.mark.parametrize(
    ("nested_edge_labels", "nested_node_labels", "expected"),
    [
        (
            ["description", "homepage", "geoLocation"],
            ["Text", "Link", "Location"],
            """\
MERGE (merged:MergedThat {identifier: $stable_target_id})
MERGE (current:ExtractedThat {identifier: $identifier})-[:stableTargetId {position: 0}]->(merged)
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
    [edge_0, edge_1, edge_2] AS edges,
    [value_0, value_1, value_2] AS values
CALL (current, values) {
    MATCH (current)-[]->(outdated_node:Link|Text|Location)
    WHERE NOT outdated_node IN values
    DETACH DELETE outdated_node
    RETURN count(outdated_node) AS pruned
}
RETURN current, edges, values, pruned;""",
        ),
        (
            [],
            [],
            """\
MERGE (merged:MergedThat {identifier: $stable_target_id})
MERGE (current:ExtractedThat {identifier: $identifier})-[:stableTargetId {position: 0}]->(merged)
ON CREATE SET current = $on_create
ON MATCH SET current += $on_match
WITH current,
    [] AS edges,
    [] AS values
CALL (current, values) {
    MATCH (current)-[]->(outdated_node:Link|Text|Location)
    WHERE NOT outdated_node IN values
    DETACH DELETE outdated_node
    RETURN count(outdated_node) AS pruned
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
    assert str(query) == expected
