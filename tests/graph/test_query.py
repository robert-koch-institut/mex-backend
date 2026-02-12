import pytest
from pydantic import ValidationError

from mex.backend.graph.query import QueryBuilder, render_constraints


@pytest.fixture
def query_builder() -> QueryBuilder:
    builder = QueryBuilder.get()
    builder._env.globals.update(
        extracted_labels=[
            "ExtractedPerson",
            "ExtractedVariable",
            "ExtractedDistribution",
        ],
        merged_labels=["MergedPerson", "MergedVariable", "MergedDistribution"],
        nested_labels=["Link", "Text", "Location"],
        rule_labels=["AdditivePerson", "AdditiveVariable", "AdditiveDistribution"],
        extracted_or_rule_labels=[
            "ExtractedPerson",
            "ExtractedVariable",
            "ExtractedDistribution",
            "AdditivePerson",
            "AdditiveVariable",
            "AdditiveDistribution",
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
        query.render()
        == """\
CREATE FULLTEXT INDEX search_index IF NOT EXISTS
FOR (n:Apple|Orange)
ON EACH [n.texture, n.sugarContent, n.color]
OPTIONS {indexConfig: $index_config};"""
    )


def test_create_identifier_constraint(query_builder: QueryBuilder) -> None:
    query = query_builder.create_identifier_constraint(node_label="BlueBerryPie")
    assert (
        query.render()
        == """\
CREATE CONSTRAINT blue_berry_pie_identifier_uniqueness IF NOT EXISTS
FOR (n:BlueBerryPie)
REQUIRE n.identifier IS UNIQUE;"""
    )


def test_create_provenance_constraint(query_builder: QueryBuilder) -> None:
    query = query_builder.create_provenance_constraint(node_label="BlueBerryPie")
    assert (
        query.render()
        == """\
CREATE CONSTRAINT blue_berry_pie_provenance_uniqueness IF NOT EXISTS
FOR (n:BlueBerryPie)
REQUIRE (n.hadPrimarySource, n.identifierInPrimarySource) IS UNIQUE;"""
    )


def test_delete_merged_item(query_builder: QueryBuilder) -> None:
    query = query_builder.delete_merged_item()
    assert (
        query.render()
        == """\
MATCH (merged:MergedPerson|MergedVariable|MergedDistribution {identifier: $identifier})
OPTIONAL MATCH (merged)<-[:stableTargetId]-(extracted:ExtractedPerson|ExtractedVariable|ExtractedDistribution)
OPTIONAL MATCH (merged)<-[:stableTargetId]-(rule:AdditivePerson|AdditiveVariable|AdditiveDistribution)
OPTIONAL MATCH (merged)-[outbound]->()
OPTIONAL MATCH (extracted)-[]->(extracted_nested:Link|Text|Location)
OPTIONAL MATCH (rule)-[]->(rule_nested:Link|Text|Location)

DETACH DELETE extracted_nested, rule_nested
DETACH DELETE extracted, rule
DELETE outbound
DELETE merged

RETURN
    count(DISTINCT merged) AS deleted_merged_count,
    count(DISTINCT extracted) AS deleted_extracted_count,
    count(DISTINCT rule) AS deleted_rule_count,
    count(DISTINCT extracted_nested) + count(DISTINCT rule_nested) AS deleted_nested_count;"""
    )


def test_fetch_database_status(query_builder: QueryBuilder) -> None:
    query = query_builder.fetch_database_status()
    assert (
        query.render()
        == """\
SHOW DEFAULT DATABASE
YIELD currentStatus;"""
    )


@pytest.mark.parametrize(
    (
        "enable_filters",
        "expected",
    ),
    [
        pytest.param(
            True,
            r"""CALL () {
    OPTIONAL CALL db.index.fulltext.queryNodes("search_index", $query_string)
    YIELD node AS hit, score
    CALL (hit) {
        MATCH (hit:ExtractedPerson|ExtractedVariable|ExtractedDistribution|AdditivePerson|AdditiveVariable|AdditiveDistribution)-[:stableTargetId]->(merged_node:MergedPerson|MergedVariable|MergedDistribution)
        RETURN hit as extracted_or_rule_node, merged_node
    UNION
        MATCH (hit:Link|Text|Location)<-[]-(extracted_or_rule_node:ExtractedPerson|ExtractedVariable|ExtractedDistribution|AdditivePerson|AdditiveVariable|AdditiveDistribution)-[:stableTargetId]->(merged_node:MergedPerson|MergedVariable|MergedDistribution)
        RETURN extracted_or_rule_node, merged_node
    }
    WITH DISTINCT extracted_or_rule_node, merged_node
    WHERE
        ANY(label IN labels(extracted_or_rule_node) WHERE label IN $labels)
        AND extracted_or_rule_node.identifier = $identifier
        AND merged_node.identifier = $stable_target_id
    CALL (extracted_or_rule_node) {
        MATCH (extracted_or_rule_node:AdditivePerson|AdditiveVariable|AdditiveDistribution)
        RETURN extracted_or_rule_node AS filtered_node
    UNION
        MATCH (extracted_or_rule_node)-[:hadPrimarySource]->(referenced_node_to_filter_by)
        WHERE
            referenced_node_to_filter_by.identifier IN $referenced_identifiers
        RETURN extracted_or_rule_node AS filtered_node
    }
    WITH filtered_node as extracted_or_rule_node, merged_node
    RETURN COUNT(extracted_or_rule_node) AS total
}
CALL () {
    OPTIONAL CALL db.index.fulltext.queryNodes("search_index", $query_string)
    YIELD node AS hit, score
    CALL (hit) {
        MATCH (hit:ExtractedPerson|ExtractedVariable|ExtractedDistribution|AdditivePerson|AdditiveVariable|AdditiveDistribution)-[:stableTargetId]->(merged_node:MergedPerson|MergedVariable|MergedDistribution)
        RETURN hit as extracted_or_rule_node, merged_node
    UNION
        MATCH (hit:Link|Text|Location)<-[]-(extracted_or_rule_node:ExtractedPerson|ExtractedVariable|ExtractedDistribution|AdditivePerson|AdditiveVariable|AdditiveDistribution)-[:stableTargetId]->(merged_node:MergedPerson|MergedVariable|MergedDistribution)
        RETURN extracted_or_rule_node, merged_node
    }
    WITH DISTINCT extracted_or_rule_node, merged_node
    WHERE
        ANY(label IN labels(extracted_or_rule_node) WHERE label IN $labels)
        AND extracted_or_rule_node.identifier = $identifier
        AND merged_node.identifier = $stable_target_id
    CALL (extracted_or_rule_node) {
        MATCH (extracted_or_rule_node:AdditivePerson|AdditiveVariable|AdditiveDistribution)
        RETURN extracted_or_rule_node AS filtered_node
    UNION
        MATCH (extracted_or_rule_node)-[:hadPrimarySource]->(referenced_node_to_filter_by)
        WHERE
            referenced_node_to_filter_by.identifier IN $referenced_identifiers
        RETURN extracted_or_rule_node AS filtered_node
    }
    WITH filtered_node as extracted_or_rule_node, merged_node
    ORDER BY merged_node.identifier, extracted_or_rule_node.identifier, head(labels(extracted_or_rule_node)) ASC
    SKIP $skip
    LIMIT $limit
    WITH
        extracted_or_rule_node,
        [
            (extracted_or_rule_node)-[r]->(referenced_merged_node:MergedPerson|MergedVariable|MergedDistribution) |
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
            id="all-filters",
        ),
        pytest.param(
            False,
            r"""CALL () {
    OPTIONAL MATCH (extracted_or_rule_node:ExtractedPerson|ExtractedVariable|ExtractedDistribution|AdditivePerson|AdditiveVariable|AdditiveDistribution)-[:stableTargetId]->(merged_node:MergedPerson|MergedVariable|MergedDistribution)
    WHERE
        ANY(label IN labels(extracted_or_rule_node) WHERE label IN $labels)
    RETURN COUNT(extracted_or_rule_node) AS total
}
CALL () {
    OPTIONAL MATCH (extracted_or_rule_node:ExtractedPerson|ExtractedVariable|ExtractedDistribution|AdditivePerson|AdditiveVariable|AdditiveDistribution)-[:stableTargetId]->(merged_node:MergedPerson|MergedVariable|MergedDistribution)
    WHERE
        ANY(label IN labels(extracted_or_rule_node) WHERE label IN $labels)
    ORDER BY merged_node.identifier, extracted_or_rule_node.identifier, head(labels(extracted_or_rule_node)) ASC
    SKIP $skip
    LIMIT $limit
    WITH
        extracted_or_rule_node,
        [
            (extracted_or_rule_node)-[r]->(referenced_merged_node:MergedPerson|MergedVariable|MergedDistribution) |
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
            id="no-filters",
        ),
    ],
)
def test_fetch_extracted_or_rule_items(
    query_builder: QueryBuilder,
    enable_filters: bool,  # noqa: FBT001
    expected: str,
) -> None:
    query = query_builder.fetch_extracted_or_rule_items(
        filter_by_query_string=enable_filters,
        filter_by_identifier=enable_filters,
        filter_by_stable_target_id=enable_filters,
        filter_by_referenced_identifiers=enable_filters,
        filter_rule_items=enable_filters,
        reference_field="hadPrimarySource",
    )
    assert query.render() == expected


@pytest.mark.parametrize(
    (
        "filter_by_query_string",
        "filter_by_identifier",
        "filter_by_referenced_identifiers",
        "filter_items_with_rules",
        "expected",
    ),
    [
        pytest.param(
            True,
            True,
            True,
            True,
            r"""CALL () {
    OPTIONAL CALL db.index.fulltext.queryNodes("search_index", $query_string)
    YIELD node AS hit, score
    CALL (hit) {
        MATCH (hit:ExtractedPerson|ExtractedVariable|ExtractedDistribution|AdditivePerson|AdditiveVariable|AdditiveDistribution)-[:stableTargetId]->(merged_node:MergedPerson|MergedVariable|MergedDistribution)
        RETURN hit as extracted_or_rule_node, merged_node
    UNION
        MATCH (hit:Link|Text|Location)<-[]-(extracted_or_rule_node:ExtractedPerson|ExtractedVariable|ExtractedDistribution|AdditivePerson|AdditiveVariable|AdditiveDistribution)-[:stableTargetId]->(merged_node:MergedPerson|MergedVariable|MergedDistribution)
        RETURN extracted_or_rule_node, merged_node
    }
    WITH DISTINCT merged_node AS merged_node
    WHERE
        ANY(label IN labels(merged_node) WHERE label IN $labels)
        AND merged_node.identifier = $identifier
    MATCH (merged_node)
    WHERE
        EXISTS {(merged_node)<-[:stableTargetId]-(:AdditivePerson|AdditiveVariable|AdditiveDistribution)}
        OR
        EXISTS {
            MATCH (merged_node)<-[:stableTargetId]-()-[:hadPrimarySource]->(referenced_node_to_filter_by)
            WHERE
                referenced_node_to_filter_by.identifier IN $referenced_identifiers
        }
    RETURN COUNT(merged_node) AS total
}
CALL () {
    OPTIONAL CALL db.index.fulltext.queryNodes("search_index", $query_string)
    YIELD node AS hit, score
    CALL (hit) {
        MATCH (hit:ExtractedPerson|ExtractedVariable|ExtractedDistribution|AdditivePerson|AdditiveVariable|AdditiveDistribution)-[:stableTargetId]->(merged_node:MergedPerson|MergedVariable|MergedDistribution)
        RETURN hit as extracted_or_rule_node, merged_node
    UNION
        MATCH (hit:Link|Text|Location)<-[]-(extracted_or_rule_node:ExtractedPerson|ExtractedVariable|ExtractedDistribution|AdditivePerson|AdditiveVariable|AdditiveDistribution)-[:stableTargetId]->(merged_node:MergedPerson|MergedVariable|MergedDistribution)
        RETURN extracted_or_rule_node, merged_node
    }
    WITH DISTINCT merged_node AS merged_node
    WHERE
        ANY(label IN labels(merged_node) WHERE label IN $labels)
        AND merged_node.identifier = $identifier
    MATCH (merged_node)
    WHERE
        EXISTS {(merged_node)<-[:stableTargetId]-(:AdditivePerson|AdditiveVariable|AdditiveDistribution)}
        OR
        EXISTS {
            MATCH (merged_node)<-[:stableTargetId]-()-[:hadPrimarySource]->(referenced_node_to_filter_by)
            WHERE
                referenced_node_to_filter_by.identifier IN $referenced_identifiers
        }
    ORDER BY merged_node.identifier ASC
    SKIP $skip
    LIMIT $limit
    OPTIONAL MATCH (extracted_or_rule_node)-[:stableTargetId]->(merged_node)
    ORDER BY merged_node.identifier, extracted_or_rule_node.identifier, head(labels(extracted_or_rule_node)) ASC
    WITH
        extracted_or_rule_node,
        merged_node,
        [
            (extracted_or_rule_node)-[r]->(referenced_merged_node:MergedPerson|MergedVariable|MergedDistribution) |
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
            id="all-filters",
        ),
        pytest.param(
            False,
            False,
            False,
            False,
            r"""CALL () {
    OPTIONAL MATCH (merged_node:MergedPerson|MergedVariable|MergedDistribution)
    WHERE
        ANY(label IN labels(merged_node) WHERE label IN $labels)
    RETURN COUNT(merged_node) AS total
}
CALL () {
    OPTIONAL MATCH (merged_node:MergedPerson|MergedVariable|MergedDistribution)
    WHERE
        ANY(label IN labels(merged_node) WHERE label IN $labels)
    ORDER BY merged_node.identifier ASC
    SKIP $skip
    LIMIT $limit
    OPTIONAL MATCH (extracted_or_rule_node)-[:stableTargetId]->(merged_node)
    ORDER BY merged_node.identifier, extracted_or_rule_node.identifier, head(labels(extracted_or_rule_node)) ASC
    WITH
        extracted_or_rule_node,
        merged_node,
        [
            (extracted_or_rule_node)-[r]->(referenced_merged_node:MergedPerson|MergedVariable|MergedDistribution) |
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
            id="no-filters",
        ),
    ],
)
def test_fetch_merged_items(  # noqa: PLR0913
    query_builder: QueryBuilder,
    filter_by_query_string: bool,  # noqa: FBT001
    filter_by_identifier: bool,  # noqa: FBT001
    filter_by_referenced_identifiers: bool,  # noqa: FBT001
    filter_items_with_rules: bool,  # noqa: FBT001
    expected: str,
) -> None:
    query = query_builder.fetch_merged_items(
        filter_by_query_string=filter_by_query_string,
        filter_by_identifier=filter_by_identifier,
        filter_by_referenced_identifiers=filter_by_referenced_identifiers,
        filter_items_with_rules=filter_items_with_rules,
        reference_field="hadPrimarySource",
    )
    assert query.render() == expected


@pytest.mark.parametrize(
    (
        "filter_by_had_primary_source",
        "filter_by_identifier_in_primary_source",
        "filter_by_stable_target_id",
        "expected",
    ),
    [
        pytest.param(
            True,
            True,
            True,
            """\
MATCH (n:ExtractedPerson|ExtractedVariable|ExtractedDistribution)-[:stableTargetId]->(merged:MergedPerson|MergedVariable|MergedDistribution)
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
            id="all-filters",
        ),
        pytest.param(
            False,
            False,
            False,
            """\
MATCH (n:ExtractedPerson|ExtractedVariable|ExtractedDistribution)-[:stableTargetId]->(merged:MergedPerson|MergedVariable|MergedDistribution)
MATCH (n)-[:hadPrimarySource]->(primary_source:MergedPrimarySource)
RETURN
    merged.identifier AS stableTargetId,
    primary_source.identifier AS hadPrimarySource,
    n.identifierInPrimarySource AS identifierInPrimarySource,
    n.identifier AS identifier
ORDER BY n.identifier ASC
LIMIT $limit;""",
            id="no-filters",
        ),
        pytest.param(
            False,
            False,
            True,
            """\
MATCH (n:ExtractedPerson|ExtractedVariable|ExtractedDistribution)-[:stableTargetId]->(merged:MergedPerson|MergedVariable|MergedDistribution)
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
            id="id-filter",
        ),
    ],
)
def test_fetch_identities(
    query_builder: QueryBuilder,
    filter_by_had_primary_source: bool,  # noqa: FBT001
    filter_by_identifier_in_primary_source: bool,  # noqa: FBT001
    filter_by_stable_target_id: bool,  # noqa: FBT001
    expected: str,
) -> None:
    query = query_builder.fetch_identities(
        filter_by_had_primary_source=filter_by_had_primary_source,
        filter_by_identifier_in_primary_source=filter_by_identifier_in_primary_source,
        filter_by_stable_target_id=filter_by_stable_target_id,
    )
    assert query.render() == expected


def test_delete_rule_set(query_builder: QueryBuilder) -> None:
    query = query_builder.delete_rule_set()
    assert (
        query.render()
        == """\
MATCH (merged:MergedPerson|MergedVariable|MergedDistribution {identifier: $stable_target_id})
OPTIONAL MATCH (merged)<-[:stableTargetId]-(rule:AdditivePerson|AdditiveVariable|AdditiveDistribution)
OPTIONAL MATCH (rule)-[outbound]->()
OPTIONAL MATCH (rule)-[]->(nested:Link|Text|Location)

DETACH DELETE nested
DELETE outbound
DETACH DELETE rule

RETURN
    merged,
    count(DISTINCT rule) AS deleted_rule_count,
    count(DISTINCT nested) AS deleted_nested_count;"""
    )


def test_merge_item(query_builder: QueryBuilder) -> None:
    query = query_builder.get_ingest_query_for_entity_type("ExtractedVariable")
    assert (
        query
        == """\
CYPHER 25

WITH $data AS data

MERGE (merged:MergedVariable {identifier: data.stableTargetId})
CALL (merged, data) {
  WHEN data.identifier IS NOT NULL THEN {
    MERGE (main:ExtractedVariable {identifier: data.identifier})-[:stableTargetId {position: 0}]->(merged)
    RETURN main
  }
  ELSE {
    MERGE (main:ExtractedVariable )-[:stableTargetId {position: 0}]->(merged)
    RETURN main
  }
}

SET main += data.nodeProps

WITH main, merged.identifier AS stableTargetId, data

CALL (main, data) {
    UNWIND data.createRels AS createRel
    MERGE (main)-[newCreateEdge:$(createRel.edgeLabel) {position: createRel.edgeProps.position}]->(creationTarget:$(createRel.nodeLabels))
    SET creationTarget += createRel.nodeProps
    RETURN collect(newCreateEdge) as newCreateEdges
}

CALL (main, data) {
    UNWIND data.linkRels AS linkRel
    MATCH (linkTarget:MergedPrimarySource|MergedResource|MergedVariable|MergedVariableGroup {identifier: linkRel.nodeProps.identifier})
    MERGE (main)-[newLinkEdge:$(linkRel.edgeLabel) {position: linkRel.edgeProps.position}]->(linkTarget)
    RETURN collect(newLinkEdge) as newLinkEdges
}

CALL (main, newCreateEdges) {
    OPTIONAL MATCH (main)-[gcEdge:description|label]->(gcNode:Text)
        WHERE NOT gcEdge IN newCreateEdges
    DETACH DELETE gcNode
}

CALL (main, newLinkEdges) {
    OPTIONAL MATCH (main)-[gcEdge:belongsTo|hadPrimarySource|usedIn]->(:MergedPrimarySource|MergedResource|MergedVariable|MergedVariableGroup)
    WHERE NOT gcEdge IN newLinkEdges
    DELETE gcEdge
}

CALL (main) {
    MATCH (main)-[createEdge]->(createNode:Text)
    ORDER BY type(createEdge), createEdge.position
    RETURN collect(
        {
            nodeLabels: labels(createNode),
            nodeProps: properties(createNode),
            edgeLabel: type(createEdge),
            edgeProps: properties(createEdge)
        }
    ) AS createRels
}

CALL (main) {
    MATCH (main)-[linkEdge]->(linkNode:MergedPrimarySource|MergedResource|MergedVariable|MergedVariableGroup)
    WHERE type(linkEdge) <> "stableTargetId"
    ORDER BY type(linkEdge), linkEdge.position
    RETURN collect(
        {
            nodeLabels: labels(linkNode),
            nodeProps: properties(linkNode),
            edgeLabel: type(linkEdge),
            edgeProps: properties(linkEdge)
        }
    ) AS linkRels
}

RETURN
    main.identifier AS identifier,
    stableTargetId,
    head(labels(main)) AS entityType,
    linkRels,
    createRels,
    properties(main) AS nodeProps;"""
    )
