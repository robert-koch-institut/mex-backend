from collections.abc import Iterable, Mapping

from mex.common.transform import dromedary_to_snake

NOOP_STATEMENT = r"""
RETURN 1;
"""


def noop() -> str:
    """Build a noop query to test the database connection."""
    return NOOP_STATEMENT.strip()


CREATE_CONSTRAINTS_STATEMENT = r"""
CREATE CONSTRAINT {lowercase_label}_identifier_uniqueness IF NOT EXISTS
FOR (node:{node_label})
REQUIRE node.identifier IS UNIQUE;
"""


def identifier_uniqueness_constraint(node_label: str) -> str:
    """Build a uniqueness constraint statement for the `identifier` field."""
    return CREATE_CONSTRAINTS_STATEMENT.format(
        lowercase_label=dromedary_to_snake(node_label), node_label=node_label
    ).strip()


CREATE_INDEX_STATEMENT = r"""
CREATE FULLTEXT INDEX text_fields IF NOT EXISTS
FOR (n:{node_labels})
ON EACH [{node_fields}]
OPTIONS {{indexConfig: $config}};
"""


def full_text_search_index(fields_by_label: Mapping[str, Iterable[str]]) -> str:
    """Build a full text search index on the given label and fields."""
    node_labels = "|".join(fields_by_label)
    node_fields = ", ".join(
        sorted({f"n.{f}" for fields in fields_by_label.values() for f in fields})
    )
    return CREATE_INDEX_STATEMENT.format(
        node_labels=node_labels, node_fields=node_fields
    ).strip()


MERGE_EDGE_STATEMENT = r"""
MATCH (fromNode {{identifier:$fromIdentifier}})
MATCH (toNode {{stableTargetId:$toStableTargetId}})
MERGE (fromNode)-[edge:{edge_label} {{position:$position}}]->(toNode)
RETURN edge;
"""


def merge_edge(label: str) -> str:
    """Build a statement to merge an edge into the graph."""
    return MERGE_EDGE_STATEMENT.format(edge_label=label).strip()


IDENTITY_QUERY = r"""
MATCH (n)-[:hadPrimarySource]->(p:ExtractedPrimarySource)
{where_clause}
RETURN
  n.stableTargetId as stableTargetId,
  p.stableTargetId as hadPrimarySource,
  n.identifierInPrimarySource as identifierInPrimarySource,
  n.identifier as identifier
ORDER BY n.identifier ASC
LIMIT $limit;
"""

STABLE_TARGET_ID_IDENTITY_WHERE_CLAUSE = r"""
WHERE n.stableTargetId = $stable_target_id
"""


def stable_target_id_identity() -> str:
    """Build a query to get all identities for the given `stableTargetId`."""
    clause = STABLE_TARGET_ID_IDENTITY_WHERE_CLAUSE
    return IDENTITY_QUERY.format(where_clause=clause.strip()).strip()


HAD_PRIMARY_SOURCE_AND_IDENTIFIER_IN_PRIMARY_SOURCE_IDENTITY_WHERE_CLAUSE = r"""
WHERE n.identifierInPrimarySource = $identifier_in_primary_source
AND p.stableTargetId = $had_primary_source
"""


def had_primary_source_and_identifier_in_primary_source_identity() -> str:
    """Build a query to get all identities for the given hps/iips combo."""
    clause = HAD_PRIMARY_SOURCE_AND_IDENTIFIER_IN_PRIMARY_SOURCE_IDENTITY_WHERE_CLAUSE
    return IDENTITY_QUERY.format(where_clause=clause.strip()).strip()
