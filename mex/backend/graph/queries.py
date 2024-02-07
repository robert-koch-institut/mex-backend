from collections.abc import Callable, Iterable
from functools import wraps
from typing import Any, TypeVar, cast

from mex.common.models import (
    EXTRACTED_MODEL_CLASSES_BY_NAME,
    MERGED_MODEL_CLASSES_BY_NAME,
)
from mex.common.transform import dromedary_to_snake
from mex.common.types import Link, Text

from mex.backend.transform import to_primitive

QueryFactory = TypeVar("QueryFactory", bound=Callable[..., str])


EXTRACTED_LABELS = "|".join(EXTRACTED_MODEL_CLASSES_BY_NAME)
MERGED_LABELS = "|".join(MERGED_MODEL_CLASSES_BY_NAME)
NESTED_LABELS = "|".join([Link.__name__, Text.__name__])


def query_factory(factory: QueryFactory) -> QueryFactory:
    # @cache
    @wraps(factory)
    def wrapper(*args: Any, **kwds: Any) -> str:
        query = factory(*args, **kwds)
        return query.strip()

    return cast(QueryFactory, wrapper)


NOOP_STATEMENT = r"""
RETURN 1;
"""


@query_factory
def noop() -> str:
    """Build a noop query to test the database connection."""
    return NOOP_STATEMENT


CREATE_CONSTRAINTS_STATEMENT = r"""
CREATE CONSTRAINT {lowercase_label}_identifier_uniqueness IF NOT EXISTS
FOR (n:{node_label})
REQUIRE n.identifier IS UNIQUE;
"""


@query_factory
def identifier_uniqueness_constraint(node_label: str) -> str:
    """Build a uniqueness constraint statement for the `identifier` field."""
    return CREATE_CONSTRAINTS_STATEMENT.format(
        lowercase_label=dromedary_to_snake(node_label), node_label=node_label
    )


CREATE_INDEX_STATEMENT = r"""
CREATE FULLTEXT INDEX search_index IF NOT EXISTS
FOR (n:{node_labels})
ON EACH [{node_fields}]
OPTIONS {{indexConfig: $config}};
"""


@query_factory
def full_text_search_index(**fields_by_label: Iterable[str]) -> str:
    """Build a full text search index on the given label and fields."""
    node_labels = "|".join(fields_by_label)
    node_fields = ", ".join(
        sorted({f"n.{f}" for fields in fields_by_label.values() for f in fields})
    )
    return CREATE_INDEX_STATEMENT.format(
        node_labels=node_labels, node_fields=node_fields
    )


MERGE_EDGE_STATEMENT = r"""
MATCH (fromNode:{extracted_labels} {{identifier:$fromIdentifier}})
MATCH (toNode:{merged_labels} {{identifier:$toStableTargetId}})
MERGE (fromNode)-[edge:{edge_label} {{position:$position}}]->(toNode)
RETURN edge;
"""


@query_factory
def merge_edge(label: str) -> str:
    """Build a statement to merge an edge into the graph."""
    return MERGE_EDGE_STATEMENT.format(
        edge_label=label, merged_labels=MERGED_LABELS, extracted_labels=EXTRACTED_LABELS
    )


IDENTITY_QUERY = r"""
MATCH (n:{extracted_labels})-[:stableTargetId]->(m:{merged_labels})
MATCH (n:{extracted_labels})-[:hadPrimarySource]->(p:MergedPrimarySource)
{where_clause}
RETURN
  m.identifier as stableTargetId,
  p.identifier as hadPrimarySource,
  n.identifierInPrimarySource as identifierInPrimarySource,
  n.identifier as identifier
ORDER BY n.identifier ASC
LIMIT $limit;
"""

STABLE_TARGET_ID_IDENTITY_WHERE_CLAUSE = r"""
WHERE m.identifier = $stable_target_id
"""


@query_factory
def stable_target_id_identity_where_clause() -> str:
    return STABLE_TARGET_ID_IDENTITY_WHERE_CLAUSE


@query_factory
def stable_target_id_identity() -> str:
    """Build a query to get all identities for the given `stableTargetId`."""
    return IDENTITY_QUERY.format(
        where_clause=stable_target_id_identity_where_clause(),
        extracted_labels=EXTRACTED_LABELS,
        merged_labels=MERGED_LABELS,
    )


HAD_PRIMARY_SOURCE_AND_IDENTIFIER_IN_PRIMARY_SOURCE_IDENTITY_WHERE_CLAUSE = r"""
WHERE n.identifierInPrimarySource = $identifier_in_primary_source
AND p.identifier = $had_primary_source
"""


@query_factory
def had_primary_source_and_identifier_in_primary_source_identity_where_clause() -> str:
    return HAD_PRIMARY_SOURCE_AND_IDENTIFIER_IN_PRIMARY_SOURCE_IDENTITY_WHERE_CLAUSE


@query_factory
def had_primary_source_and_identifier_in_primary_source_identity() -> str:
    """Build a query to get all identities for the given hps/iips combo."""
    return IDENTITY_QUERY.format(
        where_clause=had_primary_source_and_identifier_in_primary_source_identity_where_clause(),
        extracted_labels=EXTRACTED_LABELS,
        merged_labels=MERGED_LABELS,
    )


FULL_TEXT_MATCH_CLAUSE = r"""
  CALL db.index.fulltext.queryNodes('search_index', $query)
  YIELD node AS hit, score
  MATCH (n:{extracted_labels})
  WHERE elementId(hit) = elementId(n)
  AND
  ANY(label IN labels(n) WHERE label IN $labels)
"""


@query_factory
def full_text_match_clause() -> str:
    return FULL_TEXT_MATCH_CLAUSE.format(extracted_labels=EXTRACTED_LABELS)


STABLE_TARGET_ID_MATCH_CLAUSE = r"""
  MATCH (n:{extracted_labels})-[:stableTargetId]->(m:{merged_labels})
  WHERE m.identifier = $stable_target_id
"""


@query_factory
def stable_target_id_match_clause() -> str:
    return STABLE_TARGET_ID_MATCH_CLAUSE.format(
        extracted_labels=EXTRACTED_LABELS, merged_labels=MERGED_LABELS
    )


ENTITY_TYPE_MATCH_CLAUSE = r"""
  MATCH (n:{extracted_labels})
  WHERE ANY(label IN labels(n) WHERE label IN $labels)
"""


@query_factory
def entity_type_match_clause() -> str:
    return ENTITY_TYPE_MATCH_CLAUSE.format(extracted_labels=EXTRACTED_LABELS)


EXTRACTED_DATA_QUERY = r"""
CALL {{
  {match}
  RETURN COUNT(n) AS total
}}
CALL {{
  {match}
  CALL {{
    WITH n
    MATCH (n:{extracted_labels})-[r]->(t:{merged_labels})
    RETURN type(r) as label, r.position as position, t.identifier as value
  UNION
    WITH n
    MATCH (n:{extracted_labels})-[r]->(t:{nested_labels})
    RETURN type(r) as label, r.position as position, properties(t) as value
  }}
  WITH n, collect({{label: label, position: position, value: value}}) as refs
  RETURN n{{.*, entityType: head(labels(n)), _refs: refs}}
  ORDER BY n.identifier ASC
  SKIP $skip
  LIMIT $limit
}}
RETURN collect(n) AS items, total;
"""


@query_factory
def extracted_data_query(match_clause: str) -> str:
    return EXTRACTED_DATA_QUERY.format(
        match=match_clause,
        extracted_labels=EXTRACTED_LABELS,
        merged_labels=MERGED_LABELS,
        nested_labels=NESTED_LABELS,
    )
