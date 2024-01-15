NOOP_STATEMENT = r"""
RETURN 1;
"""

CREATE_CONSTRAINTS_STATEMENT = r"""
CREATE CONSTRAINT IF NOT EXISTS
FOR (n:{node_label})
REQUIRE n.identifier IS UNIQUE;
"""

CREATE_INDEX_STATEMENT = r"""
CREATE FULLTEXT INDEX text_fields IF NOT EXISTS
FOR (n:{node_labels})
ON EACH [{node_fields}]
OPTIONS {{indexConfig: $config}};
"""

MERGE_NODE_STATEMENT = r"""
MERGE (n:{node_label} {{identifier:$identifier}})
ON CREATE SET n = $on_create
ON MATCH SET n += $on_match
RETURN n;
"""

MERGE_EDGE_STATEMENT = r"""
MATCH (s {{identifier:$fromID}})
MATCH (t {{stableTargetId:$toSTI}})
MERGE (s)-[e:{edge_label}]->(t)
RETURN e;
"""

STABLE_TARGET_ID_IDENTITY_QUERY = r"""
MATCH (n)-[:hadPrimarySource]->(p:ExtractedPrimarySource)
WHERE n.stableTargetId = $stable_target_id
RETURN {
  stableTargetId: n.stableTargetId,
  hadPrimarySource: p.stableTargetId,
  identifierInPrimarySource: n.identifierInPrimarySource,
  identifier: n.identifier
} as i
ORDER BY n.identifier ASC
LIMIT $limit;
"""

HAD_PRIMARY_SOURCE_AND_IDENTIFIER_IN_PRIMARY_SOURCE_IDENTITY_QUERY = r"""
MATCH (n)-[:hadPrimarySource]->(p:ExtractedPrimarySource)
WHERE n.identifierInPrimarySource = $identifier_in_primary_source
  AND p.stableTargetId = $had_primary_source
RETURN {
  stableTargetId: n.stableTargetId,
  hadPrimarySource: p.stableTargetId,
  identifierInPrimarySource: n.identifierInPrimarySource,
  identifier: n.identifier
} as i
ORDER BY n.identifier ASC
LIMIT $limit;
"""

FULL_TEXT_ID_AND_LABEL_FILTER_SEARCH_QUERY = r"""
CALL db.index.fulltext.queryNodes('text_fields', $query)
YIELD node AS hit, score
MATCH (n)
WHERE elementId(hit) = elementId(n)
  AND n.stableTargetId = $stable_target_id
  AND ANY(label IN labels(n) WHERE label IN $labels)
CALL {
  WITH n
  MATCH (n)-[r]->()
  RETURN collect({key: type(r), value: endNode(r).stableTargetId}) as r
}
RETURN n, head(labels(n)) AS l, r
ORDER BY score DESC
SKIP $skip
LIMIT $limit;
"""

FULL_TEXT_ID_AND_LABEL_FILTER_COUNT_QUERY = r"""
CALL db.index.fulltext.queryNodes('text_fields', $query)
YIELD node AS hit, score
MATCH (n)
WHERE elementId(hit) = elementId(n)
  AND n.stableTargetId = $stable_target_id
  AND ANY(label IN labels(n) WHERE label IN $labels)
RETURN COUNT(n) AS c;
"""

FULL_TEXT_ID_FILTER_SEARCH_QUERY = r"""
CALL db.index.fulltext.queryNodes('text_fields', $query)
YIELD node AS hit, score
MATCH (n)
WHERE elementId(hit) = elementId(n)
  AND n.stableTargetId = $stable_target_id
CALL {
  WITH n
  MATCH (n)-[r]->()
  RETURN collect({key: type(r), value: endNode(r).stableTargetId}) as r
}
RETURN n, head(labels(n)) AS l, r
ORDER BY score DESC
SKIP $skip
LIMIT $limit;
"""

FULL_TEXT_ID_FILTER_COUNT_QUERY = r"""
CALL db.index.fulltext.queryNodes('text_fields', $query)
YIELD node AS hit, score
MATCH (n)
WHERE elementId(hit) = elementId(n)
  AND n.stableTargetId = $stable_target_id
RETURN COUNT(n) AS c;
"""

FULL_TEXT_LABEL_FILTER_SEARCH_QUERY = r"""
CALL db.index.fulltext.queryNodes('text_fields', $query)
YIELD node AS hit, score
MATCH (n)
WHERE elementId(hit) = elementId(n)
  AND ANY(label IN labels(n) WHERE label IN $labels)
CALL {
  WITH n
  MATCH (n)-[r]->()
  RETURN collect({key: type(r), value: endNode(r).stableTargetId}) as r
}
RETURN n, head(labels(n)) AS l, r
ORDER BY score DESC
SKIP $skip
LIMIT $limit;
"""

FULL_TEXT_LABEL_FILTER_COUNT_QUERY = r"""
CALL db.index.fulltext.queryNodes('text_fields', $query)
YIELD node AS hit, score
MATCH (n)
WHERE elementId(hit) = elementId(n)
  AND ANY(label IN labels(n) WHERE label IN $labels)
RETURN COUNT(n) AS c;
"""

FULL_TEXT_SEARCH_QUERY = r"""
CALL db.index.fulltext.queryNodes('text_fields', $query)
YIELD node AS hit, score
MATCH (n)
WHERE elementId(hit) = elementId(n)
CALL {
  WITH n
  MATCH (n)-[r]->()
  RETURN collect({key: type(r), value: endNode(r).stableTargetId}) as r
}
RETURN n, head(labels(n)) AS l, r
ORDER BY score DESC
SKIP $skip
LIMIT $limit;
"""

FULL_TEXT_COUNT_QUERY = r"""
CALL db.index.fulltext.queryNodes('text_fields', $query)
YIELD node AS hit, score
MATCH (n)
WHERE elementId(hit) = elementId(n)
RETURN COUNT(n) AS c;
"""

ID_AND_LABEL_FILTER_SEARCH_QUERY = r"""
MATCH (n)
WHERE n.stableTargetId = $stable_target_id
  AND ANY(label IN labels(n) WHERE label IN $labels)
  CALL {
  WITH n
  MATCH (n)-[r]->()
  RETURN collect({key: type(r), value: endNode(r).stableTargetId}) as r
}
RETURN n, head(labels(n)) AS l, r
ORDER BY n.identifier ASC
SKIP $skip
LIMIT $limit;
"""

ID_AND_LABEL_FILTER_COUNT_QUERY = r"""
MATCH (n)
WHERE n.stableTargetId = $stable_target_id
  AND ANY(label IN labels(n) WHERE label IN $labels)
RETURN COUNT(n) AS c;
"""

ID_FILTER_SEARCH_QUERY = r"""
MATCH (n)
WHERE n.stableTargetId = $stable_target_id
CALL {
  WITH n
  MATCH (n)-[r]->()
  RETURN collect({key: type(r), value: endNode(r).stableTargetId}) as r
}
RETURN n, head(labels(n)) AS l, r
ORDER BY n.identifier ASC
SKIP $skip
LIMIT $limit;
"""

ID_FILTER_COUNT_QUERY = r"""
MATCH (n)
WHERE n.stableTargetId = $stable_target_id
RETURN COUNT(n) AS c;
"""

LABEL_FILTER_SEARCH_QUERY = r"""
MATCH (n)
WHERE ANY(label IN labels(n) WHERE label IN $labels)
CALL {
  WITH n
  MATCH (n)-[r]->()
  RETURN collect({key: type(r), value: endNode(r).stableTargetId}) as r
}
RETURN n, head(labels(n)) AS l, r
ORDER BY n.identifier ASC
SKIP $skip
LIMIT $limit;
"""

LABEL_FILTER_COUNT_QUERY = r"""
MATCH (n)
WHERE ANY(label IN labels(n) WHERE label IN $labels)
RETURN COUNT(n) AS c;
"""

GENERAL_SEARCH_QUERY = r"""
MATCH (n)
CALL {
  WITH n MATCH (n)-[r]->()
  RETURN collect({key: type(r), value: endNode(r).stableTargetId}) as r
}
RETURN n, head(labels(n)) AS l, r
ORDER BY n.identifier ASC
SKIP $skip
LIMIT $limit;
"""

GENERAL_COUNT_QUERY = r"""
MATCH (n)
RETURN COUNT(n) AS c;
"""

QUERY_MAP = {
    # (full_text, id_filter, label_filter) => search_query, count_query
    (True, True, True): (
        FULL_TEXT_ID_AND_LABEL_FILTER_SEARCH_QUERY,
        FULL_TEXT_ID_AND_LABEL_FILTER_COUNT_QUERY,
    ),
    (True, True, False): (
        FULL_TEXT_ID_FILTER_SEARCH_QUERY,
        FULL_TEXT_ID_FILTER_COUNT_QUERY,
    ),
    (True, False, True): (
        FULL_TEXT_LABEL_FILTER_SEARCH_QUERY,
        FULL_TEXT_LABEL_FILTER_COUNT_QUERY,
    ),
    (True, False, False): (
        FULL_TEXT_SEARCH_QUERY,
        FULL_TEXT_COUNT_QUERY,
    ),
    (False, True, True): (
        ID_AND_LABEL_FILTER_SEARCH_QUERY,
        ID_AND_LABEL_FILTER_COUNT_QUERY,
    ),
    (False, True, False): (
        ID_FILTER_SEARCH_QUERY,
        ID_FILTER_COUNT_QUERY,
    ),
    (False, False, True): (
        LABEL_FILTER_SEARCH_QUERY,
        LABEL_FILTER_COUNT_QUERY,
    ),
    (False, False, False): (
        GENERAL_SEARCH_QUERY,
        GENERAL_COUNT_QUERY,
    ),
}
