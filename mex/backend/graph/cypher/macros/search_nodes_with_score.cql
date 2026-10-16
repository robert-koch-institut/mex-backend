<# Resolve full text hits to merged items, keeping the lucene relevance score.

A sibling of `match_or_search_nodes`, which discards the score it yields. Kept
separate so the paginated search paths sharing that macro stay untouched: adding
the score to their `WITH DISTINCT` would change row cardinality from one per
node pair to one per hit. This variant also caps the procedure call and reduces
to one score per merged item, neither of which the other macro can express.

The query string must be escaped by the caller, see
`mex.backend.suggestion.transform.escape_query_string`.

Globals:
    any_extracted_or_rule_label: pipe delimited sequence of extracted and rule labels
    any_merged_label: pipe delimited sequence of all merged class labels
    any_nested_label: pipe delimited sequence of all nestable object labels

Args:
    query_string: Escaped lucene query string
    hit_limit: Cap on raw hits requested from the index

Returns:
    - merged_node: node (label: any_merged_label)
    - score: Highest lucene score of any hit resolving to that merged node
-#>
<% macro search_nodes_with_score() -%>
CALL db.index.fulltext.queryNodes("search_index", $query_string, {limit: $hit_limit})
YIELD node AS hit, score
CALL (hit, score) {
    MATCH (hit:<<any_extracted_or_rule_label>>)-[:stableTargetId]->(merged_node:<<any_merged_label>>)
    RETURN merged_node, score AS hit_score
UNION
    MATCH (hit:<<any_nested_label>>)<-[]-(:<<any_extracted_or_rule_label>>)-[:stableTargetId]->(merged_node:<<any_merged_label>>)
    RETURN merged_node, score AS hit_score
}
WITH merged_node, max(hit_score) AS score
<%- endmacro %>
