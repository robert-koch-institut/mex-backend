<# Match extracted or rule nodes and their merged counterparts, optionally via full text search.

When filter_by_query_string is enabled, nodes are found through the full text search
index and then resolved to their extracted/rule and merged nodes. Otherwise, a direct
match is performed.

Globals:
    any_extracted_or_rule_label: List of all extracted and rule class labels
    any_merged_label: List of all merged class labels
    any_nested_label: List of labels for all nestable objects

Jinja:
    filter_by_query_string: Whether to use full text search or direct matching

Args:
    query_string: Full text query string (only when filter_by_query_string is enabled)
-#>
<% macro match_or_search_nodes() -%>
<%- if filter_by_query_string -%>
OPTIONAL CALL db.index.fulltext.queryNodes("search_index", $query_string)
YIELD node AS hit, score
CALL (hit) {
    MATCH (hit:<<any_extracted_or_rule_label>>)-[:stableTargetId]->(merged_node:<<any_merged_label>>)
    RETURN hit as extracted_or_rule_node, merged_node
UNION
    MATCH (hit:<<any_nested_label>>)<-[]-(extracted_or_rule_node:<<any_extracted_or_rule_label>>)-[:stableTargetId]->(merged_node:<<any_merged_label>>)
    RETURN extracted_or_rule_node, merged_node
}
WITH DISTINCT extracted_or_rule_node, merged_node
<%- else -%>
OPTIONAL MATCH (extracted_or_rule_node:<<any_extracted_or_rule_label>>)-[:stableTargetId]->(merged_node:<<any_merged_label>>)
<%- endif %>
<%- endmacro %>
