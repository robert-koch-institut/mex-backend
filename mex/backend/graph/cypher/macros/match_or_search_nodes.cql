<# Match extracted or rule nodes and their merged counterparts, optionally via full text search.

When filter_by_query_string is enabled, nodes are found through the full text search
index and then resolved to their extracted/rule and merged nodes. Otherwise, when
anchor_field is given, the match is anchored on the merged nodes referenced by one of
the reference filters, which reaches the candidates through the identifier index
instead of scanning all extracted and rule nodes. Without either, a direct match is
performed.

Globals:
    any_extracted_or_rule_label: List of all extracted and rule class labels
    any_merged_label: List of all merged class labels
    any_nested_label: List of labels for all nestable objects

Jinja:
    filter_by_query_string: Whether to use full text search or direct matching
    anchor_field: Reference field to anchor the match on, or None to match directly
    merged_labels: Labels to narrow merged_node down to, or None for any merged label

Macro args:
    bind_all_components: Whether to bind extracted_or_rule_node to every component of
        a matching merged item; only callers that read extracted_or_rule_node out of
        this macro need it, and an anchored match binds nothing without it

Args:
    query_string: Full text query string (only when filter_by_query_string is enabled)
    anchor_identifiers: Identifiers of the anchoring merged nodes
        (only when anchor_field is enabled)

Returns:
    - extracted_or_rule_node: node (label: any_extracted_or_rule_label)
      only bound when bind_all_components is enabled or anchor_field is disabled
    - merged_node: node (label: merged_labels or any_merged_label)
      connected via [:stableTargetId] relationship
-#>
<% macro any_merged_or_given_label() -%>
<%- if merged_labels -%><<merged_labels|join("|")>><%- else -%><<any_merged_label>><%- endif -%>
<%- endmacro %>
<% macro match_or_search_nodes(bind_all_components=False) -%>
<%- if filter_by_query_string -%>
OPTIONAL CALL db.index.fulltext.queryNodes("search_index", $query_string)
YIELD node AS hit, score
CALL (hit) {
    MATCH (hit:<<any_extracted_or_rule_label>>)-[:stableTargetId]->(merged_node:<<any_merged_or_given_label()>>)
    RETURN hit as extracted_or_rule_node, merged_node
UNION
    MATCH (hit:<<any_nested_label>>)<-[]-(extracted_or_rule_node:<<any_extracted_or_rule_label>>)-[:stableTargetId]->(merged_node:<<any_merged_or_given_label()>>)
    RETURN extracted_or_rule_node, merged_node
}
WITH DISTINCT extracted_or_rule_node, merged_node
<%- elif anchor_field -%>
MATCH (anchor_node:<<any_merged_label>>)
WHERE anchor_node.identifier IN $anchor_identifiers
MATCH (anchor_node)<-[:<<anchor_field>>]-(anchor_component:<<any_extracted_or_rule_label>>)
<#- the walk back down to the merged node needs its own MATCH: cypher forbids
    re-using a relationship within one clause, so a `stableTargetId` anchor would
    never match if both hops lived in the same pattern #>
MATCH (anchor_component)-[:stableTargetId]->(merged_node:<<any_merged_or_given_label()>>)
WITH DISTINCT merged_node
<%- if bind_all_components %>
MATCH (extracted_or_rule_node:<<any_extracted_or_rule_label>>)-[:stableTargetId]->(merged_node)
<%- endif -%>
<%- else -%>
OPTIONAL MATCH (extracted_or_rule_node:<<any_extracted_or_rule_label>>)-[:stableTargetId]->(merged_node:<<any_merged_or_given_label()>>)
<%- endif %>
<%- endmacro %>
