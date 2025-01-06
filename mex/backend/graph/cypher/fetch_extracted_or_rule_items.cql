<# Fetch extracted or rule items including their nested objects and referenced identifiers.

Globals:
    extracted_labels: List of all extracted class labels
    rule_labels: List of all rule class labels
    merged_labels: List of all merged class labels
    nested_labels: List of labels for all nestable objects

Args:
    filter_by_query_string: Whether the final query should accept a full text query string
    filter_by_stable_target_id: Whether the final query should filter by stableTargetId

Returns:
    total: Count of all items found by this query
    items: List of extracted items, each item has an extra attribute `_refs` that
           contains the values of nested objects as well as the identifiers of
           referenced items
-#>
CALL () {
<%- block match_clause -%>
<%- if filter_by_query_string %>
    OPTIONAL CALL db.index.fulltext.queryNodes("search_index", $query_string)
    YIELD node AS hit, score
<%- endif %>
    OPTIONAL MATCH (extracted_or_rule_node:<<extracted_or_rule_labels|join("|")>>)
<%- if filter_by_stable_target_id -%>
    -[:stableTargetId]->(merged_node:<<merged_labels|join("|")>>)
<%- endif %>
<%- set and_ = joiner("AND ") %>
    WHERE
    <%- if filter_by_query_string %>
        <<and_()>>elementId(hit) = elementId(extracted_or_rule_node)
    <%- endif %>
    <%- if filter_by_stable_target_id %>
        <<and_()>>merged_node.identifier = $stable_target_id
    <%- endif %>
        <<and_()>>ANY(label IN labels(extracted_or_rule_node) WHERE label IN $labels)
<%- endblock %>
    RETURN COUNT(extracted_or_rule_node) AS total
}
CALL () {
    <<-self.match_clause()>>
    WITH extracted_or_rule_node
    CALL (extracted_or_rule_node) {
        OPTIONAL MATCH (extracted_or_rule_node)-[r]->(referenced_merged_node:<<merged_labels|join("|")>>)
        RETURN CASE WHEN referenced_merged_node IS NOT NULL THEN {
            label: type(r),
            position: r.position,
            value: referenced_merged_node.identifier
        } ELSE NULL END AS ref
    UNION
        OPTIONAL MATCH (extracted_or_rule_node)-[r]->(referenced_nested_node:<<nested_labels|join("|")>>)
        RETURN CASE WHEN referenced_nested_node IS NOT NULL THEN {
            label: type(r),
            position: r.position,
            value: properties(referenced_nested_node)
        } ELSE NULL END AS ref
    }
    WITH extracted_or_rule_node, collect(ref) AS refs
    RETURN extracted_or_rule_node{.*, entityType: head(labels(extracted_or_rule_node)), _refs: refs}
    ORDER BY extracted_or_rule_node.identifier, extracted_or_rule_node.entityType ASC
    SKIP $skip
    LIMIT $limit
}
RETURN collect(extracted_or_rule_node) AS items, total;
