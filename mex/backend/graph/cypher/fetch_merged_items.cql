<# Fetch merged items including their nested objects and referenced identifiers.

Globals:
    extracted_labels: List of all extracted class labels
    rule_labels: List of all rule class labels
    merged_labels: List of all merged class labels
    nested_labels: List of labels for all nestable objects

Args:
    filter_by_query_string: Whether the final query should accept a full text query string
    filter_by_identifier: Whether the final query should filter by the merged identifier

Returns:
    total: Count of all items found by this query
    items: List of merged items, each item has the following attributes:
        entityType: the type of the merged item
        identifier: the identifier of the merged item
        components: The rule and / or extracted items with the stableTargetId of this
            merged item. Each component has an extra attribute `_refs` that contains the
            values of nested objects as well as the identifiers of referenced items.
-#>
CALL () {
<%- block match_clause -%>
<%- if filter_by_query_string %>
    OPTIONAL CALL db.index.fulltext.queryNodes("search_index", $query_string)
    YIELD node AS hit, score
<%- endif %>
    OPTIONAL MATCH (extracted_or_rule_node:<<extracted_or_rule_labels|join("|")>>)-[:stableTargetId]->(merged_node:<<merged_labels|join("|")>>)
<%- set and_ = joiner("AND ") %>
    WHERE
    <%- if filter_by_query_string %>
        <<and_()>>elementId(hit) = elementId(extracted_or_rule_node)
    <%- endif %>
    <%- if filter_by_identifier %>
        <<and_()>>merged_node.identifier = $identifier
    <%- endif %>
        <<and_()>>ANY(label IN labels(merged_node) WHERE label IN $labels)
    WITH DISTINCT merged_node AS merged_node
<%- endblock %>
    RETURN COUNT(merged_node) AS total
}
CALL () {
    <<-self.match_clause()>>
    OPTIONAL MATCH (extracted_or_rule_node)-[:stableTargetId]->(merged_node)
    WITH extracted_or_rule_node, merged_node
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
    WITH merged_node, extracted_or_rule_node, collect(ref) AS refs
    ORDER BY merged_node.identifier, extracted_or_rule_node.identifier, head(labels(extracted_or_rule_node)) ASC
    WITH merged_node, collect(extracted_or_rule_node{.*, entityType: head(labels(extracted_or_rule_node)), _refs: refs}) AS extracted_or_rule_node
    RETURN merged_node{entityType: head(labels(merged_node)), identifier: merged_node.identifier, components: extracted_or_rule_node}
    SKIP $skip
    LIMIT $limit
}
RETURN collect(merged_node) AS items, total;
