<# Fetch extracted or rule items including their nested objects and referenced identifiers.

Globals:
    extracted_or_rule_labels: List of all extracted or rule labels
    merged_labels: List of all merged class labels
    nested_labels: List of labels for all nestable objects
    rule_labels: List of all rule class labels

Jinja:
    filter_by_query_string: Whether the final query should accept a full text query string
    filter_by_identifier: Whether the final query should filter by identifiers
    filter_by_stable_target_id: Whether the final query should filter by stableTargetIds
    filter_by_referenced_identifiers: Whether to filter for references to items with specific identifiers
    filter_rule_items: Whether to filter for items that are rule items
    reference_field: Name of the relationship holding referenced identifiers to use for filtering

Args:
    query_string: Full text query string
    labels: Labels to filter items by
    identifier: Identifier to filter items by
    stable_target_id: Identifier of the connected merged item to filter by
    referenced_identifiers: Identifiers referenced by `reference_field`
    skip: Skip these many nodes for pagination
    limit: Limit pagination to this many items

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
    CALL (hit) {
        MATCH (hit:<<extracted_or_rule_labels|join("|")>>)-[:stableTargetId]->(merged_node:<<merged_labels|join("|")>>)
        RETURN hit as extracted_or_rule_node, merged_node
    UNION
        MATCH (hit:<<nested_labels|join("|")>>)<-[]-(extracted_or_rule_node:<<extracted_or_rule_labels|join("|")>>)-[:stableTargetId]->(merged_node:<<merged_labels|join("|")>>)
        RETURN extracted_or_rule_node, merged_node
    }
    WITH DISTINCT extracted_or_rule_node, merged_node
<%- else %>
    OPTIONAL MATCH (extracted_or_rule_node:<<extracted_or_rule_labels|join("|")>>)-[:stableTargetId]->(merged_node:<<merged_labels|join("|")>>)
<%- endif %>
    WHERE
        ANY(label IN labels(extracted_or_rule_node) WHERE label IN $labels)
        <%- if filter_by_identifier %>
        AND extracted_or_rule_node.identifier = $identifier
        <%- endif %>
        <%- if filter_by_stable_target_id %>
        AND merged_node.identifier = $stable_target_id
        <%- endif %>
    <%- if filter_rule_items or filter_by_referenced_identifiers %>
    CALL (extracted_or_rule_node) {
        <%- if filter_rule_items %>
        MATCH (extracted_or_rule_node:<<rule_labels|join("|")>>)
        <%- else %>
        MATCH (extracted_or_rule_node)
        WHERE false
        <%- endif %>
        RETURN extracted_or_rule_node AS filtered_node
    UNION
        <%- if filter_by_referenced_identifiers %>
        MATCH (extracted_or_rule_node)-[:<<reference_field>>]->(referenced_node_to_filter_by)
        WHERE
            referenced_node_to_filter_by.identifier IN $referenced_identifiers
        <%- else %>
        MATCH (extracted_or_rule_node)
        WHERE false
        <%- endif %>
        RETURN extracted_or_rule_node AS filtered_node
    }
    WITH filtered_node as extracted_or_rule_node
    <%- endif %>
<%- endblock %>
    RETURN COUNT(extracted_or_rule_node) AS total
}
CALL () {
    <<-self.match_clause()>>
    ORDER BY extracted_or_rule_node.identifier, head(labels(extracted_or_rule_node)) ASC
    SKIP $skip
    LIMIT $limit
    WITH
        extracted_or_rule_node,
        [
            (extracted_or_rule_node)-[r]->(referenced_merged_node:<<merged_labels|join("|")>>) |
            {value: referenced_merged_node.identifier, position:r.position, label: type(r)}
        ] + [
            (extracted_or_rule_node)-[r]->(referenced_nested_node:<<nested_labels|join("|")>>) |
            {value: properties(referenced_nested_node), position:r.position , label: type(r)}
        ] AS refs
    WITH
        collect(
            extracted_or_rule_node{.*, entityType: head(labels(extracted_or_rule_node)), _refs: refs}
        ) AS items
    RETURN items
}
RETURN items, total;
