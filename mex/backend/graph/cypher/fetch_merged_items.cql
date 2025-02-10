<# Fetch merged items including their nested objects and referenced identifiers.

Globals:
    extracted_labels: List of all extracted class labels
    extracted_or_rule_labels: List of all extracted and rule class labels
    merged_labels: List of all merged class labels
    nested_labels: List of labels for all nestable objects

Args:
    filter_by_query_string: Whether the final query should accept a full text query string
    filter_by_identifier: Whether the final query should filter by the merged identifier
    filter_by_reference_to_merged_item: Whether to filter for merged items that have a relation to a specific merged item
    reference_field_name: name of the relationship to use for filtering by reference

Returns:
    total: Count of all items found by this query
    items: List of merged items, each item has the following attributes:
        entityType: the type of the merged item
        identifier: the identifier of the merged item
        _components: The rule and / or extracted items with the stableTargetId of this
            merged item. Each component has an extra attribute `_refs` that contains the
            values of nested objects as well as the identifiers of referenced items.
-#>
CALL () {
<%- block match_clause -%>
<%- if filter_by_reference_to_merged_item %>
    OPTIONAL MATCH (merged_node:<<merged_labels|join("|")>>)<-[:stableTargetId]-(:<<extracted_labels|join("|")>>)-[:<<reference_field_name>>]->(referenced_merged_node_to_filter_by)
    WHERE
         referenced_merged_node_to_filter_by.identifier = $referenced_identifier
<%- endif %>
<%- if filter_by_query_string %>
    OPTIONAL CALL db.index.fulltext.queryNodes("search_index", $query_string)
    YIELD node AS hit, score
    CALL (hit) {
        MATCH (extracted_or_rule_node:<<extracted_or_rule_labels|join("|")>>)-[:stableTargetId]->(merged_node:<<merged_labels|join("|")>>)
        WHERE
            elementId(hit) = elementId(extracted_or_rule_node)
            AND ANY(label IN labels(merged_node) WHERE label IN $labels)
            <%- if filter_by_identifier %>
            AND merged_node.identifier = $identifier
            <%- endif %>
        RETURN merged_node
    UNION
        MATCH (nested_node:<<nested_labels|join("|")>>)<-[]-(:<<extracted_or_rule_labels|join("|")>>)-[:stableTargetId]->(merged_node:<<merged_labels|join("|")>>)
        WHERE
            elementId(hit) = elementId(nested_node)
            AND ANY(label IN labels(merged_node) WHERE label IN $labels)
            <%- if filter_by_identifier %>
            AND merged_node.identifier = $identifier
            <%- endif %>
        RETURN merged_node
    }
    WITH DISTINCT merged_node AS merged_node
<%- else %>
    OPTIONAL MATCH (merged_node:<<merged_labels|join("|")>>)
    WHERE
        ANY(label IN labels(merged_node) WHERE label IN $labels)
        <%- if filter_by_identifier %>
        AND merged_node.identifier = $identifier
        <%- endif %>
<%- endif %>
<%- endblock %>
    RETURN COUNT(merged_node) AS total
}
CALL () {
    <<-self.match_clause()>>
    ORDER BY merged_node.identifier, head(labels(merged_node)) ASC
    SKIP $skip
    LIMIT $limit
    OPTIONAL MATCH (extracted_or_rule_node)-[:stableTargetId]->(merged_node)
    ORDER BY extracted_or_rule_node.identifier, head(labels(extracted_or_rule_node)) ASC
    WITH
        extracted_or_rule_node,
        merged_node,
        [
            (extracted_or_rule_node)-[r]->(referenced_merged_node:<<merged_labels|join("|")>>) |
            {value: referenced_merged_node.identifier, position:r.position, label: type(r)}
        ] + [
            (extracted_or_rule_node)-[r]->(referenced_nested_node:<<nested_labels|join("|")>>) |
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
RETURN items, total;
