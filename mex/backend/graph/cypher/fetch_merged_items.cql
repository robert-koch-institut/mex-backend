<# Fetch merged items including their nested objects and referenced identifiers.

Globals:
    extracted_or_rule_labels: List of all extracted and rule class labels
    merged_labels: List of all merged class labels
    nested_labels: List of labels for all nestable objects

Jinja:
    filter_by_query_string: Whether the final query should accept a full text query string
    filter_by_identifier: Whether the final query should filter by item identifiers
    filter_by_references: Whether to filter for references to items with specific identifiers
    reference_fields: List of relationship names holding referenced identifiers to use for filtering

Args:
    query_string: Full text query string
    labels: Labels to filter items by
    identifier: Identifier to filter items by
    reference_filters: List of reference filters
    skip: Skip these many nodes for pagination
    limit: Limit pagination to this many items

Returns:
    total: Count of all items found by this query
    items: List of merged items, each item has the following attributes:
        entityType: the type of the merged item
        identifier: the identifier of the merged item
        _components: List of extracted or rule items
            entityType: Label of the node
            _refs: Values of nested objects as well as referenced identifiers
            *nodeProps: All other properties of the node
-#>
<% from "macros/check_reference_filters.cql" import check_reference_filters with context -%>

CALL () {
<%- block match_clause -%>
<%- if filter_by_query_string %>
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
<%- else %>
    OPTIONAL MATCH (merged_node:<<any_merged_label>>)
<%- endif %>
    WHERE
        ANY(label IN labels(merged_node) WHERE label IN $labels)
        <%- if filter_by_identifier %>
        AND merged_node.identifier = $identifier
        <%- endif %>
        <%- if filter_by_references %>
        AND EXISTS {
            MATCH (merged_node)<-[:stableTargetId]-(component:<<any_extracted_or_rule_label>>)
            <<check_reference_filters("component", "component")|indent(12)>>
        }
        <%- endif %>
<%- endblock %>
    RETURN COUNT(merged_node) AS total
}
CALL () {
    <<-self.match_clause()>>
    ORDER BY merged_node.identifier ASC
    SKIP $skip
    LIMIT $limit
    OPTIONAL MATCH (extracted_or_rule_node)-[:stableTargetId]->(merged_node)
    ORDER BY merged_node.identifier, extracted_or_rule_node.identifier, head(labels(extracted_or_rule_node)) ASC
    WITH
        extracted_or_rule_node,
        merged_node,
        [
            (extracted_or_rule_node)-[r]->(referenced_merged_node:<<any_merged_label>>) |
            {value: referenced_merged_node.identifier, position:r.position, label: type(r)}
        ] + [
            (extracted_or_rule_node)-[r]->(referenced_nested_node:<<any_nested_label>>) |
            {value: properties(referenced_nested_node), position:r.position , label: type(r)}
        ] AS refs
    WITH
        merged_node,
        collect(
            DISTINCT extracted_or_rule_node{.*, entityType: head(labels(extracted_or_rule_node)), _refs: refs}
        ) AS components
    WITH
        collect(
            merged_node{.*, entityType: head(labels(merged_node)), _components: components}
        ) AS items
    RETURN items
}
RETURN items, total;
