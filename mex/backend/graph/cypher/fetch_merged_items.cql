<# Fetch merged items including their nested objects and referenced identifiers.

Globals:
    any_extracted_or_rule_label: List of all extracted and rule class labels
    any_merged_label: List of all merged class labels
    any_nested_label: List of labels for all nestable objects

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
<% from "macros/match_or_search_nodes.cql" import match_or_search_nodes with context -%>
<% from "macros/check_reference_filters.cql" import check_reference_filters with context -%>
<% from "macros/collect_references_and_nested.cql" import collect_references_and_nested with context -%>

CALL () {
<%- block match_clause -%>
    <<match_or_search_nodes()>>
    WHERE
        ANY(label IN labels(merged_node) WHERE label IN $labels)
        <%- if filter_by_identifier %>
        AND merged_node.identifier = $identifier
        <%- endif %>
        <%- if filter_by_references %>
        AND EXISTS {<<check_reference_filters()>>}
        <%- endif %>
<%- endblock %>
    RETURN COUNT(DISTINCT merged_node) AS total
}
CALL () {
    <<self.match_clause()>>
    WITH DISTINCT merged_node
    ORDER BY merged_node.identifier ASC
    SKIP $skip
    LIMIT $limit
    MATCH (extracted_or_rule_node:<<any_extracted_or_rule_label>>)-[:stableTargetId]->(merged_node)
    WITH
        extracted_or_rule_node,
        merged_node,
        <<collect_references_and_nested()>> AS refs
    ORDER BY merged_node.identifier, extracted_or_rule_node.identifier, head(labels(extracted_or_rule_node)) ASC
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
