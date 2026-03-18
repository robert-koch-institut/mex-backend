<# Fetch extracted or rule items including their nested objects and referenced identifiers.

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
    items: List of extracted or rule items
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
    <%- if filter_by_references %>
    <<check_reference_filters("extracted_or_rule_node", "merged_node, extracted_or_rule_node", "merged_node, extracted_or_rule_node")|indent(4)>>
    <%- endif %>
<%- endblock %>
    RETURN COUNT(extracted_or_rule_node) AS total
}
CALL () {
    <<-self.match_clause()>>
    ORDER BY merged_node.identifier, extracted_or_rule_node.identifier, head(labels(extracted_or_rule_node)) ASC
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
