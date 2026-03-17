<# Fetch extracted or rule items including their nested objects and referenced identifiers.

Globals:
    extracted_or_rule_labels: List of all extracted or rule labels
    merged_labels: List of all merged class labels
    nested_labels: List of labels for all nestable objects

Jinja:
    filter_by_query_string: Whether the final query should accept a full text query string
    filter_by_identifier: Whether the final query should filter by identifiers
    filter_by_references: Whether to filter for references to items with specific identifiers
    reference_fields: List of relationship names holding referenced identifiers to use for filtering

Args:
    query_string: Full text query string
    labels: Labels to filter items by
    identifier: Identifier to filter items by
    reference_filters: List of reference filters with field and identifiers
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
    <%- if filter_by_references %>
    OPTIONAL MATCH (extracted_or_rule_node)-[reference:<<reference_fields|join("|")>>]->(ref_target:<<merged_labels|join("|")>>)
    WITH merged_node, extracted_or_rule_node,
        collect(CASE WHEN reference IS NOT NULL THEN type(reference) END) AS found_fields,
        collect(CASE WHEN reference IS NOT NULL
            THEN {field: type(reference), identifier: ref_target.identifier} END
        ) AS existing_refs
    WITH merged_node, extracted_or_rule_node,
        existing_refs +
        [f IN $reference_fields WHERE NOT f IN found_fields | {field: f, identifier: "__NO_REF__"}]
        AS ref_matches
    WHERE ALL(rf IN $reference_filters WHERE
        ANY(m IN ref_matches WHERE m.field = rf.field AND m.identifier IN rf.identifiers)
    )
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
