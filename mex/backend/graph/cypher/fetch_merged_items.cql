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
    items: List of extracted data items, each item has an extra attribute `_refs` that
           contains the values of nested objects as well as the identifiers of
           referenced items
-#>
CALL {
<%- block match_clause -%>
<%- if filter_by_query_string %>
    CALL db.index.fulltext.queryNodes("search_index", $query_string)
    YIELD node AS hit, score
<%- endif %>
    OPTIONAL MATCH (n:<<rule_labels|join("|")>>|<<extracted_labels|join("|")>>)-[:stableTargetId]->(merged:<<merged_labels|join("|")>>)
<%- set and_ = joiner("AND ") %>
    WHERE
    <%- if filter_by_query_string %>
        <<and_()>>elementId(hit) = elementId(n)
    <%- endif %>
    <%- if filter_by_stable_target_id %>
        <<and_()>>merged.identifier = $stable_target_id
    <%- endif %>
        <<and_()>>ANY(label IN labels(merged) WHERE label IN $labels)
    WITH DISTINCT merged as merged
<%- endblock %>
    RETURN COUNT(merged) AS total
}
CALL {
    <<-self.match_clause()>>
    OPTIONAL MATCH (n)-[:stableTargetId]->(merged)
    CALL {
        WITH n
        OPTIONAL MATCH (n)-[r]->(referenced:<<merged_labels|join("|")>>)
        RETURN CASE WHEN referenced IS NOT NULL THEN {
            label: type(r),
            position: r.position,
            value: referenced.identifier
        } ELSE NULL END as ref
    UNION
        WITH n
        OPTIONAL MATCH (n)-[r]->(nested:<<nested_labels|join("|")>>)
        RETURN CASE WHEN nested IS NOT NULL THEN {
            label: type(r),
            position: r.position,
            value: properties(nested)
        } ELSE NULL END as ref
    }
    WITH n, collect(ref) as refs
    RETURN n{.*, entityType: head(labels(n)), _refs: refs}
    ORDER BY n.identifier ASC
}
RETURN merged, collect(n) AS items, total
ORDER BY merged.identifier ASC
SKIP $skip
LIMIT $limit;
