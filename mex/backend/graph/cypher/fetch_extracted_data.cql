<# Fetches extracted items, including their nested objects and referenced identifiers.

Globals:
    extracted_labels: List of all extracted class labels
    merged_labels: List of all merged class labels
    nested_labels: List of labels for all nestable objects

Args:
    filter_by_query_string: Whether the final query should accept a full text query string
    filter_by_stable_target_id: Whether the final query should filter by stableTargetId
    filter_by_labels: Whether the final query should filter by entity type labels

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
<%- if filter_by_stable_target_id %>
    MATCH (n:<<extracted_labels|join("|")>>)-[:stableTargetId]->(merged:<<merged_labels|join("|")>>)
<%- else %>
    MATCH (n:<<extracted_labels|join("|")>>)
<%- endif %>
<%- if filter_by_query_string or filter_by_stable_target_id or filter_by_labels -%>
<%- set and_ = joiner("AND ") %>
    WHERE
    <%- if filter_by_query_string %>
        <<and_()>>elementId(hit) = elementId(n)
    <%- endif %>
    <%- if filter_by_stable_target_id %>
        <<and_()>>merged.identifier = $stable_target_id
    <%- endif %>
    <%- if filter_by_labels %>
        <<and_()>>ANY(label IN labels(n) WHERE label IN $labels)
    <%- endif %>
<%- endif %>
<%- endblock %>
    RETURN COUNT(n) AS total
}
CALL {
    <<-self.match_clause()>>
    CALL {
        WITH n
        MATCH (n)-[r]->(merged:<<merged_labels|join("|")>>)
        RETURN type(r) as label, r.position as position, merged.identifier as value
    UNION
        WITH n
        MATCH (n)-[r]->(nested:<<nested_labels|join("|")>>)
        RETURN type(r) as label, r.position as position, properties(nested) as value
    }
    WITH n, collect({label: label, position: position, value: value}) as refs
    RETURN n{.*, entityType: head(labels(n)), _refs: refs}
    ORDER BY n.identifier ASC
    SKIP $skip
    LIMIT $limit
}
RETURN collect(n) AS items, total;
