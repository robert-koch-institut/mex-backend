CALL {
<% block match_clause %>
<% if query %>
    CALL db.index.fulltext.queryNodes("search_index", $query)
    YIELD node AS hit, score
<% endif %>
<% if stable_target_id %>
    MATCH (n:<<extracted_labels|join("|")>>)-[:stableTargetId]->(m:<<merged_labels|join("|")>>)
<% else %>
    MATCH (n:<<extracted_labels|join("|")>>)
<% endif %>
<% if query or stable_target_id or labels -%>
<%- set and_ = joiner("AND") %>
    WHERE
    <% if query %>
        <<and_()>> elementId(hit) = elementId(n)
    <% endif %>
    <% if stable_target_id %>
        <<and_()>> m.identifier = $stable_target_id
    <% endif %>
    <% if labels %>
        <<and_()>> ANY(label IN labels(n) WHERE label IN $labels)
    <% endif %>
<% endif %>
<% endblock %>
    RETURN COUNT(n) AS total
}
CALL {
    <<self.match_clause()>>
    CALL {
        WITH n
        MATCH (n:<<extracted_labels|join("|")>>)-[r]->(t:<<merged_labels|join("|")>>)
        RETURN type(r) as label, r.position as position, t.identifier as value
    UNION
        WITH n
        MATCH (n:<<extracted_labels|join("|")>>)-[r]->(t:<<nested_labels|join("|")>>)
        RETURN type(r) as label, r.position as position, properties(t) as value
    }
    WITH n, collect({label: label, position: position, value: value}) as refs
    RETURN n{.*, entityType: head(labels(n)), _refs: refs}
    ORDER BY n.identifier ASC
    SKIP $skip
    LIMIT $limit
}
RETURN collect(n) AS items, total;
