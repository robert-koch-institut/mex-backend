MATCH (n:<<extracted_labels|join("|")>>)-[:stableTargetId]->(m:<<merged_labels|join("|")>>)
MATCH (n:<<extracted_labels|join("|")>>)-[:hadPrimarySource]->(p:MergedPrimarySource)
<% if had_primary_source or identifier_in_primary_source or stable_target_id -%>
WHERE
    <%- set and_ = joiner("AND ") -%>
    <%- if had_primary_source %>
    <<and_()>>p.identifier = $had_primary_source
    <%- endif %>
    <%- if identifier_in_primary_source %>
    <<and_()>>n.identifierInPrimarySource = $identifier_in_primary_source
    <%- endif -%>
    <%- if stable_target_id %>
    <<and_()>>m.identifier = $stable_target_id
    <%- endif -%>
<%- endif %>
RETURN
    m.identifier as stableTargetId,
    p.identifier as hadPrimarySource,
    n.identifierInPrimarySource as identifierInPrimarySource,
    n.identifier as identifier
ORDER BY n.identifier ASC
LIMIT $limit;
