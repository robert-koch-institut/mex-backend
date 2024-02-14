MATCH (source:<<extracted_label>> {identifier: $identifier})
CALL {
<% if ref_labels %>
    WITH source
<% set union = joiner("UNION") %>
<% for ref_label in ref_labels %>
<% set index = loop.index0 %>
    <<union()>>
    MATCH (target_<<index>> {identifier: $ref_identifiers[<<index>>]})
    MERGE (source)-[edge:<<ref_label>> {position: $ref_positions[<<index>>]}]->(target_<<index>>)
    RETURN edge
<% endfor %>
<% else %>
    RETURN null as edge
<% endif %>
}
WITH source, collect(edge) as edges
CALL {
    WITH source, edges
    MATCH (source)-[gc]->(:<<merged_labels|join("|")>>)
    WHERE NOT gc IN edges
    DELETE gc
    RETURN count(gc) as pruned
}
RETURN count(edges) as merged, pruned, edges
