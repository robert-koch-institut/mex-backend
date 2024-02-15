MERGE (merged:<<merged_label>> {identifier: $stable_target_id})
MERGE (extracted:<<extracted_label>> {identifier: $identifier})-[stableTargetId:stableTargetId {position: 0}]->(merged)
ON CREATE SET extracted = $on_create
ON MATCH SET extracted += $on_match
<%- for edge_label in nested_edge_labels -%>
<%- set index = loop.index0 %>
MERGE (extracted)-[edge_<<index>>:<<edge_label>> {position: $nested_positions[<<index>>]}]->(value_<<index>>:<<nested_node_labels[index]>>)
ON CREATE SET value_<<index>> = $nested_values[<<index>>]
ON MATCH SET value_<<index>> += $nested_values[<<index>>]
<%- endfor %>
WITH extracted,
    [<<range(nested_edge_labels|count)|map("ensure_prefix", "edge_")|join(", ")>>] as edges,
    [<<range(nested_edge_labels|count)|map("ensure_prefix", "value_")|join(", ")>>] as values
CALL {
    WITH values
    MATCH (:<<extracted_label>> {identifier: $identifier})-[]->(gc:<<nested_labels|join("|")>>)
    WHERE NOT gc IN values
    DETACH DELETE gc
    RETURN count(gc) as pruned
}
RETURN extracted, edges, values, pruned;
