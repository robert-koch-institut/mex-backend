MERGE (merged:<<merged_label>> {identifier: $stable_target_id})
MERGE (rule:<<rule_label>>)-[stableTargetId:stableTargetId {position: 0}]->(merged)
ON CREATE SET rule = $on_create
ON MATCH SET rule += $on_match
<%- for edge_label in nested_edge_labels -%>
<%- set index = loop.index0 %>
MERGE (rule)-[edge_<<index>>:<<edge_label>> {position: $nested_positions[<<index>>]}]->(value_<<index>>:<<nested_node_labels[index]>>)
ON CREATE SET value_<<index>> = $nested_values[<<index>>]
ON MATCH SET value_<<index>> += $nested_values[<<index>>]
<%- endfor %>
WITH rule,
    [] as edges,
    [] as values
CALL {
    WITH rule, values
    RETURN count([]) as pruned
}
RETURN rule, edges, values, pruned;
