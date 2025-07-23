<# Copy all outbound edges from old rules to new rules.

This query copies all outbound edges (excluding stableTargetId) from all rule items
connected to one merged node to all rule items of another merged node.
Edge types, edge properties and the edge target node are preserved.

Jinja:
    rule_labels: List of all rule labels
    merged_labels: List of all merged labels
    nested_labels: List of labels for nested nodes

Args:
    old_merged_identifier: Identifier of the old merged node
    new_merged_identifier: Identifier of the new merged node

Returns:
    copied_edges_count: Number of edges that were copied
-#>
MATCH (old_rule:<<rule_labels|join("|")>>)-[:stableTargetId]->(:<<merged_labels|join("|")>> {identifier: $old_merged_identifier})
MATCH (new_rule:<<rule_labels|join("|")>>)-[:stableTargetId]->(:<<merged_labels|join("|")>> {identifier: $new_merged_identifier})
WHERE labels(old_rule) = labels(new_rule)
MATCH (old_rule)-[old_edge]->(target_node:<<merged_labels|join("|")>>)
WHERE type(old_edge) <> "stableTargetId"
WITH
    new_rule,
    type(old_edge) AS edge_type,
    properties(old_edge) AS edge_props
MERGE (new_rule)-[new_edge:$(edge_type)]->(target_node)
ON CREATE SET new_edge = edge_props
RETURN count(new_edge) AS copied_edges_count;
