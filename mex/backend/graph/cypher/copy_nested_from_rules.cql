<# Copy all nested nodes from the rules node to a new rule node.

This query duplicates all nested nodes connected to the old rules and connects
the duplicates to the new rules with the same nested node and edge properties.

Globals:
    rule_labels: List of all rule labels
    merged_labels: List of all merged labels
    nested_labels: List of labels for nested nodes

Args:
    old_merged_identifier: Identifier of the old merged node
    new_merged_identifier: Identifier of the new merged node

Returns:
    copied_nested_count: Number of nested nodes that were copied
-#>
MATCH (old_rule:<<rule_labels|join("|")>>)-[:stableTargetId]->(:<<merged_labels|join("|")>> {identifier: $old_merged_identifier})
MATCH (new_rule:<<rule_labels|join("|")>>)-[:stableTargetId]->(:<<merged_labels|join("|")>> {identifier: $new_merged_identifier})
WHERE labels(old_rule) = labels(new_rule)
MATCH (old_rule)-[old_edge]->(old_nested_node:<<nested_labels|join("|")>>)
WITH
    new_rule,
    type(old_edge) AS edge_type,
    properties(old_edge) AS edge_props,
    labels(old_nested_node) AS nested_labels,
    properties(old_nested_node) AS nested_props
CREATE (new_nested_node:$(nested_labels))
SET new_nested_node = nested_props
CREATE (new_rule)-[new_edge:$(edge_type)]->(new_nested_node)
SET new_edge = edge_props
RETURN count(new_nested_node) AS copied_nested_count;
