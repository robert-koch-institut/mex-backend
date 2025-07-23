<# Copy all inbound edges from an old merged item to a new one.

This query copies all edges that point to a merged node with the old identifier to also
point to the merged node with the new identifier.
The copying process creates new edges with the same labels and properties,
but pointing to the new target node.

Jinja:
    merged_labels: List of all merged class labels

Args:
    old_target_identifier: Identifier of the old target node
    new_target_identifier: Identifier of the new target node

Returns:
    copied_edges_count: Number of edges that were copied
-#>
MATCH (old_target:<<merged_labels|join("|")>> {identifier: $old_target_identifier})
MATCH (new_target:<<merged_labels|join("|")>> {identifier: $new_target_identifier})
MATCH (source)-[old_edge]->(old_target)
WHERE type(old_edge) <> "stableTargetId"
WITH
    source,
    old_edge,
    new_target,
    type(old_edge) AS edge_type,
    properties(old_edge) AS edge_props
MERGE (source)-[new_edge:$(edge_type)]->(new_target)
ON CREATE SET new_edge = edge_props
RETURN count(new_edge) AS copied_edges_count;
