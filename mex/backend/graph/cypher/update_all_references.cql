<# Update all references from old identifier to new identifier.

This query rewires all edges that point to a node with the old identifier to instead
point to the node with the new identifier.
The rewiring process deletes the old edges and recreates them with the same
labels and properties, but pointing to the new target node.

Args:
    old_identifier: Identifier of the old target node
    new_identifier: Identifier of the new target node
    merged_labels: List of all merged class labels

Returns:
    updated_edges_count: Number of edges that were rewired
-#>
MATCH (old_target:<<merged_labels|join("|")>> {identifier: $old_identifier})
MATCH (new_target:<<merged_labels|join("|")>> {identifier: $new_identifier})
MATCH (source)-[old_edge]->(old_target)
WITH
    source,
    old_edge,
    new_target,
    type(old_edge) AS edge_type,
    properties(old_edge) AS edge_props
MERGE (source)-[new_edge:$(edge_type)]->(new_target)
ON CREATE SET new_edge = edge_props
DELETE old_edge
RETURN count(new_edge) AS updated_edges_count;
