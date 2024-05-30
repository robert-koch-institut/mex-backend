<# Upsert an extracted item with its accociated merged item and all nested objects.

One extracted item is created (or updated) along with its accociated merged item.
In addition, all nested objects of the extracted item are created as their own
nodes and linked to the extracted item via edges that have the field names as labels.
We also prune all connected nodes that have nested labels but are not part of
the extracted item any more.

Globals:
    nested_labels: List of all labels for nested objects

Args:
    merged_label: Label for the merged item accociated with the extracted item,
                  e.g. MergedPerson
    current_label: Label of the current extracted item, e.g. ExtractedPerson
    nested_edge_labels: Ordered list of field names that contain nested objects
                        e.g. homepage, alternativeName, methodDescription
    nested_node_labels: Ordered list of class names for the `nested_edge_labels`,
                        e.g. Link, Text, Text

Returns:
    current: The current extracted item with all inline properties
    edges: List of edges connecting the current item with its nested objects
    values: List of nested objects that are merged by this statement
    pruned: Number of nested objects that were removed by this statement
-#>
MERGE (merged:<<merged_label>> {identifier: $stable_target_id})
MERGE (current:<<current_label>> {identifier: $identifier})-[stableTargetId:stableTargetId {position: 0}]->(merged)
ON CREATE SET current = $on_create
ON MATCH SET current += $on_match
<%- for edge_label in nested_edge_labels -%>
<%- set index = loop.index0 %>
MERGE (current)-[edge_<<index>>:<<edge_label>> {position: $nested_positions[<<index>>]}]->(value_<<index>>:<<nested_node_labels[index]>>)
ON CREATE SET value_<<index>> = $nested_values[<<index>>]
ON MATCH SET value_<<index>> += $nested_values[<<index>>]
<%- endfor %>
WITH current,
    [<<range(nested_edge_labels|count)|map("ensure_prefix", "edge_")|join(", ")>>] as edges,
    [<<range(nested_edge_labels|count)|map("ensure_prefix", "value_")|join(", ")>>] as values
CALL {
    WITH current, values
    MATCH (current)-[]->(outdated_node:<<nested_labels|join("|")>>)
    WHERE NOT outdated_node IN values
    DETACH DELETE outdated_node
    RETURN count(outdated_node) as pruned
}
RETURN current, edges, values, pruned;
