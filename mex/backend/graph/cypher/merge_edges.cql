<# Merge all edges from a single item to all connected merged items.

The current item can either be an extracted item or a rule.
This statement also prunes all edges originating from the current item that are not
part of the references to be merged during this operation.

Globals:
    merged_labels: List of all merged class labels

Args:
    current_label: Label of the current item that is the source of all edges
                   being merged in this statement
    current_constraints: List of fields that will render as constraints for matching
                         the current item
    ref_labels: Ordered list of merged item labels of the edges being merged

Returns:
    merged: Number of merged edges
    pruned: Number of pruned edges
    edges: List of the merged edge objects
-#>
MATCH (source:<<current_label>> {<<current_constraints|render_constraints>>})-[:stableTargetId]->({identifier: $stable_target_id})
CALL (source) {
<%- if ref_labels %>
<%- set union = joiner("UNION ALL\n    ") %>
<%- for ref_label in ref_labels %>
<%- set index = loop.index0 %>
    <<union()>>WITH source
    MATCH (target_<<index>> {identifier: $ref_identifiers[<<index>>]})
    MERGE (source)-[edge:<<ref_label>> {position: $ref_positions[<<index>>]}]->(target_<<index>>)
    RETURN edge
<%- endfor %>
<%- else %>
    RETURN null AS edge
<%- endif %>
}
WITH source, count(edge) AS merged, collect(edge) AS edges
CALL (source, edges) {
    MATCH (source)-[outdated_edge]->(:<<merged_labels|join("|")>>)
    WHERE NOT outdated_edge IN edges
    DELETE outdated_edge
    RETURN count(outdated_edge) AS pruned
}
RETURN merged, pruned, edges;
