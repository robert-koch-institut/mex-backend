<# Update the stableTargetId relationship for an extracted item.

Globals:
    extracted_labels: List of all extracted item labels
    merged_labels: List of all merged item labels

Args:
    extracted_item_identifier: Identifier of the extracted item to update
    merged_item_identifier: Identifier of the merged item to link to

Returns:
    No explicit return value, but updates the stableTargetId relationship
-#>
MATCH (extracted_node:<<extracted_labels|join("|")>> {identifier: $extracted_item_identifier})-[old_edge:stableTargetId]->(:<<merged_labels|join("|")>>)
MERGE (extracted_node)-[:stableTargetId {position: 0}]->(:<<merged_labels|join("|")>> {identifier: $merged_item_identifier})
DELETE old_edge;
