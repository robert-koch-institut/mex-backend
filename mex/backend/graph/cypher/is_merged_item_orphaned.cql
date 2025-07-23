<# Check if a merged item is orphaned (no extracted items reference it).

This query determines whether a merged item is orphaned by checking if any
extracted items still reference it via the stableTargetId relationship.

Jinja:
    merged_labels: List of all merged class labels
    extracted_labels: List of all extracted class labels

Args:
    merged_identifier: Identifier of the merged item to check

Returns:
    is_orphaned: Boolean indicating if the merged item is orphaned
-#>
MATCH (merged:<<merged_labels|join("|")>> {identifier: $merged_identifier})
OPTIONAL MATCH (merged)<-[:stableTargetId]-(extracted:<<extracted_labels|join("|")>>)
RETURN count(extracted) = 0 AS is_orphaned;
