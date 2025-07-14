<# Delete orphaned merged items along with their associated rule items and nested items.

This query identifies merged items that are orphaned (no longer referenced by any
extracted items) and deletes them along with:
- All rule items that point to the merged item via stableTargetId
- All nested items connected to those rule items
- The merged item itself

Args:
    merged_labels: List of all merged class labels
    rule_labels: List of all rule class labels
    extracted_labels: List of all extracted class labels
    nested_labels: List of labels for all nestable objects

Returns:
    deleted_merged_count: Number of merged items deleted
    deleted_rule_count: Number of rule items deleted
    deleted_nested_count: Number of nested items deleted
-#>

MATCH (merged:<<merged_labels|join("|")>>)
WHERE NOT (merged)<-[:stableTargetId]-(:<<extracted_labels|join("|")>>)
OPTIONAL MATCH (rule_item:<<rule_labels|join("|")>>)-[:stableTargetId]->(merged)
OPTIONAL MATCH (rule_item)-[]->(nested_item:<<nested_labels|join("|")>>)
WITH count(DISTINCT merged) AS merged_count, count(DISTINCT rule_item) AS rule_count, count(DISTINCT nested_item) AS nested_count
MATCH (merged:<<merged_labels|join("|")>>)
WHERE NOT (merged)<-[:stableTargetId]-(:<<extracted_labels|join("|")>>)
OPTIONAL MATCH (rule_item:<<rule_labels|join("|")>>)-[:stableTargetId]->(merged)
OPTIONAL MATCH (rule_item)-[]->(nested_item:<<nested_labels|join("|")>>)
DETACH DELETE merged, rule_item, nested_item
RETURN merged_count AS deleted_merged_count, rule_count AS deleted_rule_count, nested_count AS deleted_nested_count;
