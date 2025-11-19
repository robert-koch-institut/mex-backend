<# Delete a rule-set (all 3 rules) by stableTargetId.

Deletes all additive, subtractive, and preventive rules connected to the given
stableTargetId, along with their nested items (text/link) and outbound connections.

Globals:
    merged_labels: List of all merged class labels
    rule_labels: List of all rule class labels
    nested_labels: List of labels for all nestable objects

Args:
    stable_target_id: stableTargetId of the merged item whose rules to delete

Returns:
    deleted_rule_count: Number of rule items deleted
    deleted_nested_count: Number of nested items deleted
-#>

MATCH (merged:<<merged_labels|join("|")>> {identifier: $stable_target_id})
OPTIONAL MATCH (merged)<-[:stableTargetId]-(rule:<<rule_labels|join("|")>>)
OPTIONAL MATCH (rule)-[outbound]->()
OPTIONAL MATCH (rule)-[]->(nested:<<nested_labels|join("|")>>)

DETACH DELETE nested
DELETE outbound
DETACH DELETE rule

RETURN
    merged,
    count(DISTINCT rule) AS deleted_rule_count,
    count(DISTINCT nested) AS deleted_nested_count;
