<# Delete a rule-set (all 3 rules) by stableTargetId.

Deletes all additive, subtractive, and preventive rules connected to the given
stableTargetId, along with their nested items (text/link) and outbound connections.

Globals:
    any_merged_label: Pipe-delimited sequence of all merged class labels
    any_rule_label: Pipe-delimited sequence of all rule class labels
    any_nested_label: Pipe-delimited sequence of labels for all nestable objects

Args:
    stable_target_id: stableTargetId of the merged item whose rules to delete

Returns:
    deleted_rule_count: Number of rule items deleted
    deleted_nested_count: Number of nested items deleted
-#>

MATCH (merged:<<any_merged_label>> {identifier: $stable_target_id})
OPTIONAL MATCH (merged)<-[:stableTargetId]-(rule:<<any_rule_label>>)
OPTIONAL MATCH (rule)-[outbound]->()
OPTIONAL MATCH (rule)-[]->(nested:<<any_nested_label>>)

DETACH DELETE nested
DELETE outbound
DETACH DELETE rule

RETURN
    0 as deleted_merged_count,
    0 as deleted_extracted_count,
    count(DISTINCT rule) AS deleted_rule_count,
    count(DISTINCT nested) AS deleted_nested_count;
