<# Delete a merged item along with its associated extracted, rule and nested items.

Globals:
    merged_labels: List of all merged class labels
    rule_labels: List of all rule class labels
    extracted_labels: List of all extracted class labels
    nested_labels: List of labels for all nestable objects

Args:
    identifier: Identifier of the merged item to be deleted

Returns:
    deleted_merged_count: Number of merged items deleted
    deleted_extracted: Number of extracted items deleted
    deleted_rule_count: Number of rule items deleted
    deleted_nested_count: Number of nested items deleted
-#>

MATCH (merged:<<merged_labels|join("|")>> {identifier: $identifier})
OPTIONAL MATCH (merged)<-[:stableTargetId]-(extracted:<<extracted_labels|join("|")>>)
OPTIONAL MATCH (merged)<-[:stableTargetId]-(rule:<<rule_labels|join("|")>>)
OPTIONAL MATCH (merged)-[outbound]->()
OPTIONAL MATCH (extracted)-[]->(extracted_nested:<<nested_labels|join("|")>>)
OPTIONAL MATCH (rule)-[]->(rule_nested:<<nested_labels|join("|")>>)

DETACH DELETE extracted_nested, rule_nested
DETACH DELETE extracted, rule
DELETE outbound
DELETE merged

RETURN
    count(DISTINCT merged) AS deleted_merged_count,
    count(DISTINCT extracted) AS deleted_extracted_count,
    count(DISTINCT rule) AS deleted_rule_count,
    count(DISTINCT extracted_nested) + count(DISTINCT rule_nested) AS deleted_nested_count;
