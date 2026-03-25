<# Delete a merged item along with its associated extracted, rule and nested items.

Incoming edges from the according extracted item and rules are detached (= deleted).
Outgoing edges are deleted separately. If there are still incoming edges (= item is
referenced by other items), Neo4j will throw an error when trying to delete the merged
item. Therefore referenced merged item can not be deleted by this endpoint.

Globals:
    any_extracted_label: Pipe-delimited sequence of all extracted class labels
    any_merged_label: Pipe-delimited sequence of all merged class labels
    any_rule_label: Pipe-delimited sequence of all rule class labels
    any_nested_label: Pipe-delimited sequence of labels for all nestable objects

Args:
    identifier: Identifier of the merged item to be deleted

Returns:
    deleted_merged_count: Number of merged items deleted
    deleted_extracted: Number of extracted items deleted
    deleted_rule_count: Number of rule items deleted
    deleted_nested_count: Number of nested items deleted
-#>

MATCH (merged:<<any_merged_label>> {identifier: $identifier})
OPTIONAL MATCH (merged)<-[:stableTargetId]-(extracted:<<any_extracted_label>>)
OPTIONAL MATCH (merged)<-[:stableTargetId]-(rule:<<any_rule_label>>)
OPTIONAL MATCH (merged)-[outbound]->()
OPTIONAL MATCH (extracted)-[]->(extracted_nested:<<any_nested_label>>)
OPTIONAL MATCH (rule)-[]->(rule_nested:<<any_nested_label>>)

DETACH DELETE extracted_nested, rule_nested
DETACH DELETE extracted, rule
DELETE outbound
DELETE merged

RETURN
    count(DISTINCT merged) AS deleted_merged_count,
    count(DISTINCT extracted) AS deleted_extracted_count,
    count(DISTINCT rule) AS deleted_rule_count,
    count(DISTINCT extracted_nested) + count(DISTINCT rule_nested) AS deleted_nested_count;
