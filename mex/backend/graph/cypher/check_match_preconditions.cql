<# Check preconditions for merging an extracted item with a merged item.

This query validates that all necessary conditions are met before allowing
an extracted item to be merged with a different merged item. It checks:
- The extracted item exists and has an associated merged item
- The target merged item exists
- Both merged items have complete rule sets (3 rules each)
- The merge is not a self-merge (different merged items)
- The items have matching types
- The extracted item type is allowed to be merged

Args:
    extracted_identifier: Identifier of the extracted item to merge
    merged_identifier: Identifier of the target merged item
    extracted_labels: List of all extracted class labels
    merged_labels: List of all merged class labels
    blocked_types: List of entity types that cannot be merged

Returns:
    extracted_exists: Boolean indicating if the extracted item exists
    old_rules_exist: Boolean indicating if the old merged item has complete rules
    merged_exists: Boolean indicating if the target merged item exists
    new_rules_exist: Boolean indicating if the target merged item has complete rules
    not_self_merge: Boolean indicating this is not a noop self-merge
    matching_types: Boolean indicating the items have matching stem types
    allowed_types: Boolean indicating the extracted type is not blocked
-#>
OPTIONAL MATCH (extracted:<<extracted_labels|join("|")>> {identifier: $extracted_identifier})-[:stableTargetId]->(old_merged:<<merged_labels|join("|")>>)
OPTIONAL MATCH (old_merged)<-[:stableTargetId]-(old_rules:<<merged_labels|join("|")>>)
OPTIONAL MATCH (new_merged:<<merged_labels|join("|")>> {identifier: $merged_identifier})
OPTIONAL MATCH (new_merged)<-[:stableTargetId]-(new_rules:<<merged_labels|join("|")>>)
WITH
   count(extracted) = 1 AS extracted_exists,
   count(old_rules) = 3 AS old_rules_exist,
   count(new_merged) = 1 AS merged_exists,
   count(new_rules) = 3 AS new_rules_exist,
   elementId(old_merged) <> elementId(new_merged) AS not_self_merge,
   labels(old_merged) = labels(new_merged) AS matching_types,
   ANY(label IN labels(extracted) WHERE label IN $blocked_types) <> false AS allowed_types,
RETURN *;
