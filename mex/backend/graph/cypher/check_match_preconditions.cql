OPTIONAL MATCH (extracted:<<extracted_labels|join("|")>> {identifier: $extracted_identifier})-[:stableTargetId]->(old_merged:<<merged_labels|join("|")>>)
OPTIONAL MATCH (new_merged:<<merged_labels|join("|")>> {identifier: $merged_identifier})
WITH
   count(extracted) = 1 AS extracted_exists,
   count(new_merged) = 1 AS merged_exists,
   elementId(old_merged) <> elementId(new_merged) AS not_self_merge,
   labels(old_merged) = labels(new_merged) AS matching_types,
   ANY(label IN labels(extracted) WHERE label IN $blocked_types) <> false AS allowed_type
RETURN *;
