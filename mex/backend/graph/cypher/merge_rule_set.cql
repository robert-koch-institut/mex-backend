MERGE (merged:<<merged_label>> {identifier: $stable_target_id})
MERGE (additive:<<additive_label>>)-[stableTargetId:stableTargetId {position: 0}]->(merged)
ON CREATE SET additive = $on_create
ON MATCH SET additive += $on_match
WITH additive, [] as edges, [] as values, [] as pruned
RETURN additive, edges, values, pruned;
