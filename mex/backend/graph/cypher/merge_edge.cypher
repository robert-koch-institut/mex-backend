MATCH (fromNode:<<extracted_labels|join("|")>> {identifier: $source_node})
MATCH (toNode:<<merged_labels|join("|")>> {identifier: $target_node})
MERGE (fromNode)-[edge:<<edge_label>> {position: $position}]->(toNode)
RETURN edge;
