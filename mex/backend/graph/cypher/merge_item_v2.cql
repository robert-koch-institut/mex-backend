WITH $data AS data

MERGE (merged:$(data.mergedLabels) {identifier: data.stableTargetId})
MERGE (main:$(data.nodeLabels) {identifier: data.identifier})
MERGE (main)-[:stableTargetId {position: 0}]->(merged)
SET main += data.nodeProps

WITH main, data

CALL (main, data) {
    UNWIND data.createRels AS rel
    MERGE (main)-[newEdge:$(rel.edgeLabel) {position: rel.edgeProps.position}]->(target:$(rel.nodeLabels))
    SET target += rel.nodeProps

    WITH rel, collect(newEdge) as newEdges, collect(target) AS createdTargets
    MATCH (main)-[gcEdge]->()
    WHERE TYPE(gcEdge) IN data.detachNodes
    AND NOT gcEdge IN newEdges
    DELETE gcEdge
    RETURN newEdges as createRels
}

CALL (main, data) {
    UNWIND data.linkRels AS rel
    MATCH (target:$(rel.nodeLabels) {identifier: rel.nodeProps.identifier})
    MERGE (main)-[newEdge:$(rel.edgeLabel) {position: rel.edgeProps.position}]->(target)

    WITH rel, collect(newEdge) as newEdges, collect(target) AS createdTargets
    MATCH (main)-[gcEdge]->(gcNode)
    WHERE TYPE(gcEdge) IN data.deleteNodes
    AND NOT gcEdge IN newEdges
    DETACH DELETE gcNode
    RETURN newEdges as linkRels
}

WITH collect(linkRels) as edges
RETURN "ok" AS status, edges;
