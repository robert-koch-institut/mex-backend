WITH $data AS data

MERGE (merged:$(data.mergedLabels) {identifier: data.stableTargetId})
MERGE (main:$(data.nodeLabels) {identifier: data.identifier})
MERGE (main)-[:stableTargetId {position: 0}]->(merged)
SET main += data.nodeProps

WITH main, merged, data

CALL (main, data) {
    UNWIND data.nestedTexts AS rel
    MATCH (t:Text {value: rel.nodeProps["value"]})
    MERGE (main)-[newEdge:$(rel.edgeLabel) {position: rel.edgeProps.position}]->(target:$(rel.nodeLabels))
    SET target += rel.nodeProps
    RETURN collect(newEdge) as newTextEdges
}

CALL (main, data) {
    UNWIND data.createRels AS rel
    MERGE (main)-[newEdge:$(rel.edgeLabel) {position: rel.edgeProps.position}]->(target:$(rel.nodeLabels))
    SET target += rel.nodeProps
    RETURN collect(newEdge) as newCreateEdges
}

CALL (main, data) {
    UNWIND data.linkRels AS rel
    MATCH (target:$(rel.nodeLabels) {identifier: rel.nodeProps.identifier})
    MERGE (main)-[newEdge:$(rel.edgeLabel) {position: rel.edgeProps.position}]->(target)
    RETURN collect(newEdge) as newLinkEdges
}

CALL (main, newCreateEdges, data) {
    OPTIONAL MATCH (main)-[gcEdge]->(gcNode)
    WHERE TYPE(gcEdge) IN data.deleteNodes
    AND NOT gcEdge IN newCreateEdges
    DETACH DELETE gcNode
}

CALL (main, newLinkEdges, data) {
    OPTIONAL MATCH (main)-[gcEdge]->()
    WHERE TYPE(gcEdge) IN data.detachNodes
    AND NOT gcEdge IN newLinkEdges
    DELETE gcEdge
}

CALL (main) {
    MATCH (main)-[linkEdge]->(linkNode:<<merged_labels|join("|")>>)
    WHERE type(linkEdge) <> "stableTargetId"
    ORDER BY type(linkEdge), linkEdge.position
    RETURN collect(
        {
            nodeLabels: [],
            nodeProps: properties(linkNode),
            edgeLabel: type(linkEdge),
            edgeProps: properties(linkEdge)
        }
    ) AS linkRels
}

CALL (main) {
    MATCH (main)-[createEdge]->(createNode:Link|Text)
    ORDER BY type(createEdge), createEdge.position
    RETURN collect(
        {
            nodeLabels: labels(createNode),
            nodeProps: properties(createNode),
            edgeLabel: type(createEdge),
            edgeProps: properties(createEdge)
        }
    ) AS createRels
}

RETURN
    main.identifier AS identifier,
    merged.identifier AS stableTargetId,
    labels(merged) AS mergedLabels,
    labels(main) AS nodeLabels,
    properties(main) AS nodeProps,
    linkRels AS linkRels,
    createRels AS createRels,
    data.deleteNodes as deleteNodes,
    data.detachNodes as detachNodes;
