WITH $data AS data

<# create merged node if not exist -#>
MERGE (merged:$(data.mergedLabels) {identifier: data.stableTargetId})
<# create extracted/rule node if not exists -#>
MERGE (main:$(data.nodeLabels) {identifier: data.identifier})
<# connect extracted/rule node with merged node -#>
MERGE (main)-[:stableTargetId {position: 0}]->(merged)
SET main += data.nodeProps

WITH main, merged, data

<# create Text/Link nodes and connect them to extracted/rule item -#>
CALL (main, data) {
    UNWIND data.createRels AS rel
    MERGE (main)-[newEdge:$(rel.edgeLabel) {position: rel.edgeProps.position}]->(target:$(rel.nodeLabels))
    SET target += rel.nodeProps
    RETURN collect(newEdge) as newCreateEdges
}

<# connect extracted/rule item to referenced merged items -#>
CALL (main, data) {
    UNWIND data.linkRels AS rel
    MATCH (target:$(rel.nodeLabels) {identifier: rel.nodeProps.identifier})
    MERGE (main)-[newEdge:$(rel.edgeLabel) {position: rel.edgeProps.position}]->(target)
    RETURN collect(newEdge) as newLinkEdges
}

<# delete referenced Text/Link nodes and their edges that are not in $data -#>
CALL (main, newCreateEdges, data) {
    OPTIONAL MATCH (main)-[gcEdge]->(gcNode)
        WHERE TYPE(gcEdge) IN data.deleteNodes
        AND NOT gcEdge IN newCreateEdges
    DETACH DELETE gcNode
}

<# delete edges to merged nodes that are not in $data -#>
CALL (main, newLinkEdges, data) {
    OPTIONAL MATCH (main)-[gcEdge]->()
    WHERE TYPE(gcEdge) IN data.detachNodes
    AND NOT gcEdge IN newLinkEdges
    DELETE gcEdge
}

<# get all relations to merged items that are not of relation type "stableTargetId" -#>
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

<# get all relations to nested items -#>
CALL (main) {
    MATCH (main)-[createEdge]->(createNode:<<nested_labels|join("|")>>)
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

<# return current state for validation -#>
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
