<# Upsert extracted or rule items with their associated merged items and relationships.

Jinja:
    params: IngestParams object query configuration

Args:
    data: IngestData object containing items to merge

Returns:
    identifier: Identifier of the extracted item or null when rule
    stableTargetId: Identifier of the associated merged item
    entityType: Type of the extracted or rule item
    nodeProps: Properties of the extracted or rule item
    linkRels: Relations that link to other existing nodes
    createRels: Relations that link to nodes that need creating
-#>
CYPHER 25

WITH $data AS data

<# create merged node if not exist -#>
MERGE (merged:<<params.merged_label>> {identifier: data.stableTargetId})
<# create extracted node or rule node and connect it to merged node -#>
CALL (merged, data) {
  WHEN data.identifier IS NOT NULL THEN {
    MERGE (main:<<params.node_label>> {identifier: data.identifier})-[:stableTargetId {position: 0}]->(merged)
    RETURN main
  }
  ELSE {
    MERGE (main:<<params.node_label>> )-[:stableTargetId {position: 0}]->(merged)
    RETURN main
  }
}

SET main += data.nodeProps

WITH main, merged.identifier AS stableTargetId, data

<% if params.has_create_rels -%>
<# create Text/Link nodes and connect them to main node -#>
CALL (main, data) {
    UNWIND data.createRels AS createRel
    MERGE (main)-[newCreateEdge:$(createRel.edgeLabel) {position: createRel.edgeProps.position}]->(creationTarget:$(createRel.nodeLabels))
    SET creationTarget += createRel.nodeProps
    RETURN collect(newCreateEdge) as newCreateEdges
}
<%- endif %>

<% if params.has_link_rels -%>
<# connect main node to referenced merged items -#>
CALL (main, data) {
    UNWIND data.linkRels AS linkRel
    MATCH (linkTarget:<<params.all_referenced_labels|join("|")>> {identifier: linkRel.nodeProps.identifier})
    MERGE (main)-[newLinkEdge:$(linkRel.edgeLabel) {position: linkRel.edgeProps.position}]->(linkTarget)
    RETURN collect(newLinkEdge) as newLinkEdges
}
<%- endif %>

<% if params.has_create_rels -%>
<# delete referenced Text/Link nodes and their edges that are not in newCreateEdges -#>
CALL (main, newCreateEdges) {
    OPTIONAL MATCH (main)-[gcEdge:<<params.delete_node_edges|join("|")>>]->(gcNode:<<params.all_nested_labels|join("|")>>)
        WHERE NOT gcEdge IN newCreateEdges
    DETACH DELETE gcNode
}
<%- endif %>

<% if params.has_link_rels -%>
<# delete edges to merged nodes that are not in newLinkEdges -#>
CALL (main, newLinkEdges) {
    OPTIONAL MATCH (main)-[gcEdge:<<params.detach_node_edges|join("|")>>]->(:<<params.all_referenced_labels|join("|")>>)
    WHERE NOT gcEdge IN newLinkEdges
    DELETE gcEdge
}
<%- endif %>

<% if params.has_create_rels -%>
<# get all relations to nested items -#>
CALL (main) {
    MATCH (main)-[createEdge]->(createNode:<<params.all_nested_labels|join("|")>>)
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
<%- endif %>

<# get all relations to merged items that are not of relation type "stableTargetId" -#>
<% if params.has_link_rels -%>
CALL (main) {
    MATCH (main)-[linkEdge]->(linkNode:<<params.all_referenced_labels|join("|")>>)
    WHERE type(linkEdge) <> "stableTargetId"
    ORDER BY type(linkEdge), linkEdge.position
    RETURN collect(
        {
            nodeLabels: labels(linkNode),
            nodeProps: properties(linkNode),
            edgeLabel: type(linkEdge),
            edgeProps: properties(linkEdge)
        }
    ) AS linkRels
}
<%- endif %>

<# return current state for validation -#>
RETURN
    main.identifier AS identifier,
    stableTargetId,
    head(labels(main)) AS entityType,
<%- if params.has_link_rels %>
    linkRels,
<%- endif %>
<%- if params.has_create_rels %>
    createRels,
<%- endif %>
    properties(main) AS nodeProps;
