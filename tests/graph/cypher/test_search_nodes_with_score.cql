<% from "macros/search_nodes_with_score.cql" import search_nodes_with_score with context -%>

<<search_nodes_with_score()>>
ORDER BY score DESC, merged_node.identifier ASC
RETURN merged_node.identifier AS identifier, score;
