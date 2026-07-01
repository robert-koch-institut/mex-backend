<% from "macros/filter_nodes_by_reference_filters.cql" import filter_nodes_by_reference_filters with context -%>

MATCH (merged_node:<<any_merged_label>>)
<<filter_nodes_by_reference_filters()>>
RETURN merged_node;
