<% from "macros/collect_effective_vocabulary_values.cql" import collect_effective_vocabulary_values with context -%>

MATCH (merged_node:<<any_merged_label>>)
<<collect_effective_vocabulary_values()>>
RETURN merged_node.identifier AS identifier, field, values_on_item
ORDER BY identifier, field ASC;
