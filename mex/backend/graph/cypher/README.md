The queries in this directory are written in the
[cypher query language](https://neo4j.com/docs/getting-started/cypher-intro/),
but contain dynamic templated elements utilizing the
[jinja templating engine](https://jinja.palletsprojects.com/en/latest/).

The templated elements never contain user input or concrete query parameters!
Those are transmitted to the driver separately from the query, to protect against
injection and improve performance.
See: https://neo4j.com/docs/python-manual/current/query-simple/#query-parameters
Instead, the templated elements are only used to dynamically adjust to changes in
the data structure or to render multiple similar queries from the same template:
For example, a new model class or changing model fields are automatically handled
and don't require rewriting any cypher query.

Some of these use-cases could be covered by neo4j's [APOC](https://neo4j.com/labs/apoc/)
add-on (e.g. `expand_references_in_search_result`). However, APOC is not included in the
official neo4j docker image. So, to keep deployment simple, the use of APOC was avoided.

Contrary to the jinja default tags that are centered around curly braces, we use
less/greater signs that do not collide with cypher syntax that often.
See: `mex.backend.graph.query.QueryBuilder`
