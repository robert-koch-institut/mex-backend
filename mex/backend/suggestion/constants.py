from typing import Final

#: how many raw lucene hits to pull before resolving them to merged items
NEIGHBOURHOOD_HIT_LIMIT: Final = 200
#: how many top scoring merged items are allowed to vote
NEIGHBOURHOOD_SIZE: Final = 25
#: how many values per field the aggregation keeps. `country` has 281 members,
#: every other vocabulary has at most 22, so this only truncates that one tail:
#: a rare but correct country can drop out of the conditioned signals while
#: still surviving in the corpus prior
AGGREGATION_TOP_K: Final = 25
#: default and maximum number of candidates returned per field
DEFAULT_CANDIDATE_LIMIT: Final = 3
MAX_CANDIDATE_LIMIT: Final = 10
#: seconds after which the server aborts a suggestion query, so a pathological
#: cohort cannot hold a pooled connection for the whole session timeout
SUGGESTION_QUERY_TIMEOUT: Final = 2.0
#: cache key prefix for the corpus wide prior
CORPUS_PRIOR_CACHE_PREFIX: Final = "suggestion-corpus-prior"
