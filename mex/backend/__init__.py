from mex.backend.cache import (
    MemoryCacheConnector,
    ValkeyCacheConnector,
    register_cache_connector,
)
from mex.backend.identity.provider import GraphIdentityProvider
from mex.backend.types import CacheConnectorType
from mex.common.identity import register_provider
from mex.common.types import IdentityProvider

register_provider(IdentityProvider.GRAPH, GraphIdentityProvider)
register_cache_connector(CacheConnectorType.MEMORY, MemoryCacheConnector)
register_cache_connector(CacheConnectorType.VALKEY, ValkeyCacheConnector)
