from mex.backend.identity.provider import GraphIdentityProvider
from mex.common.identity import register_provider
from mex.common.types import IdentityProvider

register_provider(IdentityProvider.GRAPH, GraphIdentityProvider)
