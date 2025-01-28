from pkgutil import extend_path

__path__ = extend_path(__path__, __name__)

from mex.backend.identity.provider import GraphIdentityProvider
from mex.common.identity.registry import register_provider
from mex.common.types import IdentityProvider

register_provider(IdentityProvider.GRAPH, GraphIdentityProvider)
