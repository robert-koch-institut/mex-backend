from pkgutil import extend_path

__path__ = extend_path(__path__, __name__)

from mex.backend.identity.provider import GraphIdentityProvider
from mex.backend.types import BackendIdentityProvider
from mex.common.identity.registry import register_provider

register_provider(BackendIdentityProvider.GRAPH, GraphIdentityProvider)
