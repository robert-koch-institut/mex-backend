import httpx
from fastapi import APIRouter, Request, Response

from mex.backend.settings import BackendSettings

router = APIRouter()


@router.post("/oauth/token", tags=["oauth"])
async def proxy_token(request: Request) -> Response:
    """Proxy the OAuth2 token request to the OIDC issuer.

    This avoids cross-origin requests from Swagger UI to the OIDC provider,
    which may not support CORS on the token endpoint.
    """
    settings = BackendSettings.get()
    token_url = f"{settings.oidc_issuer_url}/token"
    body = await request.body()
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            token_url,
            content=body,
            headers={"Content-Type": request.headers.get("content-type", "")},
        )
    return Response(
        content=resp.content,
        status_code=resp.status_code,
        headers=dict(resp.headers),
    )
