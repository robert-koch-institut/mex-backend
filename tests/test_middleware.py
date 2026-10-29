import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pytest import MonkeyPatch
from starlette import status

from mex.backend.middleware import (
    API_CSP,
    DOCS_CSP,
    SECURITY_HEADERS,
    SettingsCORSMiddleware,
)
from mex.backend.settings import BackendSettings


@pytest.mark.parametrize(
    ("path", "expected_csp"),
    [("/v0/_system/check", API_CSP), ("/docs", DOCS_CSP)],
    ids=["api", "docs"],
)
def test_security_headers(client: TestClient, path: str, expected_csp: str) -> None:
    response = client.get(path)

    assert response.status_code == status.HTTP_200_OK, response.text
    assert response.headers["Content-Security-Policy"] == expected_csp
    assert response.headers["Cache-Control"] == "no-store"
    for name, value in SECURITY_HEADERS.items():
        assert response.headers[name] == value


def test_redoc_is_disabled(client: TestClient) -> None:
    response = client.get("/redoc")

    assert response.status_code == status.HTTP_404_NOT_FOUND


@pytest.mark.parametrize(
    ("allowed_origins", "expected_header"),
    [([], None), (["https://editor.example.org"], "https://editor.example.org")],
    ids=["same-origin-only", "configured-origin"],
)
def test_settings_cors_middleware(
    monkeypatch: MonkeyPatch, allowed_origins: list[str], expected_header: str | None
) -> None:
    settings = BackendSettings.get()
    monkeypatch.setattr(settings, "backend_cors_allowed_origins", allowed_origins)
    app = FastAPI()

    @app.get("/")
    def root() -> dict[str, str]:
        return {}

    app.add_middleware(SettingsCORSMiddleware)

    with TestClient(app) as client:
        response = client.get("/", headers={"Origin": "https://editor.example.org"})

    assert response.headers.get("Access-Control-Allow-Origin") == expected_header


def test_settings_cors_middleware_restricts_methods_and_headers(
    monkeypatch: MonkeyPatch,
) -> None:
    settings = BackendSettings.get()
    monkeypatch.setattr(
        settings, "backend_cors_allowed_origins", ["https://editor.example.org"]
    )
    app = FastAPI()
    app.add_middleware(SettingsCORSMiddleware)
    preflight = {
        "Origin": "https://editor.example.org",
        "Access-Control-Request-Method": "POST",
    }

    with TestClient(app) as client:
        allowed = client.options(
            "/", headers={**preflight, "Access-Control-Request-Headers": "x-api-key"}
        )
        disallowed_header = client.options(
            "/", headers={**preflight, "Access-Control-Request-Headers": "x-custom"}
        )
        disallowed_method = client.options(
            "/",
            headers={**preflight, "Access-Control-Request-Method": "PATCH"},
        )

    assert allowed.status_code == status.HTTP_200_OK
    assert "*" not in allowed.headers["Access-Control-Allow-Methods"]
    assert disallowed_header.status_code == status.HTTP_400_BAD_REQUEST
    assert disallowed_method.status_code == status.HTTP_400_BAD_REQUEST
