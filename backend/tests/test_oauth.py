import asyncio
from urllib.parse import parse_qs, urlparse

import httpx
from fastapi import HTTPException
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.pool import StaticPool

from events_api.config import Settings
from events_api.database import Base
from events_api.models import ExternalIdentity, OAuthAttempt, User
from events_api.oauth import configured_providers, finish_oauth, normalize_profile, start_oauth
from events_api.security import issue_user_token, verify_user_token


def build_session() -> Session:
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    return Session(engine, expire_on_commit=False)


def oauth_settings() -> Settings:
    return Settings(
        _env_file=None,
        environment="test",
        app_secret="test-oauth-signing-secret",
        oauth_public_base_url="https://events.example.test",
        oauth_frontend_url="https://app.example.test",
        google_oauth_client_id="google-id",
        google_oauth_client_secret="google-secret",
    )


def test_provider_list_only_marks_complete_credentials_available():
    items = configured_providers(oauth_settings())
    assert items[0] == {"id": "google", "name": "Google", "available": True}
    assert {item["id"] for item in items if not item["available"]} == {"yandex", "github"}


def test_start_uses_state_pkce_and_verified_guest_identity():
    session = build_session()
    try:
        settings = oauth_settings()
        user = User(id="guest")
        session.add(user)
        session.commit()
        token = issue_user_token(user.id, settings)
        url = start_oauth(
            session,
            "google",
            settings,
            user_id=user.id,
            authorization=f"Bearer {token}",
        )
        query = parse_qs(urlparse(url).query)
        assert query["client_id"] == ["google-id"]
        assert query["code_challenge_method"] == ["S256"]
        attempt = session.get(OAuthAttempt, query["state"][0])
        assert attempt is not None
        assert attempt.user_id == "guest"
        assert attempt.code_verifier not in url
    finally:
        session.close()


def test_start_rejects_an_unverified_guest_identity():
    session = build_session()
    try:
        try:
            start_oauth(
                session,
                "google",
                oauth_settings(),
                user_id="guest",
                authorization="Bearer bad-token",
            )
        except HTTPException as error:
            assert error.status_code == 401
        else:
            raise AssertionError("invalid token was accepted")
    finally:
        session.close()


def test_callback_links_account_without_saving_provider_token():
    session = build_session()
    try:
        settings = oauth_settings()
        user = User(id="guest")
        session.add(user)
        session.commit()
        token = issue_user_token(user.id, settings)
        start_url = start_oauth(
            session,
            "google",
            settings,
            user_id=user.id,
            authorization=f"Bearer {token}",
        )
        state = parse_qs(urlparse(start_url).query)["state"][0]

        def handler(request: httpx.Request) -> httpx.Response:
            if request.url.path == "/token":
                assert b"code_verifier=" in request.content
                return httpx.Response(200, json={"access_token": "provider-access-token"})
            assert request.headers["Authorization"] == "Bearer provider-access-token"
            return httpx.Response(
                200,
                json={
                    "sub": "google-42",
                    "name": "Ada Eventova",
                    "email": "ada@example.test",
                    "picture": "https://example.test/ada.jpg",
                },
            )

        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        redirect = asyncio.run(
            finish_oauth(
                session,
                "google",
                settings,
                code="one-time-code",
                state=state,
                client=client,
            )
        )
        asyncio.run(client.aclose())
        fragment = parse_qs(urlparse(redirect).fragment)
        assert redirect.startswith("https://app.example.test/#")
        assert fragment["user_id"] == ["guest"]
        assert verify_user_token(fragment["auth_token"][0], "guest", settings)
        assert session.get(OAuthAttempt, state) is None
        identity = session.query(ExternalIdentity).one()
        assert identity.subject == "google-42"
        assert identity.email == "ada@example.test"
        assert session.get(User, "guest").display_name == "Ada Eventova"
        assert "provider-access-token" not in redirect
    finally:
        session.close()


def test_profile_normalization_uses_provider_specific_fields():
    assert normalize_profile("github", {"id": 7, "login": "octo"})["display_name"] == "octo"
    yandex = normalize_profile(
        "yandex", {"id": "ya-1", "login": "yana", "default_avatar_id": "avatar"}
    )
    assert yandex["subject"] == "ya-1"
    assert yandex["avatar_url"].endswith("/avatar/islands-200")
