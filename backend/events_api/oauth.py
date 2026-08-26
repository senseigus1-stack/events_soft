from __future__ import annotations

import base64
import hashlib
import secrets
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Literal
from urllib.parse import urlencode
from uuid import uuid4

import httpx
from fastapi import HTTPException
from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from .config import Settings
from .models import ExternalIdentity, OAuthAttempt, User
from .security import issue_user_token, signing_settings, verify_authorization

ProviderId = Literal["google", "yandex", "github"]


@dataclass(frozen=True)
class Provider:
    id: ProviderId
    name: str
    authorize_url: str
    token_url: str
    userinfo_url: str
    scope: str


PROVIDERS: dict[ProviderId, Provider] = {
    "google": Provider(
        "google",
        "Google",
        "https://accounts.google.com/o/oauth2/v2/auth",
        "https://oauth2.googleapis.com/token",
        "https://openidconnect.googleapis.com/v1/userinfo",
        "openid email profile",
    ),
    "yandex": Provider(
        "yandex",
        "Яндекс ID",
        "https://oauth.yandex.ru/authorize",
        "https://oauth.yandex.ru/token",
        "https://login.yandex.ru/info",
        "login:email login:info login:avatar",
    ),
    "github": Provider(
        "github",
        "GitHub",
        "https://github.com/login/oauth/authorize",
        "https://github.com/login/oauth/access_token",
        "https://api.github.com/user",
        "read:user",
    ),
}


def credentials(provider_id: ProviderId, settings: Settings) -> tuple[str, str]:
    return (
        getattr(settings, f"{provider_id}_oauth_client_id"),
        getattr(settings, f"{provider_id}_oauth_client_secret"),
    )


def configured_providers(settings: Settings) -> list[dict[str, str | bool]]:
    return [
        {
            "id": provider.id,
            "name": provider.name,
            "available": all(credentials(provider.id, settings)),
        }
        for provider in PROVIDERS.values()
    ]


def _callback_url(provider_id: ProviderId, settings: Settings) -> str:
    return (
        f"{settings.oauth_public_base_url.rstrip('/')}"
        f"/api/v1/auth/oauth/{provider_id}/callback"
    )


def _pkce_challenge(verifier: str) -> str:
    digest = hashlib.sha256(verifier.encode()).digest()
    return base64.urlsafe_b64encode(digest).decode().rstrip("=")


def start_oauth(
    session: Session,
    provider_id: ProviderId,
    settings: Settings,
    *,
    user_id: str | None,
    authorization: str,
) -> str:
    provider = PROVIDERS.get(provider_id)
    if provider is None:
        raise HTTPException(status_code=404, detail="oauth_provider_not_found")
    client_id, client_secret = credentials(provider_id, settings)
    if not client_id or not client_secret:
        raise HTTPException(status_code=503, detail="oauth_provider_not_configured")
    if user_id and not verify_authorization(authorization, user_id, settings):
        raise HTTPException(status_code=401, detail="invalid_user_token")

    session.execute(
        delete(OAuthAttempt).where(OAuthAttempt.expires_at < datetime.now(timezone.utc))
    )
    state = secrets.token_urlsafe(32)
    verifier = secrets.token_urlsafe(64)
    session.add(
        OAuthAttempt(
            state=state,
            provider=provider_id,
            code_verifier=verifier,
            user_id=user_id,
            expires_at=datetime.now(timezone.utc)
            + timedelta(minutes=settings.oauth_state_minutes),
        )
    )
    session.commit()
    params = {
        "response_type": "code",
        "client_id": client_id,
        "redirect_uri": _callback_url(provider_id, settings),
        "scope": provider.scope,
        "state": state,
        "code_challenge": _pkce_challenge(verifier),
        "code_challenge_method": "S256",
    }
    return f"{provider.authorize_url}?{urlencode(params)}"


def normalize_profile(provider_id: ProviderId, payload: dict[str, Any]) -> dict[str, str]:
    if provider_id == "google":
        return {
            "subject": str(payload.get("sub", "")),
            "display_name": str(payload.get("name", "")),
            "email": str(payload.get("email", "")),
            "avatar_url": str(payload.get("picture", "")),
        }
    if provider_id == "yandex":
        avatar_id = str(payload.get("default_avatar_id", ""))
        return {
            "subject": str(payload.get("id", "")),
            "display_name": str(
                payload.get("display_name") or payload.get("real_name") or payload.get("login") or ""
            ),
            "email": str(payload.get("default_email", "")),
            "avatar_url": (
                f"https://avatars.yandex.net/get-yapic/{avatar_id}/islands-200"
                if avatar_id
                else ""
            ),
        }
    return {
        "subject": str(payload.get("id", "")),
        "display_name": str(payload.get("name") or payload.get("login") or ""),
        "email": str(payload.get("email") or ""),
        "avatar_url": str(payload.get("avatar_url") or ""),
    }


async def finish_oauth(
    session: Session,
    provider_id: ProviderId,
    settings: Settings,
    *,
    code: str,
    state: str,
    client: httpx.AsyncClient | None = None,
) -> str:
    provider = PROVIDERS.get(provider_id)
    attempt = session.get(OAuthAttempt, state)
    if provider is None or attempt is None or attempt.provider != provider_id:
        raise HTTPException(status_code=400, detail="invalid_oauth_state")
    expires_at = attempt.expires_at
    if expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=timezone.utc)
    if expires_at <= datetime.now(timezone.utc):
        session.delete(attempt)
        session.commit()
        raise HTTPException(status_code=400, detail="expired_oauth_state")

    client_id, client_secret = credentials(provider_id, settings)
    owns_client = client is None
    if client is None:
        client = httpx.AsyncClient(timeout=15, follow_redirects=False)
    try:
        token_response = await client.post(
            provider.token_url,
            data={
                "grant_type": "authorization_code",
                "code": code,
                "client_id": client_id,
                "client_secret": client_secret,
                "redirect_uri": _callback_url(provider_id, settings),
                "code_verifier": attempt.code_verifier,
            },
            headers={"Accept": "application/json"},
        )
        token_response.raise_for_status()
        access_token = str(token_response.json().get("access_token", ""))
        if not access_token:
            raise HTTPException(status_code=502, detail="oauth_token_missing")
        profile_response = await client.get(
            provider.userinfo_url,
            headers={
                "Authorization": f"Bearer {access_token}",
                "Accept": "application/json",
                "User-Agent": "Vayobyzh/1.0",
            },
        )
        profile_response.raise_for_status()
    except httpx.HTTPError as error:
        raise HTTPException(status_code=502, detail="oauth_provider_error") from error
    finally:
        if owns_client:
            await client.aclose()

    profile = normalize_profile(provider_id, profile_response.json())
    if not profile["subject"]:
        raise HTTPException(status_code=502, detail="oauth_profile_invalid")
    identity = session.scalar(
        select(ExternalIdentity).where(
            ExternalIdentity.provider == provider_id,
            ExternalIdentity.subject == profile["subject"],
        )
    )
    if identity:
        user = session.get(User, identity.user_id)
        if user is None:
            raise HTTPException(status_code=409, detail="oauth_identity_orphaned")
    else:
        user = session.get(User, attempt.user_id) if attempt.user_id else None
        if user is None:
            user = User(id=str(uuid4()))
            session.add(user)
        identity = ExternalIdentity(provider=provider_id, subject=profile["subject"], user_id=user.id)
        session.add(identity)

    identity.email = profile["email"]
    identity.display_name = profile["display_name"]
    identity.avatar_url = profile["avatar_url"]
    if profile["display_name"]:
        user.display_name = profile["display_name"]
    if profile["avatar_url"]:
        user.avatar_url = profile["avatar_url"]
    session.delete(attempt)
    session.commit()

    token = issue_user_token(user.id, signing_settings(settings))
    fragment = urlencode({"auth_token": token, "user_id": user.id, "provider": provider_id})
    return f"{settings.oauth_frontend_url.rstrip('/')}/#{fragment}"
