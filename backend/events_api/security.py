from __future__ import annotations

import base64
import hashlib
import hmac
import time

from fastapi import Depends, Header, HTTPException

from .config import Settings, get_settings


def signing_settings(settings: Settings) -> Settings:
    if settings.app_secret:
        return settings
    return settings.model_copy(update={"app_secret": "development-only"})


def _encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).decode().rstrip("=")


def issue_user_token(user_id: str, settings: Settings) -> str:
    if not settings.app_secret:
        raise RuntimeError("APP_SECRET is required")
    issued_at = int(time.time())
    payload = f"{user_id}.{issued_at}"
    signature = hmac.new(
        settings.app_secret.encode(), payload.encode(), hashlib.sha256
    ).digest()
    return f"{payload}.{_encode(signature)}"


def verify_user_token(token: str, user_id: str, settings: Settings) -> bool:
    try:
        token_user_id, issued_at_text, supplied_signature = token.rsplit(".", 2)
        issued_at = int(issued_at_text)
    except (TypeError, ValueError):
        return False
    if token_user_id != user_id:
        return False
    if issued_at > int(time.time()) + 300:
        return False
    max_age = settings.auth_token_days * 86400
    if int(time.time()) - issued_at > max_age:
        return False
    payload = f"{token_user_id}.{issued_at}"
    expected = _encode(
        hmac.new(settings.app_secret.encode(), payload.encode(), hashlib.sha256).digest()
    )
    return hmac.compare_digest(expected, supplied_signature)


def require_user(
    user_id: str,
    authorization: str = Header(default=""),
    settings: Settings = Depends(get_settings),
) -> None:
    if settings.environment != "production":
        return
    if not settings.app_secret:
        raise HTTPException(status_code=503, detail="auth_not_configured")
    scheme, _, token = authorization.partition(" ")
    if scheme.casefold() != "bearer" or not verify_user_token(token, user_id, settings):
        raise HTTPException(status_code=401, detail="invalid_user_token")


def verify_authorization(authorization: str, user_id: str, settings: Settings) -> bool:
    scheme, _, token = authorization.partition(" ")
    return scheme.casefold() == "bearer" and verify_user_token(
        token, user_id, signing_settings(settings)
    )
