from __future__ import annotations

import time
from typing import Optional

import requests

from ..config import OAuthConfig
from ..errors import AuthError


class OAuthTokenProvider:
    def __init__(self, oauth: OAuthConfig, session: Optional[requests.Session] = None) -> None:
        self._oauth = oauth
        self._session = session or requests.Session()
        self._token: str | None = None
        self._expires_at: float = 0.0

    def get_token(self) -> str:
        now = time.time()
        if self._token and now < self._expires_at - 30:
            return self._token
        return self._refresh_token()

    def _refresh_token(self) -> str:
        data = {
            "grant_type": "client_credentials",
            "client_id": self._oauth.client_id,
            "client_secret": self._oauth.client_secret,
        }
        if self._oauth.scope:
            data["scope"] = self._oauth.scope

        try:
            response = self._session.post(self._oauth.token_url, data=data, timeout=10)
        except requests.RequestException as exc:
            raise AuthError("Failed to contact token endpoint") from exc

        if response.status_code != 200:
            raise AuthError(f"Token endpoint returned {response.status_code}")

        try:
            payload = response.json()
            token = payload["access_token"]
            expires_in = int(payload.get("expires_in", 3600))
        except (ValueError, KeyError) as exc:
            raise AuthError("Invalid token response payload") from exc

        self._token = token
        self._expires_at = time.time() + expires_in
        return token
