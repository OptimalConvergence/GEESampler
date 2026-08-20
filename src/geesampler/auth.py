from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from .models import AuthConfig
from .transport import RequestsTransport

HIGH_VOLUME_URL = "https://earthengine-highvolume.googleapis.com"


def configure_proxy(proxy_url: str | None = None) -> None:
    """Set standard proxy variables without creating or managing a VPN process."""
    if not proxy_url:
        return
    os.environ["HTTP_PROXY"] = proxy_url
    os.environ["HTTPS_PROXY"] = proxy_url
    os.environ["http_proxy"] = proxy_url
    os.environ["https_proxy"] = proxy_url


def validate_auth_config(config: AuthConfig) -> None:
    """Validate a credential reference without returning or logging its contents."""
    if not config.service_account:
        if config.key_file:
            raise ValueError("service_account is required when key_file is set")
        return
    if not config.key_file:
        raise ValueError("key_file is required when service_account is set")
    key_file = Path(config.key_file).expanduser()
    if not key_file.is_file():
        raise FileNotFoundError("Configured service-account key file does not exist")
    if os.name == "posix" and key_file.stat().st_mode & 0o077:
        raise PermissionError("Service-account key file must have owner-only permissions (0600)")
    try:
        payload = json.loads(key_file.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError("Service-account key file is not valid JSON") from exc
    required = ("type", "project_id", "client_email", "private_key")
    if payload.get("type") != "service_account" or not all(payload.get(key) for key in required):
        raise ValueError("Credential file is not a complete service-account key")
    if payload["client_email"] != config.service_account:
        raise ValueError("Configured service account does not match credential file")
    if payload["project_id"] != config.project:
        raise ValueError("Configured project does not match credential file")


def initialize_earth_engine(config: AuthConfig, *, pool_size: int = 8) -> Any:
    import ee

    validate_auth_config(config)
    endpoint = HIGH_VOLUME_URL if config.high_volume else None
    kwargs: dict[str, Any] = {"project": config.project}
    if endpoint:
        kwargs["url"] = endpoint
    if config.service_account:
        key_file = Path(config.key_file).expanduser()
        kwargs["credentials"] = ee.ServiceAccountCredentials(config.service_account, str(key_file))
    kwargs["http_transport"] = RequestsTransport(pool_size=max(1, pool_size))
    ee.Initialize(**kwargs)
    return ee
