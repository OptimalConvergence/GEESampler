from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from .models import AuthConfig

HIGH_VOLUME_URL = "https://earthengine-highvolume.googleapis.com"


def configure_proxy(proxy_url: str | None = None) -> None:
    """Set standard proxy variables without creating or managing a VPN process."""
    if not proxy_url:
        return
    os.environ["HTTP_PROXY"] = proxy_url
    os.environ["HTTPS_PROXY"] = proxy_url
    os.environ["http_proxy"] = proxy_url
    os.environ["https_proxy"] = proxy_url


def initialize_earth_engine(config: AuthConfig) -> Any:
    import ee

    endpoint = HIGH_VOLUME_URL if config.high_volume else None
    kwargs: dict[str, Any] = {"project": config.project}
    if endpoint:
        kwargs["opt_url"] = endpoint
    if config.service_account:
        if not config.key_file:
            raise ValueError("key_file is required when service_account is set")
        key_file = Path(config.key_file).expanduser()
        if not key_file.is_file():
            raise FileNotFoundError(key_file)
        os.environ.setdefault("GOOGLE_APPLICATION_CREDENTIALS", str(key_file))
        kwargs["credentials"] = ee.ServiceAccountCredentials(config.service_account, str(key_file))
    ee.Initialize(**kwargs)
    return ee
