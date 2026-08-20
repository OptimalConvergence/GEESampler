from __future__ import annotations

from typing import Any

import httplib2
import requests
from requests.adapters import HTTPAdapter


class RequestsTransport:
    """Thread-safe-enough httplib2-compatible transport with a sized connection pool."""

    def __init__(self, *, pool_size: int, timeout: float | None = None):
        self.pool_size = pool_size
        self.timeout = timeout
        self.session = requests.Session()
        adapter = HTTPAdapter(
            pool_connections=pool_size,
            pool_maxsize=pool_size,
            pool_block=True,
            max_retries=0,
        )
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    def request(
        self,
        uri: str,
        method: str = "GET",
        body: bytes | str | None = None,
        headers: dict[str, str] | None = None,
        redirections: int | None = None,
        connection_type: type[Any] | None = None,
    ) -> tuple[httplib2.Response, bytes]:
        del redirections, connection_type
        response = self.session.request(
            method,
            uri,
            data=body,
            headers=headers,
            timeout=self.timeout,
        )
        response_headers: dict[str, Any] = dict(response.headers)
        response_headers["status"] = response.status_code
        return httplib2.Response(response_headers), response.content

    def close(self) -> None:
        self.session.close()
