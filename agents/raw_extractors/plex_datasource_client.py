"""Async client for Plex Data Source API."""

from __future__ import annotations

import asyncio
import base64
import json
import logging
from typing import Any, Dict, Optional

import aiohttp

logger = logging.getLogger(__name__)


class PlexDataSourceClient:
    """Wrapper for invoking Plex Data Source execute endpoints."""

    def __init__(
        self,
        *,
        host: str,
        username: str,
        password: str,
        max_retries: int = 3,
        retry_delay: int = 5,
    ) -> None:
        self.base_url = host.rstrip("/")
        encoded = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("utf-8")
        self.auth_header = f"Basic {encoded}"
        self.max_retries = max_retries
        self.retry_delay = retry_delay

    async def execute(self, datasource_id: int, inputs: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{self.base_url}/api/datasources/{datasource_id}/execute"
        payload = {"inputs": inputs} if inputs else {"inputs": {}}
        headers = {
            "Authorization": self.auth_header,
            "Content-Type": "application/json; charset=utf-8",
            "Accept": "application/json",
        }

        last_error: Optional[Exception] = None
        for attempt in range(1, self.max_retries + 1):
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.post(url, json=payload, headers=headers) as response:
                        if response.status >= 400:
                            text = await response.text()
                            raise RuntimeError(
                                f"Plex Data Source {response.status}: {text}"
                            )
                        if response.content_type == "application/json":
                            return await response.json()
                        text = await response.text()
                        # Wrap non-JSON responses for downstream handling
                        return {"raw": text}
            except Exception as exc:  # pragma: no cover - network errors
                last_error = exc
                logger.warning(
                    "Data source %s request failed (%s/%s): %s",
                    datasource_id,
                    attempt,
                    self.max_retries,
                    exc,
                )
                if attempt >= self.max_retries:
                    break
                await asyncio.sleep(self.retry_delay * attempt)
        assert last_error is not None
        raise last_error
