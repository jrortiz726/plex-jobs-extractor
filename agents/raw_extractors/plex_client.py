"""Async helper for calling Plex REST endpoints with retry logic."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional

import aiohttp

logger = logging.getLogger(__name__)


class PlexAPIClient:
    """Thin wrapper around Plex REST endpoints with exponential backoff."""

    def __init__(
        self,
        *,
        base_url: str,
        api_key: str,
        customer_id: str,
        max_retries: int = 3,
        retry_delay: int = 5,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.headers = {
            "X-Plex-Connect-Api-Key": api_key,
            "X-Plex-Connect-Customer-Id": customer_id,
            "Content-Type": "application/json",
        }
        self.max_retries = max_retries
        self.retry_delay = retry_delay

    async def get(
        self,
        endpoint: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        session: Optional[aiohttp.ClientSession] = None,
    ) -> Any:
        url = f"{self.base_url}{endpoint}"
        attempt = 0
        while True:
            attempt += 1
            try:
                if session:
                    resp = await session.get(url, headers=self.headers, params=params)
                    data = await self._handle_response(resp)
                else:
                    async with aiohttp.ClientSession() as local_session:
                        resp = await local_session.get(url, headers=self.headers, params=params)
                        data = await self._handle_response(resp)
                return data
            except Exception as exc:  # broad catch -> logged and re-raised after retries
                logger.warning("Plex request failed (%s/%s): %s", attempt, self.max_retries, exc)
                if attempt >= self.max_retries:
                    raise
                await asyncio.sleep(self.retry_delay * attempt)

    async def fetch_paginated(
        self,
        endpoint: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        page_param: str = "offset",
        page_size: int = 1000,
        data_key: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        collected: List[Dict[str, Any]] = []
        offset = 0
        params = dict(params or {})
        async with aiohttp.ClientSession() as session:
            while True:
                params[page_param] = offset
                payload = await self.get(endpoint, params=params, session=session)
                if isinstance(payload, dict) and data_key:
                    items = payload.get(data_key, [])
                elif isinstance(payload, list):
                    items = payload
                else:
                    items = payload.get("items", []) if isinstance(payload, dict) else []

                if not items:
                    break
                collected.extend(items)
                if len(items) < page_size:
                    break
                offset += len(items)
        return collected

    async def fetch_since(
        self,
        endpoint: str,
        *,
        since: Optional[datetime] = None,
        params: Optional[Dict[str, Any]] = None,
        timestamp_field: str = "lastUpdated",
    ) -> List[Dict[str, Any]]:
        records = await self.fetch_paginated(endpoint, params=params)
        if since is None:
            return records
        filtered: List[Dict[str, Any]] = []
        for record in records:
            candidate = record.get(timestamp_field) or record.get("updated_at") or record.get("updatedAt")
            if candidate is None:
                filtered.append(record)
                continue
            try:
                candidate_dt = self._parse_datetime(candidate)
            except ValueError:
                filtered.append(record)
                continue
            if candidate_dt >= since:
                filtered.append(record)
        return filtered

    async def _handle_response(self, response: aiohttp.ClientResponse) -> Any:
        if response.status >= 400:
            text = await response.text()
            raise RuntimeError(f"Plex API {response.status}: {text}")
        if "application/json" in response.headers.get("Content-Type", ""):
            return await response.json()
        return await response.text()

    def _parse_datetime(self, value: Any) -> datetime:
        if isinstance(value, datetime):
            return value
        if isinstance(value, (int, float)):
            return datetime.utcfromtimestamp(value)
        if isinstance(value, str):
            # Plex timestamps are ISO8601
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        raise ValueError(f"Unsupported timestamp value: {value!r}")
