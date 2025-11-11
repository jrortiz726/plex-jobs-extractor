"""Inventory RAW extractor."""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Optional

if __package__ in (None, ""):
    sys.path.append(str(Path(__file__).resolve().parents[2]))

from agents.raw_extractors.base import PlexRawExtractor, RawExtractorConfig
from agents.raw_extractors.common import env_int
from agents.raw_extractors.plex_client import PlexAPIClient


class InventoryRawExtractor(PlexRawExtractor):
    """Ingest Plex inventory containers into CDF RAW."""

    def __init__(self, config: RawExtractorConfig | None = None) -> None:
        super().__init__("inventory", config)
        self.lookback_days = env_int("INVENTORY_LOOKBACK_DAYS", 7)
        self.plex = PlexAPIClient(
            base_url=self.config.plex_base_url,
            api_key=self.config.plex_api_key,
            customer_id=self.config.plex_customer_id,
            max_retries=self.config.max_retries,
            retry_delay=self.config.retry_delay,
        )

    async def fetch_records(self, since: Optional[datetime]) -> list[Dict[str, Any]]:
        params = {"limit": 1000}
        effective_since = since
        if effective_since is None and self.lookback_days:
            effective_since = datetime.now(timezone.utc) - timedelta(days=self.lookback_days)
        records = await self.plex.fetch_paginated(
            "/inventory/v1/inventory-tracking/containers",
            params=params,
            data_key="data",
        )
        if effective_since:
            filtered: list[Dict[str, Any]] = []
            for record in records:
                timestamp = record.get("lastUpdatedDate") or record.get("lastUpdated")
                if not timestamp:
                    filtered.append(record)
                    continue
                try:
                    ts = self.plex._parse_datetime(timestamp)
                except ValueError:
                    filtered.append(record)
                    continue
                if ts >= effective_since:
                    filtered.append(record)
            return filtered
        return records

    def raw_table_name(self) -> str:
        return "inventory_containers"

    def record_key(self, record: Dict[str, Any]) -> str:
        key = record.get("id") or record.get("containerId") or record.get("container")
        if not key:
            key = f"{record.get('partNumber')}-{record.get('locationId')}"
        if not key:
            raise ValueError("Inventory record missing identifier")
        return str(key)

    def transform_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        enriched = dict(record)
        enriched.setdefault("rowKey", self.record_key(record))
        enriched.setdefault("pcn", self.config.plex_customer_id)
        enriched.setdefault("facility", os.getenv("FACILITY_NAME", ""))
        return enriched

    def extractor_metadata(self, record: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "partNumber": record.get("partNumber"),
            "locationId": record.get("locationId"),
            "status": record.get("status"),
        }


if __name__ == "__main__":
    from agents.raw_extractors.base import run_sync

    run_sync(InventoryRawExtractor())
