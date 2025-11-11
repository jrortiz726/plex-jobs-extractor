"""Master data RAW extractor."""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

if __package__ in (None, ""):
    sys.path.append(str(Path(__file__).resolve().parents[2]))

from agents.raw_extractors.base import PlexRawExtractor, RawExtractorConfig
from agents.raw_extractors.plex_client import PlexAPIClient


@dataclass(frozen=True)
class EndpointConfig:
    record_type: str
    endpoint: str
    id_fields: tuple[str, ...]
    timestamp_field: str | None = "lastUpdated"


ENDPOINTS: tuple[EndpointConfig, ...] = (
    EndpointConfig(
        record_type="workcenter",
        endpoint="/production/v1/production-definitions/workcenters",
        id_fields=("id", "workcenterId", "externalId"),
        timestamp_field="lastUpdated",
    ),
    EndpointConfig(
        record_type="part",
        endpoint="/mdm/v1/parts",
        id_fields=("id", "partId", "partNumber"),
        timestamp_field="lastUpdatedDate",
    ),
    EndpointConfig(
        record_type="operation",
        endpoint="/mdm/v1/operations",
        id_fields=("id", "operationId"),
        timestamp_field="lastUpdatedDate",
    ),
)


class MasterDataRawExtractor(PlexRawExtractor):
    """Collect core Plex master data into RAW tables."""

    def __init__(self, config: RawExtractorConfig | None = None) -> None:
        super().__init__("master_data", config)
        self.lookback_days = int(os.getenv("MASTER_LOOKBACK_DAYS", "30"))
        self.plex = PlexAPIClient(
            base_url=self.config.plex_base_url,
            api_key=self.config.plex_api_key,
            customer_id=self.config.plex_customer_id,
            max_retries=self.config.max_retries,
            retry_delay=self.config.retry_delay,
        )

    async def fetch_records(self, since: Optional[datetime]) -> list[Dict[str, Any]]:
        effective_since = since
        if effective_since is None:
            effective_since = datetime.now(timezone.utc) - timedelta(days=self.lookback_days)

        aggregated: List[Dict[str, Any]] = []
        for endpoint in ENDPOINTS:
            payload = await self.plex.fetch_paginated(endpoint.endpoint, params={"limit": 1000}, data_key="data")
            for item in payload:
                record = dict(item)
                record["recordType"] = endpoint.record_type
                if self._is_after(record, endpoint.timestamp_field, effective_since):
                    key = self.record_key(record)
                    record.setdefault("rowKey", key)
                    aggregated.append(record)
        return aggregated

    def raw_table_name(self) -> str:
        return "master_data"

    def record_key(self, record: Dict[str, Any]) -> str:
        record_type = record.get("recordType", "unknown")
        for field in next((cfg.id_fields for cfg in ENDPOINTS if cfg.record_type == record_type), ("id",)):
            key = record.get(field)
            if key:
                return f"{record_type}:{key}"
        raise ValueError(f"Master data record missing identifier for type {record_type}")

    def transform_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        enriched = dict(record)
        enriched.setdefault("rowKey", self.record_key(record))
        enriched.setdefault("pcn", self.config.plex_customer_id)
        enriched.setdefault("facility", os.getenv("FACILITY_NAME", ""))
        return enriched

    def extractor_metadata(self, record: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "recordType": record.get("recordType"),
            "name": record.get("name") or record.get("description"),
        }

    def _is_after(self, record: Dict[str, Any], field: Optional[str], since: Optional[datetime]) -> bool:
        if since is None or field is None:
            return True
        timestamp = record.get(field) or record.get("lastUpdated")
        if not timestamp:
            return True
        try:
            ts = self.plex._parse_datetime(timestamp)
        except ValueError:
            return True
        return ts >= since


if __name__ == "__main__":
    from agents.raw_extractors.base import run_sync

    run_sync(MasterDataRawExtractor())
