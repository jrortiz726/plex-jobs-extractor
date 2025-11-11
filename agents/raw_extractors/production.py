"""Production RAW extractor."""

from __future__ import annotations

import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

if __package__ in (None, ""):
    sys.path.append(str(Path(__file__).resolve().parents[2]))

from agents.raw_extractors.base import PlexRawExtractor, RawExtractorConfig
from agents.raw_extractors.common import env_int, time_range
from agents.raw_extractors.plex_client import PlexAPIClient


class ProductionRawExtractor(PlexRawExtractor):
    """Ingest Plex production history entries into CDF RAW."""

    def __init__(self, config: RawExtractorConfig | None = None) -> None:
        super().__init__("production", config)
        self.lookback_days = env_int("PRODUCTION_LOOKBACK_DAYS", 3)
        self.plex = PlexAPIClient(
            base_url=self.config.plex_base_url,
            api_key=self.config.plex_api_key,
            customer_id=self.config.plex_customer_id,
            max_retries=self.config.max_retries,
            retry_delay=self.config.retry_delay,
        )

    async def fetch_records(self, since: Optional[datetime]) -> list[Dict[str, Any]]:
        date_from, date_to = time_range(since, self.lookback_days)
        params = {
            "beginDate": date_from.isoformat(),
            "endDate": date_to.isoformat(),
            "limit": 1000,
        }
        entries = await self.plex.fetch_paginated(
            "/production/v1/production-history/production-entries",
            params=params,
            data_key="data",
        )
        return entries

    def raw_table_name(self) -> str:
        return "production_entries"

    def record_key(self, record: Dict[str, Any]) -> str:
        key = record.get("id") or record.get("entryId")
        if not key:
            key = f"{record.get('workcenterId')}-{record.get('timestamp') or record.get('createdAt')}"
        if not key:
            raise ValueError("Production entry missing identifier")
        return str(key)

    def transform_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        enriched = dict(record)
        enriched.setdefault("rowKey", self.record_key(record))
        job_id = record.get("jobId")
        if job_id:
            enriched.setdefault("jobId", job_id)
            enriched.setdefault(
                "jobExternalId",
                record.get("jobExternalId") or f"{self.config.plex_customer_id}_JOB_{job_id}",
            )
        if "jobNumber" in record:
            enriched.setdefault("jobNumber", record.get("jobNumber"))
        for field in ("workcenterCode", "workcenterName"):
            if record.get(field):
                enriched.setdefault(field, record.get(field))
        if not enriched.get("workcenterCode") and isinstance(record.get("workcenter"), dict):
            wc = record.get("workcenter")
            if wc.get("code"):
                enriched.setdefault("workcenterCode", wc.get("code"))
            if wc.get("name"):
                enriched.setdefault("workcenterName", wc.get("name"))
            if wc.get("id"):
                enriched.setdefault("workcenterId", wc.get("id"))
        enriched.setdefault("status", record.get("status") or record.get("entryStatus"))
        for raw_field, target_field in (
            ("startTime", "startTime"),
            ("endTime", "endTime"),
            ("createdTime", "createdAt"),
            ("completedTime", "completedAt"),
        ):
            if record.get(raw_field):
                enriched.setdefault(target_field, record.get(raw_field))
        for num_field in ("quantityGood", "quantityRejected", "sequenceNumber"):
            if record.get(num_field) is not None:
                enriched.setdefault(num_field, record.get(num_field))
        for ref_field in ("shiftId", "operatorId", "productionLineId"):
            if record.get(ref_field):
                enriched.setdefault(ref_field, record.get(ref_field))
        enriched.setdefault("pcn", self.config.plex_customer_id)
        enriched.setdefault("facility", os.getenv("FACILITY_NAME", ""))
        return enriched

    def extractor_metadata(self, record: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "workcenterId": record.get("workcenterId"),
            "partId": record.get("partId"),
            "quantityProduced": record.get("quantityProduced"),
        }


if __name__ == "__main__":
    from agents.raw_extractors.base import run_sync

    run_sync(ProductionRawExtractor())
