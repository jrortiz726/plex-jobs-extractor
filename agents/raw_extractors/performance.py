"""Performance RAW extractor."""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Optional

if __package__ in (None, ""):
    sys.path.append(str(Path(__file__).resolve().parents[2]))

from agents.raw_extractors.base import PlexRawExtractor, RawExtractorConfig
from agents.raw_extractors.common import env_int, time_range
from agents.raw_extractors.plex_client import PlexAPIClient


class PerformanceRawExtractor(PlexRawExtractor):
    """Capture Plex production performance summaries."""

    def __init__(self, config: RawExtractorConfig | None = None) -> None:
        super().__init__("performance", config)
        self.lookback_days = env_int("PERFORMANCE_LOOKBACK_DAYS", 7)
        self.plex = PlexAPIClient(
            base_url=self.config.plex_base_url,
            api_key=self.config.plex_api_key,
            customer_id=self.config.plex_customer_id,
            max_retries=self.config.max_retries,
            retry_delay=self.config.retry_delay,
        )

    async def fetch_records(self, since: Optional[datetime]) -> list[Dict[str, Any]]:
        date_from, date_to = time_range(since, max(self.lookback_days, 365))

        entries = await self.plex.fetch_paginated(
            "/production/v1/production-history/production-entries",
            params={
                "beginDate": date_from.isoformat(),
                "endDate": date_to.isoformat(),
                "limit": 1000,
            },
            data_key="data",
        )

        summaries = await self.plex.fetch_paginated(
            "/production/v1-beta1/production-history/production-entries-summary",
            params={
                "beginDate": date_from.isoformat(),
                "endDate": date_to.isoformat(),
                "limit": 1000,
            },
            data_key="data",
        )

        tagged_entries = []
        for entry in entries:
            record = dict(entry)
            record.setdefault("recordType", "entry")
            record.setdefault("rowKey", self._make_entry_key(record))
            tagged_entries.append(record)

        tagged_summaries = []
        for summary in summaries:
            record = dict(summary)
            record.setdefault("recordType", "summary")
            record.setdefault("rowKey", self._make_summary_key(record))
            tagged_summaries.append(record)
        return tagged_entries + tagged_summaries

    def raw_table_name(self) -> str:
        return "performance_summaries"

    def record_key(self, record: Dict[str, Any]) -> str:
        if record.get("recordType") == "summary":
            return record.get("rowKey") or self._make_summary_key(record)
        return record.get("rowKey") or self._make_entry_key(record)

    def _make_entry_key(self, record: Dict[str, Any]) -> str:
        base = record.get("entryId") or record.get("id")
        if base:
            return f"entry:{base}"
        workcenter = record.get("workcenterId") or record.get("workcenter")
        start = record.get("startTime") or record.get("timestamp")
        return f"entry:{workcenter}:{start}"

    def _make_summary_key(self, record: Dict[str, Any]) -> str:
        base = record.get("summaryId") or record.get("id")
        if base:
            return f"summary:{base}"
        workcenter = record.get("workcenterId") or record.get("workcenter")
        start = record.get("startTime") or record.get("timestamp")
        return f"summary:{workcenter}:{start}"

    def transform_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        enriched = dict(record)
        enriched.setdefault("rowKey", record.get("rowKey") or self.record_key(record))
        if record.get("workcenterCode"):
            enriched.setdefault("workcenterCode", record.get("workcenterCode"))
        elif isinstance(record.get("workcenter"), dict):
            wc = record.get("workcenter")
            if wc.get("code"):
                enriched.setdefault("workcenterCode", wc.get("code"))
            if wc.get("id") and not enriched.get("workcenterId"):
                enriched.setdefault("workcenterId", wc.get("id"))
        for ts_field in ("startTime", "endTime"):
            if record.get(ts_field):
                enriched.setdefault(ts_field, record.get(ts_field))
        for num_field in ("goodQuantity", "badQuantity", "totalQuantity", "runTimeHours", "plannedRunTimeHours", "downtimeHours"):
            if record.get(num_field) is not None:
                enriched.setdefault(num_field, record.get(num_field))
        enriched.setdefault("pcn", self.config.plex_customer_id)
        enriched.setdefault("facility", os.getenv("FACILITY_NAME", ""))
        return enriched

    def extractor_metadata(self, record: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "workcenterId": record.get("workcenterId"),
            "oee": record.get("oee"),
            "availability": record.get("availability"),
        }


if __name__ == "__main__":
    from agents.raw_extractors.base import run_sync

    run_sync(PerformanceRawExtractor())
