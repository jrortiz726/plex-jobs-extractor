"""Jobs RAW extractor."""

from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

if __package__ in (None, ""):
    sys.path.append(str(Path(__file__).resolve().parents[2]))

from agents.raw_extractors.base import PlexRawExtractor, RawExtractorConfig
from agents.raw_extractors.common import env_int, time_range
from agents.raw_extractors.plex_client import PlexAPIClient


class JobsRawExtractor(PlexRawExtractor):
    """Ingest Plex scheduling jobs into CDF RAW."""

    def __init__(self, config: RawExtractorConfig | None = None) -> None:
        super().__init__("jobs", config)
        self.lookback_days = env_int("PLEX_JOBS_LOOKBACK_DAYS", 7)
        self.client_api = PlexAPIClient(
            base_url=self.config.plex_base_url,
            api_key=self.config.plex_api_key,
            customer_id=self.config.plex_customer_id,
            max_retries=self.config.max_retries,
            retry_delay=self.config.retry_delay,
        )

    async def fetch_records(self, since: Optional[datetime]) -> list[Dict[str, Any]]:
        date_from, now = time_range(since, self.lookback_days)
        params = {
            "dateFrom": date_from.isoformat(),
            "dateTo": now.isoformat(),
            "limit": 1000,
        }
        jobs = await self.client_api.fetch_paginated("/scheduling/v1/jobs", params=params, data_key="data")
        enriched: list[Dict[str, Any]] = []
        for job in jobs:
            job_id = job.get("id") or job.get("jobId")
            if job_id:
                operations = await self._fetch_job_operations(job_id)
                if operations:
                    job = dict(job)
                    job["operations"] = operations
            enriched.append(job)
        return enriched

    def raw_table_name(self) -> str:
        return "jobs"

    def record_key(self, record: Dict[str, Any]) -> str:
        key = record.get("id") or record.get("jobId")
        if not key:
            key = f"{record.get('job_no') or record.get('jobNo')}-{record.get('scheduleStartDate') or record.get('scheduledStart')}"
        if not key:
            raise ValueError("Job record missing identifier")
        return str(key)

    def transform_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        enriched = dict(record)
        key = self.record_key(record)
        enriched.setdefault("rowKey", key)
        enriched.setdefault("externalId", key)

        workcenter_value = self._extract_workcenter(record)
        if workcenter_value:
            enriched.setdefault("workcenter", workcenter_value)
        workcenter_code = self._extract_workcenter_field(record, "code")
        workcenter_id = self._extract_workcenter_field(record, "id")
        workcenter_name = self._extract_workcenter_field(record, "name")
        if workcenter_code:
            enriched.setdefault("workcenterCode", workcenter_code)
        if workcenter_id:
            enriched.setdefault("workcenterId", workcenter_id)
        if workcenter_name:
            enriched.setdefault("workcenterName", workcenter_name)

        operations = enriched.get("operations")
        if operations:
            enriched.setdefault("operationCount", len(operations))
            enriched.setdefault("primaryOperation", operations[0])
        enriched.setdefault("pcn", self.config.plex_customer_id)
        enriched.setdefault("facility", os.getenv("FACILITY_NAME", ""))
        return enriched

    def _extract_workcenter(self, record: Dict[str, Any]) -> Optional[str]:
        for key in ("workcenter", "workcenterCode", "workcenterId", "workcenterName"):
            value = record.get(key)
            if isinstance(value, str) and value.strip():
                return value
            if isinstance(value, (int, float)):
                return str(value)
        nested = record.get("workcenter")
        if isinstance(nested, dict):
            for nested_key in ("code", "id", "name"):
                value = nested.get(nested_key)
                if value:
                    return str(value)
        operations = record.get("operations")
        if isinstance(operations, list):
            for op in operations:
                if not isinstance(op, dict):
                    continue
                for key in ("workcenterCode", "workcenterId", "workcenterName"):
                    value = op.get(key)
                    if value:
                        return str(value)
        return None

    def _extract_workcenter_field(self, record: Dict[str, Any], field: str) -> Optional[str]:
        nested = record.get("workcenter")
        if isinstance(nested, dict) and nested.get(field):
            return str(nested.get(field))
        direct_key = f"workcenter{field.capitalize()}"
        value = record.get(direct_key)
        if value:
            return str(value)
        operations = record.get("operations")
        if isinstance(operations, list):
            for op in operations:
                if not isinstance(op, dict):
                    continue
                value = op.get(f"workcenter{field.capitalize()}")
                if value:
                    return str(value)
        return None

    async def _fetch_job_operations(self, job_id: str) -> Optional[List[Dict[str, Any]]]:
        try:
            response = await self.client_api.get(f"/scheduling/v1/jobs/{job_id}/operations")
        except Exception as exc:
            return None

        if isinstance(response, dict) and "data" in response:
            data = response.get("data")
        else:
            data = response

        if isinstance(data, list):
            return data

        return None

    def extractor_metadata(self, record: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "jobNumber": record.get("job_no") or record.get("jobNo") or record.get("id"),
            "status": record.get("status"),
            "workcenter": record.get("workcenterCode") or record.get("workcenterId"),
        }


if __name__ == "__main__":
    from agents.raw_extractors.base import run_sync

    run_sync(JobsRawExtractor())
