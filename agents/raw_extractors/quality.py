"""Quality RAW extractor using Plex Data Source API."""

from __future__ import annotations

import json
import logging
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

if __package__ in (None, ""):
    sys.path.append(str(Path(__file__).resolve().parents[2]))

from agents.raw_extractors.base import PlexRawExtractor, RawExtractorConfig
from agents.raw_extractors.plex_datasource_client import PlexDataSourceClient

logger = logging.getLogger(__name__)


@dataclass
class DataSourceDefinition:
    id: int
    name: str
    record_type: str
    expects_xml: bool = False


class QualityRawExtractor(PlexRawExtractor):
    """Pull quality data from Plex Data Source API into RAW."""

    def __init__(self, config: RawExtractorConfig | None = None) -> None:
        super().__init__("quality", config)
        self.start_date = self._parse_datetime(os.getenv("QUALITY_EXTRACTION_START_DATE"))
        self.days_back = int(os.getenv("QUALITY_DAYS_BACK", "30"))
        self.batch_size = int(os.getenv("QUALITY_BATCH_SIZE", "1000"))

        ds_host = os.getenv("PLEX_DS_HOST") or os.getenv("PLEX_DATASOURCE_HOST")
        if not ds_host:
            raise ValueError("PLEX_DS_HOST environment variable is required for quality extraction")
        username = os.getenv("PLEX_DS_USERNAME")
        password = os.getenv("PLEX_DS_PASSWORD")
        if not username or not password:
            raise ValueError("PLEX_DS_USERNAME and PLEX_DS_PASSWORD must be configured")

        self.datasource_client = PlexDataSourceClient(
            host=ds_host,
            username=username,
            password=password,
            max_retries=self.config.max_retries,
            retry_delay=self.config.retry_delay,
        )

        self.definitions: List[DataSourceDefinition] = [
            DataSourceDefinition(2199, "Checklist_Overview_Get", "checklist_overview"),
            DataSourceDefinition(17473, "Checksheet_Data_By_Containers_Get", "checksheet_data_by_containers"),
            DataSourceDefinition(81, "Checksheet_Get_Single", "checksheet_single"),
            DataSourceDefinition(30949, "Checksheet_History_Crosstab_Get", "checksheet_history"),
            DataSourceDefinition(2998, "Checksheet_Types_Get", "checksheet_types"),
            DataSourceDefinition(21773, "Checksheet_With_Measurements_Web_Service_Add", "checksheet_measurements", expects_xml=True),
            DataSourceDefinition(4142, "Checksheets_Get", "checksheets"),
            DataSourceDefinition(18718, "Checksheets_With_Job_Get", "checksheets_with_job"),
            DataSourceDefinition(7262, "Control_Plan_Get", "control_plan"),
            DataSourceDefinition(6456, "Defect_Type_Get", "defect_type"),
            DataSourceDefinition(19938, "Problem_Logs_Get", "problem_logs"),
            DataSourceDefinition(2158, "Sample_Plans_Get", "sample_plans"),
            DataSourceDefinition(15387, "Spec_Doc_Get", "spec_doc"),
            DataSourceDefinition(5112, "Specification_Picker_Get", "specification_picker"),
        ]
        self._control_plan_keys: Optional[List[int]] = None

    async def fetch_records(self, since: Optional[datetime]) -> list[Dict[str, Any]]:
        effective_since = since or self._default_since()
        records: List[Dict[str, Any]] = []

        for definition in self.definitions:
            if definition.id == 7262:
                await self._ensure_control_plan_keys()
            input_sets = self._build_inputs(definition)
            if not input_sets:
                continue
            for inputs in input_sets:
                try:
                    response = await self.datasource_client.execute(definition.id, inputs)
                except Exception as exc:
                    logger.warning(
                        "Data source %s (%s) failed: %s",
                        definition.id,
                        definition.name,
                        exc,
                    )
                    continue
                normalized = self._normalize_response(definition, response, inputs, effective_since)
                records.extend(normalized)
        return records

    def raw_table_name(self) -> str:
        return "quality_records"

    def record_key(self, record: Dict[str, Any]) -> str:
        key = record.get("rawKey")
        if not key:
            raise ValueError("Quality record missing rawKey")
        return key

    def transform_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        enriched = dict(record)
        enriched.setdefault("pcn", self.config.plex_customer_id)
        enriched.setdefault("facility", os.getenv("FACILITY_NAME", ""))
        return enriched

    def extractor_metadata(self, record: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "recordType": record.get("recordType"),
            "dataSourceId": record.get("dataSourceId"),
            "inputs": json.dumps(record.get("inputs", {}), default=str),
        }

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _default_since(self) -> datetime:
        if self.start_date:
            return self.start_date
        return datetime.now(timezone.utc) - timedelta(days=self.days_back)

    def _build_inputs(self, definition: DataSourceDefinition) -> List[Dict[str, Any]]:
        if definition.id == 17473:
            return [{"Containers": "", "Specification_Key": 0}]
        if definition.id == 81:
            return [{"Checksheet_No": -1}]
        if definition.id == 2199:
            return [{"Checklist_No": -1}]
        if definition.id == 7262:
            keys = self._control_plan_keys or []
            if not keys:
                logger.warning("No control plan keys discovered; skipping Control_Plan_Get")
                return []
            return [{"Control_Plan_Key": key} for key in keys]
        return [{}]

    def _normalize_response(
        self,
        definition: DataSourceDefinition,
        response: Dict[str, Any],
        inputs: Dict[str, Any],
        since: datetime,
    ) -> List[Dict[str, Any]]:
        records: List[Dict[str, Any]] = []
        transaction = response.get("transactionNo") or "no_transaction"
        row_limited = response.get("rowLimitedExceeded")
        timestamp = datetime.now(timezone.utc)

        tables = response.get("tables") or []
        if tables:
            for table_idx, table in enumerate(tables):
                columns = table.get("columns", [])
                rows = table.get("rows", []) or []
                for row_idx, row in enumerate(rows):
                    row_dict = self._row_to_dict(columns, row)
                    if not self._row_within_window(row_dict, since):
                        continue
                    key = self._make_row_key(
                        definition.record_type,
                        definition.id,
                        transaction,
                        table_idx,
                        row_idx,
                    )
                    record = {
                        "rawKey": key,
                        "recordType": definition.record_type,
                        "dataSourceId": definition.id,
                        "dataSourceName": definition.name,
                        "tableIndex": table_idx,
                        "rowIndex": row_idx,
                        "transactionNo": transaction,
                        "rowLimitedExceeded": row_limited,
                        "inputs": inputs,
                        "timestamp": timestamp.isoformat(),
                    }
                    record.update(row_dict)
                    records.append(record)
        else:
            # No tabular data; capture outputs or raw payload
            payload = response.get("outputs") or response.get("raw")
            if payload is None and response:
                payload = response
            key = self._make_row_key(
                definition.record_type,
                definition.id,
                transaction,
                -1,
                0,
            )
            record = {
                "rawKey": key,
                "recordType": definition.record_type,
                "dataSourceId": definition.id,
                "dataSourceName": definition.name,
                "tableIndex": -1,
                "rowIndex": 0,
                "transactionNo": transaction,
                "rowLimitedExceeded": row_limited,
                "inputs": inputs,
                "timestamp": timestamp.isoformat(),
            }
            if isinstance(payload, dict):
                record.update(payload)
            else:
                record["rawPayload"] = payload
            records.append(record)
        return records

    def _row_to_dict(self, columns: Iterable[str], row: Iterable[Any]) -> Dict[str, Any]:
        row_dict: Dict[str, Any] = {}
        for idx, column in enumerate(columns):
            try:
                row_dict[column] = row[idx]
            except IndexError:
                row_dict[column] = None
        return row_dict

    def _row_within_window(self, row: Dict[str, Any], since: datetime) -> bool:
        for key, value in row.items():
            if not isinstance(value, str):
                continue
            lowered = key.lower()
            if any(token in lowered for token in ("date", "time")):
                try:
                    parsed = self._parse_datetime(value)
                except ValueError:
                    continue
                if parsed and parsed >= since:
                    return True
        # If no datetime fields detected, include the row
        return True

    def _make_row_key(
        self,
        record_type: str,
        datasource_id: int,
        transaction: str,
        table_idx: int,
        row_idx: int,
    ) -> str:
        safe_tx = transaction.replace(":", "-") if transaction else "no_transaction"
        return f"{record_type}:{datasource_id}:{safe_tx}:{table_idx}:{row_idx}"

    def _parse_datetime(self, value: Optional[str]) -> Optional[datetime]:
        if not value:
            return None
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None

    def _parse_int_list(self, env_name: str) -> List[int]:
        raw = os.getenv(env_name)
        if not raw:
            return []
        values: List[int] = []
        for item in raw.split(","):
            item = item.strip()
            if not item:
                continue
            try:
                values.append(int(item))
            except ValueError:
                logger.warning("Invalid integer '%s' in %s", item, env_name)
        return values

    def _parse_str_list(self, env_name: str) -> List[str]:
        raw = os.getenv(env_name)
        if not raw:
            return []
        return [item.strip() for item in raw.split(",") if item.strip()]

    async def _ensure_control_plan_keys(self) -> None:
        if self._control_plan_keys is not None:
            return
        try:
            response = await self.datasource_client.execute(17981, {"RowLimit": self.batch_size})
        except Exception as exc:
            logger.warning("Failed to collect control plan keys: %s", exc)
            self._control_plan_keys = []
            return
        tables = response.get("tables") or []
        keys: List[int] = []
        for table in tables:
            columns = table.get("columns", [])
            rows = table.get("rows", []) or []
            for row in rows:
                row_dict = self._row_to_dict(columns, row)
                key = row_dict.get("Control_Plan_Key")
                if isinstance(key, int):
                    keys.append(key)
        self._control_plan_keys = sorted(set(keys))


if __name__ == "__main__":
    from agents.raw_extractors.base import run_sync

    run_sync(QualityRawExtractor())
