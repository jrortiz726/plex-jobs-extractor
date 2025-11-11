"""Base utilities for exporting Plex data into CDF RAW tables.

The raw extractor pipeline keeps the existing Plex connectivity patterns but
routes records into purpose-built RAW tables that downstream data models can
consume. Each extractor subclass is responsible for fetching data from Plex
and providing a deterministic RAW row key along with any record-level metadata
that should be preserved for modeling.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence

from dotenv import load_dotenv
from cognite.client import ClientConfig, CogniteClient
from cognite.client.credentials import OAuthClientCredentials
from cognite.client.data_classes.raw import RowWrite
from cognite.client.data_classes.data_modeling.extractor_extensions.v1 import (
    CogniteExtractorDataApply,
)
from cognite.client.data_classes.data_modeling.spaces import SpaceApply
from cognite.client.exceptions import CogniteAPIError

from cdf_utils import StateTracker

logger = logging.getLogger(__name__)

if not logging.getLogger().handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    )


@dataclass
class RawExtractorConfig:
    """Settings shared across RAW extractors."""

    plex_api_key: str
    plex_customer_id: str
    cdf_host: str
    cdf_project: str
    cdf_client_id: str
    cdf_client_secret: str
    cdf_token_url: str
    plex_base_url: str = "https://connect.plex.com"
    raw_database: str = "plex_raw"
    extractor_space: str = "plex_extractor_runs"
    state_directory: str = "state"
    batch_size: int = 1000
    max_retries: int = 3
    retry_delay: int = 5

    @classmethod
    def from_env(cls) -> "RawExtractorConfig":
        """Load configuration from environment variables/.env."""

        load_dotenv()

        def _require(name: str) -> str:
            value = os.getenv(name)
            if not value:
                raise ValueError(f"Missing required environment variable: {name}")
            return value

        return cls(
            plex_api_key=_require("PLEX_API_KEY"),
            plex_customer_id=_require("PLEX_CUSTOMER_ID"),
            cdf_host=_require("CDF_HOST"),
            cdf_project=_require("CDF_PROJECT"),
            cdf_client_id=_require("CDF_CLIENT_ID"),
            cdf_client_secret=_require("CDF_CLIENT_SECRET"),
            cdf_token_url=_require("CDF_TOKEN_URL"),
            plex_base_url=os.getenv("PLEX_BASE_URL", "https://connect.plex.com"),
            raw_database=os.getenv("PLEX_RAW_DATABASE", "plex_raw"),
            extractor_space=os.getenv("PLEX_EXTRACTOR_SPACE", "plex_extractor_runs"),
            state_directory=os.getenv("PLEX_STATE_DIR", "state"),
            batch_size=int(os.getenv("PLEX_BATCH_SIZE", "1000")),
            max_retries=int(os.getenv("PLEX_MAX_RETRIES", "3")),
            retry_delay=int(os.getenv("PLEX_RETRY_DELAY", "5")),
        )


class PlexRawExtractor(ABC):
    """Abstract base class for RAW-focused extractors."""

    def __init__(self, extractor_name: str, config: RawExtractorConfig | None = None) -> None:
        self.extractor_name = extractor_name
        self.config = config or RawExtractorConfig.from_env()
        self.client = self._init_cognite_client()
        self.state_tracker = self._init_state_tracker()
        self._ensure_state_dir()
        logger.info("Initialized RAW extractor '%s' targeting database '%s'", extractor_name, self.config.raw_database)

    # ------------------------------------------------------------------
    # Abstract hooks for subclasses
    # ------------------------------------------------------------------
    @abstractmethod
    async def fetch_records(self, since: Optional[datetime]) -> Sequence[Dict[str, Any]]:
        """Fetch records from Plex. Implement pagination/incremental logic here."""

    @abstractmethod
    def raw_table_name(self) -> str:
        """Return the RAW table name specific to the extractor."""

    @abstractmethod
    def record_key(self, record: Dict[str, Any]) -> str:
        """Produce a deterministic RAW row key for the given record."""

    # ------------------------------------------------------------------
    # Optional customisation points
    # ------------------------------------------------------------------
    def transform_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Sanitize/shape the record before writing to RAW."""
        return record

    def extractor_metadata(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Additional metadata stored alongside extractor extension nodes."""
        return {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    async def run(self) -> Dict[str, Any]:
        """Execute one incremental extraction and RAW upsert."""

        since = self.state_tracker.get_last_extraction_time(self.extractor_name)
        logger.info("Starting RAW extraction for %s; since=%s", self.extractor_name, since)
        try:
            records = await self.fetch_records(since)
        except Exception as exc:
            logger.exception("%s fetch failed", self.extractor_name)
            raise

        logger.info("Fetched %d records for %s", len(records), self.extractor_name)
        if not records:
            logger.info("No records to ingest for %s", self.extractor_name)
            return {"rows_written": 0, "last_timestamp": since}

        transformed = [self.transform_record(r) for r in records]
        formatted_rows: List[RowWrite] = []
        for rec in transformed:
            if not rec:
                continue
            try:
                formatted_rows.append(self._to_raw_row(rec))
            except ValueError as exc:
                logger.warning("Skipping record without stable key in %s: %s", self.extractor_name, exc)
        if not formatted_rows:
            logger.info("All records filtered out for %s", self.extractor_name)
            return {"rows_written": 0, "last_timestamp": since}

        database = self.config.raw_database
        table = self.raw_table_name()
        self._ensure_raw_destination(database, table)
        total_rows = self._insert_raw_rows(database, table, formatted_rows)
        logger.info("Inserted %s rows into %s.%s", total_rows, database, table)

        latest_timestamp = self._resolve_last_timestamp(transformed)
        if latest_timestamp:
            self.state_tracker.set_last_extraction_time(self.extractor_name, latest_timestamp)

        self._upsert_extractor_metadata(formatted_rows)

        return {"rows_written": total_rows, "last_timestamp": latest_timestamp}

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _init_cognite_client(self) -> CogniteClient:
        creds = OAuthClientCredentials(
            token_url=self.config.cdf_token_url,
            client_id=self.config.cdf_client_id,
            client_secret=self.config.cdf_client_secret,
            scopes=["user_impersonation"],
        )

        client_config = ClientConfig(
            client_name=f"plex_raw_{self.extractor_name}",
            base_url=self.config.cdf_host,
            project=self.config.cdf_project,
            credentials=creds,
        )
        return CogniteClient(client_config)

    def _init_state_tracker(self) -> StateTracker:
        state_dir = Path(self.config.state_directory)
        state_dir.mkdir(parents=True, exist_ok=True)
        state_path = state_dir / f"{self.extractor_name}_raw_state.json"
        return StateTracker(str(state_path))

    def _ensure_state_dir(self) -> None:
        Path(self.config.state_directory).mkdir(parents=True, exist_ok=True)

    def _ensure_raw_destination(self, database: str, table: str) -> None:
        if database not in getattr(self, "_ensured_dbs", set()):
            try:
                self.client.raw.databases.create(database)
                logger.info("Created RAW database %s", database)
            except CogniteAPIError as exc:
                if exc.code not in (409, 400):
                    raise
            self._ensured_dbs = getattr(self, "_ensured_dbs", set()) | {database}

        ensured_tables = getattr(self, "_ensured_tables", set())
        table_key = f"{database}:{table}"
        if table_key in ensured_tables:
            return
        try:
            self.client.raw.tables.create(database, table)
            logger.info("Created RAW table %s.%s", database, table)
        except CogniteAPIError as exc:
            if exc.code not in (409, 400):
                raise
        ensured_tables.add(table_key)
        self._ensured_tables = ensured_tables

    def _insert_raw_rows(self, database: str, table: str, rows: Sequence[RowWrite]) -> int:
        total = 0
        for chunk in self._chunk(rows, self.config.batch_size):
            self.client.raw.rows.insert(database, table, chunk)
            total += len(chunk)
        return total

    def _to_raw_row(self, record: Dict[str, Any]) -> RowWrite:
        key = self.record_key(record)
        prepared = self._stringify_nested(record)
        return RowWrite(key=key, columns=prepared)

    def _resolve_last_timestamp(self, records: Sequence[Dict[str, Any]]) -> Optional[datetime]:
        timestamps: List[datetime] = []
        for record in records:
            for candidate in (record.get("lastUpdated"), record.get("updated_at"), record.get("updatedAt"), record.get("timestamp")):
                if isinstance(candidate, datetime):
                    timestamps.append(candidate)
                elif isinstance(candidate, str):
                    try:
                        timestamps.append(datetime.fromisoformat(candidate))
                    except ValueError:
                        continue
        return max(timestamps) if timestamps else datetime.now(timezone.utc)

    def _chunk(self, items: Sequence[Any], size: int) -> Iterable[Sequence[Any]]:
        for idx in range(0, len(items), size):
            yield items[idx : idx + size]

    def _stringify_nested(self, data: Dict[str, Any]) -> Dict[str, Any]:
        prepared: Dict[str, Any] = {}
        for key, value in data.items():
            if isinstance(value, (dict, list, tuple)):
                prepared[key] = json.dumps(value, default=str)
            elif isinstance(value, datetime):
                prepared[key] = value.isoformat()
            else:
                prepared[key] = value
        return prepared

    def _ensure_extractor_space(self) -> None:
        if not self.config.extractor_space:
            return
        try:
            self.client.data_modeling.spaces.retrieve(self.config.extractor_space)
        except CogniteNotFoundError:
            logger.info("Creating extractor space %s", self.config.extractor_space)
            self.client.data_modeling.spaces.apply(
                SpaceApply(
                    space=self.config.extractor_space,
                    name="Plex Extractor Runs",
                    description="Operational nodes that reference RAW rows produced by Plex extractors.",
                )
            )
        except CogniteAPIError as exc:
            if exc.code not in (409, 400):
                raise

    def _upsert_extractor_metadata(self, rows: Sequence[RowWrite]) -> None:
        if not rows or not self.config.extractor_space:
            return
        if getattr(self, "_suppress_extractor_metadata", False):
            return

        try:
            self._ensure_extractor_space()
        except CogniteAPIError as exc:
            logger.warning("Skipping extractor extension upsert: %s", exc)
            self._suppress_extractor_metadata = True
            return

        nodes: List[CogniteExtractorDataApply] = []
        for row in rows:
            metadata = {
                "rawDatabase": self.config.raw_database,
                "rawTable": self.raw_table_name(),
                "rawKey": row.key,
                "extractor": self.extractor_name,
                "ingestedAt": datetime.now(timezone.utc).isoformat(),
            }
            metadata.update(self.extractor_metadata(row.columns or {}))
            nodes.append(
                CogniteExtractorDataApply(
                    space=self.config.extractor_space,
                    external_id=f"{self.extractor_name}:{row.key}",
                    extracted_data=metadata,
                )
            )

        for chunk in self._chunk(nodes, 500):
            try:
                self.client.data_modeling.instances.apply(nodes=chunk)
            except CogniteAPIError as exc:
                logger.warning("Extractor metadata write failed; disabling metadata sync: %s", exc)
                self._suppress_extractor_metadata = True
                break


async def run_extractor(extractor: PlexRawExtractor) -> Dict[str, Any]:
    """Convenience runner to execute an extractor asynchronously."""
    return await extractor.run()


def run_sync(extractor: PlexRawExtractor) -> Dict[str, Any]:
    """Sync wrapper for environments without asyncio orchestration."""
    return asyncio.run(run_extractor(extractor))
