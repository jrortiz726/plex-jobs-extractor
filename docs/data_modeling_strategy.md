# Plex RAW to Data Model Strategy

## Overview
The RAW extractor suite under `agents/raw_extractors/` complements the existing standalone extractors by landing Plex data directly into CDF RAW tables that provide a stable staging layer for data modeling. Each extractor subclasses the shared `PlexRawExtractor` which establishes CDF connectivity, maintains incremental state, and writes ingestion metadata through the extractor extensions data model.

## RAW Ingestion Flow
1. `PlexRawExtractor` loads credentials from `.env`, ensures the `PLEX_RAW_DATABASE` and per-domain tables exist, and executes the subclass `fetch_records()` coroutine.
2. Records are serialized into `RawRow` objects with deterministic keys (e.g., `jobs`, `production_entries`, `inventory_containers`). Complex payloads are JSON-stringified to preserve the original field structure.
3. After a successful insert, extraction metadata is recorded as `CogniteExtractorData` nodes inside `PLEX_EXTRACTOR_SPACE` so orchestrators can correlate RAW rows with downstream modeling or replay requirements.

## Plex Domain Coverage
- `jobs.py`: `/scheduling/v1/jobs` with configurable lookback (`PLEX_JOBS_LOOKBACK_DAYS`), capturing job schedules, operations, and statuses.
- `production.py`: `/production/v1/production-history/production-entries` for execution metrics.
- `inventory.py`: `/inventory/v1/containers` producing container-level stock snapshots.
- `quality.py`: `/quality/v1/inspections`, `/quality/v1/defects`, and `/quality/v1/quality-checks` unified into a single RAW table.
- `performance.py`: `/production/v1/production-history/production-summaries` for OEE-style performance indicators.
- `master_data.py`: `/production/v1/production-definitions/workcenters` and core MDM endpoints for contextual entities.

## Data Modeling Setup
`agents/data_modeling/plex_data_models.py` applies a dedicated data-modeling space (`PLEX_DM_SPACE`) with containers, views, and a composite data model (`plex_operational_model`). Mapped properties expose curated fields for analytics-ready nodes while retaining PCN scoping and now include a dedicated operations container/view linked to jobs. Run `python -m agents.data_modeling.plex_data_models` (after configuring `.env`) to apply the model so it references the RAW tables created by the extractors, keeping extractor extensions and data modeling pipelines synchronized.
