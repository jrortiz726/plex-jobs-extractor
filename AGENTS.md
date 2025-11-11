# Repository Guidelines

## Project Structure & Module Organization
- Root extractor modules (e.g., `jobs_extractor.py`, `master_data_extractor.py`) pair with enhanced variants; orchestrators live in `orchestrator*.py`.
- Supporting tooling sits in `deploy/` (deployment scripts), `docs/` (detailed architecture/reference), `state/` (run-time checkpoints), and `workflow/` (automation assets).
- Config templates such as `plex-unified-extractor-config.yml` and helper scripts like `setup_datasets.py` and `start.sh` sit at the top level for quick discovery.
- The `agents/` folder hosts RAW extractors (`agents/raw_extractors/`) and data modeling helpers (`agents/data_modeling/`) that mirror the standalone scripts.
- Tests are Python scripts named `test_*.py` alongside the code; treat them as executable smoke tests rather than a separate package.

## Build, Test, and Development Commands
- `python -m venv .venv && source .venv/bin/activate` to work inside an isolated environment.
- `pip install -r requirements.txt` installs runtime dependencies for the extractors and orchestration tools.
- `python setup_datasets.py` provisions the target CDF datasets once the `.env` secrets are loaded.
- `python orchestrator.py --mode continuous` runs the main orchestrator; `./start.sh` launches the interactive helper; `docker-compose up -d` mirrors production locally.
- `python run_all_raw.py` executes the RAW extractors once (add `--interval` to loop continuously).

## Coding Style & Naming Conventions
- Write Python 3.9+ code following PEP 8 (4-space indents, snake_case modules and functions, UpperCamelCase classes).
- Mirror existing modules by using dataclasses for configs, explicit type hints, and top-level docstrings summarizing each file.
- Prefer structured logging via the standard `logging` module and reuse the logger pattern from `orchestrator.py`.

## Testing Guidelines
- Execute scenario scripts directly (`python test_enhanced.py`, `python test_orchestrator.py`) after exporting the same env vars the orchestrator uses.
- Review `extraction_log.txt` and `orchestrator.log` to confirm success paths.
- When adding new extractors, include a dedicated `test_<feature>.py` that exercises the `.extract()` coroutine at least once.

## Commit & Pull Request Guidelines
- Follow the existing short, lowercase summaries from `git log` (e.g., `added CDF function logging`); keep subject lines under 72 characters.
- Reference related docs or scripts updated in the body and note required env changes.
- For PRs, include: purpose, execution commands run, before/after behavior, and screenshots for UI or dashboard changes.
- Link to tracking issues or deployment tickets and request review from the extractor owners listed in `docs/quick-reference.md`.

## Configuration & Security Tips
- Never commit `.env` or credential files; rely on `dotenv` loading during local runs.
- Validate new endpoints against `docs/plex-api-reference.md` before rollout to avoid breaking Plex throttling limits.
- Quality Data Source extraction expects `PLEX_DS_HOST`, `PLEX_DS_USERNAME`, and `PLEX_DS_PASSWORD` along with per-source inputs (e.g., `QUALITY_DS_2199_CHECKLIST_NO`, `QUALITY_DS_17473_CONTAINERS`).
