"""Run all Plex RAW extractors with optional scheduling."""

from __future__ import annotations

import argparse
import logging
import os
import time
from pathlib import Path
from typing import Dict, List, Sequence, Type

try:
    from dotenv import load_dotenv  # type: ignore
except ImportError:  # pragma: no cover - optional dependency
    load_dotenv = None

from agents.raw_extractors import (
    InventoryRawExtractor,
    JobsRawExtractor,
    MasterDataRawExtractor,
    PerformanceRawExtractor,
    ProductionRawExtractor,
    QualityRawExtractor,
)
from agents.raw_extractors.base import PlexRawExtractor, run_sync


def _load_env() -> None:
    env_path = Path(".env")
    if load_dotenv is not None:
        load_dotenv(dotenv_path=env_path if env_path.exists() else None)
        return

    if not env_path.exists():
        return

    for line in env_path.read_text().splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())


def _available_extractors() -> Dict[str, Type[PlexRawExtractor]]:
    return {
        "jobs": JobsRawExtractor,
        "production": ProductionRawExtractor,
        "inventory": InventoryRawExtractor,
        "performance": PerformanceRawExtractor,
        "quality": QualityRawExtractor,
        "master_data": MasterDataRawExtractor,
    }


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Plex RAW extractors")
    parser.add_argument(
        "--extractors",
        nargs="+",
        choices=sorted(_available_extractors().keys()),
        help="Subset of extractors to run (defaults to all)",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=0,
        help="Sleep seconds between iterations (0 => run once)",
    )
    parser.add_argument(
        "--max-iterations",
        type=int,
        default=1,
        help="How many iterations to run (ignored when --interval=0)",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Logging level",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    )

    _load_env()

    registry = _available_extractors()
    selected_names: List[str]
    if args.extractors:
        selected_names = list(dict.fromkeys(args.extractors))
    else:
        selected_names = list(sorted(registry.keys()))

    logging.info("Running RAW extractors: %s", ", ".join(selected_names))

    iteration = 0
    interval = max(args.interval, 0)
    max_iterations = args.max_iterations if interval > 0 else 1

    while True:
        iteration += 1
        logging.info("Starting iteration %s", iteration)

        for name in selected_names:
            extractor_cls = registry[name]
            logging.info("Launching extractor '%s'", name)
            extractor: PlexRawExtractor = extractor_cls()
            try:
                result = run_sync(extractor)
                logging.info(
                    "Extractor '%s' finished: rows_written=%s last_timestamp=%s",
                    name,
                    result.get("rows_written"),
                    result.get("last_timestamp"),
                )
            except Exception as exc:  # pragma: no cover - operational logging
                logging.exception("Extractor '%s' failed: %s", name, exc)
                continue

        if interval <= 0 or iteration >= max_iterations:
            break

        logging.info("Iteration %s complete; sleeping %s seconds", iteration, interval)
        time.sleep(interval)

    logging.info("RAW extraction run complete")


if __name__ == "__main__":
    main()
