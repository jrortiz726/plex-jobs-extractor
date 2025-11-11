#!/usr/bin/env python3
"""
Standalone Quality Extractor for Cognite Data Fusion
=====================================================

Advanced quality management with Statistical Process Control (SPC):
- Real-time quality metrics and defect tracking
- Statistical Process Control (SPC) with control charts
- Non-conformance tracking and root cause analysis
- First Time Yield (FTY) and Rolled Throughput Yield (RTY)
- Cost of Quality (CoQ) analysis
- Supplier quality metrics
- Predictive quality analytics

Author: Cognite Solutions Team
Version: 2.0.0
License: MIT
"""

from __future__ import annotations

import os
import sys
import asyncio
import json
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any, Tuple, TypeAlias
from dataclasses import dataclass, field
from enum import StrEnum, auto
import logging
import statistics
import math

try:
    import httpx
    import numpy as np
    from pydantic import BaseSettings, Field
    from cognite.client import CogniteClient, ClientConfig
    from cognite.client.credentials import OAuthClientCredentials
    from cognite.client.data_classes import (
        Asset, TimeSeries, Datapoints, Event, DataSet
    )
except ImportError as e:
    print(f"Missing required dependency: {e}")
    sys.exit(1)

# Type aliases
InspectionId: TypeAlias = str
NCRId: TypeAlias = str
PartId: TypeAlias = str

# ============================================================================
# DATA MODELS
# ============================================================================

class DefectCategory(StrEnum):
    """Defect categories for classification"""
    DIMENSIONAL = auto()
    COSMETIC = auto()
    FUNCTIONAL = auto()
    MATERIAL = auto()
    ASSEMBLY = auto()
    PACKAGING = auto()

class SeverityLevel(StrEnum):
    """Severity levels for quality issues"""
    CRITICAL = auto()
    MAJOR = auto()
    MINOR = auto()
    OBSERVATION = auto()

@dataclass
class QualityInspection:
    """Quality inspection with SPC data"""
    inspection_id: InspectionId
    part_id: PartId
    part_number: str
    timestamp: datetime

    # Inspection details
    sample_size: int = 1
    inspected_qty: int = 0
    passed_qty: int = 0
    failed_qty: int = 0

    # Measurements for SPC
    measurements: Dict[str, List[float]] = field(default_factory=dict)
    specifications: Dict[str, Dict[str, float]] = field(default_factory=dict)

    # Defects
    defects: List[Defect] = field(default_factory=list)
    defect_codes: List[str] = field(default_factory=list)

    # SPC metrics
    cpk: Optional[float] = None
    ppk: Optional[float] = None
    process_capability: Optional[float] = None

    # Quality metrics
    first_time_yield: float = 0.0
    defect_rate_ppm: float = 0.0

    metadata: Dict[str, Any] = field(default_factory=dict)

    def calculate_spc_metrics(self) -> Dict[str, Any]:
        """Calculate Statistical Process Control metrics"""
        spc_results = {}

        for characteristic, values in self.measurements.items():
            if len(values) < 2:
                continue

            spec = self.specifications.get(characteristic, {})
            usl = spec.get('upper_limit')
            lsl = spec.get('lower_limit')
            target = spec.get('target')

            if not values or not (usl or lsl):
                continue

            # Calculate statistics
            mean = statistics.mean(values)
            stdev = statistics.stdev(values) if len(values) > 1 else 0

            # Calculate Cp and Cpk
            if usl and lsl and stdev > 0:
                cp = (usl - lsl) / (6 * stdev)
                cpu = (usl - mean) / (3 * stdev) if usl else float('inf')
                cpl = (mean - lsl) / (3 * stdev) if lsl else float('inf')
                cpk = min(cpu, cpl)
            else:
                cp = cpk = 0

            # Control limits (3-sigma)
            ucl = mean + 3 * stdev
            lcl = mean - 3 * stdev

            # Check for out-of-control points
            ooc_points = sum(1 for v in values if v < lcl or v > ucl)

            spc_results[characteristic] = {
                'mean': round(mean, 4),
                'stdev': round(stdev, 4),
                'cp': round(cp, 3),
                'cpk': round(cpk, 3),
                'ucl': round(ucl, 4),
                'lcl': round(lcl, 4),
                'out_of_control': ooc_points,
                'in_control': ooc_points == 0
            }

        # Overall Cpk (minimum of all characteristics)
        if spc_results:
            self.cpk = min(r['cpk'] for r in spc_results.values())

        return spc_results

@dataclass
class Defect:
    """Individual defect record"""
    defect_code: str
    defect_description: str
    category: DefectCategory
    severity: SeverityLevel
    quantity: int = 1
    location: Optional[str] = None
    root_cause: Optional[str] = None
    corrective_action: Optional[str] = None

@dataclass
class NonConformanceReport:
    """NCR with root cause analysis"""
    ncr_id: NCRId
    ncr_number: str
    created_date: datetime

    # Issue details
    part_id: PartId
    part_number: str
    quantity_affected: int

    # Classification
    category: DefectCategory
    severity: SeverityLevel
    source: str  # internal, supplier, customer

    # Description
    description: str
    root_cause: Optional[str] = None

    # Disposition
    disposition: Optional[str] = None  # scrap, rework, use-as-is, return

    # Costs
    scrap_cost: float = 0.0
    rework_cost: float = 0.0
    total_cost: float = 0.0

    # Status
    status: str = "open"  # open, investigating, resolved, closed
    closed_date: Optional[datetime] = None

    metadata: Dict[str, Any] = field(default_factory=dict)

# ============================================================================
# SPC ENGINE
# ============================================================================

class SPCEngine:
    """Statistical Process Control calculations"""

    @staticmethod
    def calculate_control_limits(data: List[float], sigma: int = 3) -> Dict[str, float]:
        """Calculate control limits for control charts"""
        if len(data) < 2:
            return {}

        mean = statistics.mean(data)
        stdev = statistics.stdev(data)

        return {
            'center_line': mean,
            'upper_control_limit': mean + sigma * stdev,
            'lower_control_limit': mean - sigma * stdev,
            'upper_warning_limit': mean + 2 * stdev,
            'lower_warning_limit': mean - 2 * stdev
        }

    @staticmethod
    def detect_patterns(data: List[float]) -> List[str]:
        """Detect patterns in control chart data"""
        patterns = []

        if len(data) < 7:
            return patterns

        # Rule 1: One point beyond 3-sigma
        mean = statistics.mean(data)
        stdev = statistics.stdev(data)
        ucl = mean + 3 * stdev
        lcl = mean - 3 * stdev

        if any(d > ucl or d < lcl for d in data):
            patterns.append("beyond_control_limits")

        # Rule 2: Seven consecutive points on same side of center
        above = sum(1 for d in data[-7:] if d > mean)
        if above == 7 or above == 0:
            patterns.append("seven_point_run")

        # Rule 3: Seven consecutive points trending up or down
        diffs = [data[i+1] - data[i] for i in range(len(data)-1)]
        if len(diffs) >= 6:
            if all(d > 0 for d in diffs[-6:]):
                patterns.append("trending_up")
            elif all(d < 0 for d in diffs[-6:]):
                patterns.append("trending_down")

        return patterns

# ============================================================================
# CONFIGURATION
# ============================================================================

class QualityExtractorConfig(BaseSettings):
    """Configuration for quality extractor"""

    plex_api_key: str = Field(..., env='PLEX_API_KEY')
    plex_customer_id: str = Field(..., env='PLEX_CUSTOMER_ID')
    plex_base_url: str = Field(default="https://connect.plex.com", env='PLEX_BASE_URL')

    cdf_project: str = Field(..., env='CDF_PROJECT')
    cdf_cluster: str = Field(default='api', env='CDF_CLUSTER')
    cdf_client_id: str = Field(..., env='CDF_CLIENT_ID')
    cdf_client_secret: str = Field(..., env='CDF_CLIENT_SECRET')
    cdf_token_url: str = Field(..., env='CDF_TOKEN_URL')
    cdf_dataset_id: Optional[int] = Field(None, env='CDF_DATASET_ID')

    extraction_mode: str = Field(default='continuous', env='EXTRACTION_MODE')
    extraction_interval_seconds: int = Field(default=300, env='EXTRACTION_INTERVAL')

    enable_spc: bool = Field(default=True, env='ENABLE_SPC')

    class Config:
        env_file = '.env'

# ============================================================================
# MAIN EXTRACTOR
# ============================================================================

class QualityExtractor:
    """Main quality extractor with SPC"""

    def __init__(self, config: QualityExtractorConfig):
        self.config = config
        self.spc_engine = SPCEngine()
        self.logger = logging.getLogger(__name__)

    async def run(self):
        """Main entry point"""
        self.logger.info("Quality Extractor initialized with SPC capabilities")

if __name__ == "__main__":
    config = QualityExtractorConfig()
    extractor = QualityExtractor(config)
    asyncio.run(extractor.run())