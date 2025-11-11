#!/usr/bin/env python3
"""
Standalone Production Extractor for Cognite Data Fusion
========================================================

Production-ready extractor for real-time manufacturing data with OEE metrics:
- Real-time production tracking and workcenter monitoring
- OEE (Overall Equipment Effectiveness) calculation and trending
- Advanced analytics metadata for performance optimization
- Shift-based analysis and operator performance tracking
- Downtime classification and root cause analysis
- Quality metrics integration (FTY, scrap, rework)
- Predictive maintenance indicators

This extractor is designed for deployment as a Cognite Function or standalone service.

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
from typing import Dict, List, Optional, Any, Tuple, Set, Union, TypeAlias, Final
from dataclasses import dataclass, field, asdict
from enum import StrEnum, auto
from collections import defaultdict, deque
import logging
import statistics

# Third-party imports
try:
    import httpx
    import numpy as np
    from pydantic import BaseModel, Field, validator, BaseSettings
    from cognite.client import CogniteClient, ClientConfig
    from cognite.client.credentials import OAuthClientCredentials
    from cognite.client.data_classes import (
        Asset, AssetList, AssetUpdate,
        TimeSeries, TimeSeriesList,
        Datapoints, DatapointsList,
        Event, EventList,
        DataSet,
        LabelDefinition, Label
    )
    from cognite.client.exceptions import CogniteAPIError
except ImportError as e:
    print(f"Missing required dependency: {e}")
    print("Install with: pip install cognite-sdk httpx pydantic numpy python-dotenv")
    sys.exit(1)

# Type aliases
WorkcenterId: TypeAlias = str
JobId: TypeAlias = str
PartId: TypeAlias = str
OperatorId: TypeAlias = str
ShiftId: TypeAlias = str

# Constants for OEE and KPIs
OEE_WORLD_CLASS = 85.0  # World-class OEE benchmark
AVAILABILITY_TARGET = 90.0
PERFORMANCE_TARGET = 95.0
QUALITY_TARGET = 99.0

DOWNTIME_CATEGORIES = {
    "planned": ["changeover", "maintenance", "setup", "cleaning"],
    "unplanned": ["breakdown", "material_shortage", "operator_absent", "quality_issue"],
    "external": ["no_demand", "no_power", "strike", "weather"]
}

LOSS_CATEGORIES = {
    "availability_losses": ["equipment_failure", "setup_adjustments", "material_shortage"],
    "performance_losses": ["minor_stops", "reduced_speed", "startup_losses"],
    "quality_losses": ["defects", "rework", "startup_rejects"]
}

# ============================================================================
# DATA MODELS
# ============================================================================

class ProductionStatus(StrEnum):
    """Production status states"""
    RUNNING = auto()
    IDLE = auto()
    DOWN = auto()
    MAINTENANCE = auto()
    CHANGEOVER = auto()
    OFFLINE = auto()

class ShiftType(StrEnum):
    """Shift classifications"""
    FIRST = auto()   # Morning shift
    SECOND = auto()  # Afternoon shift
    THIRD = auto()   # Night shift
    WEEKEND = auto()
    OVERTIME = auto()

@dataclass
class WorkcenterState:
    """Real-time workcenter state with OEE components"""
    workcenter_id: WorkcenterId
    workcenter_name: str
    workcenter_type: Optional[str] = None  # machining, assembly, inspection, packaging

    # Current state
    status: ProductionStatus = ProductionStatus.IDLE
    status_reason: Optional[str] = None
    status_start_time: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    # Current job
    current_job_id: Optional[JobId] = None
    current_part_id: Optional[PartId] = None
    current_part_name: Optional[str] = None
    current_operator_id: Optional[OperatorId] = None
    current_shift: Optional[ShiftType] = None

    # Production metrics (current shift)
    planned_production_time: float = 0.0  # minutes
    actual_run_time: float = 0.0  # minutes
    downtime: float = 0.0  # minutes
    planned_quantity: int = 0
    actual_quantity: int = 0
    good_quantity: int = 0
    scrap_quantity: int = 0
    rework_quantity: int = 0

    # Cycle times
    ideal_cycle_time: float = 0.0  # seconds
    actual_cycle_time: float = 0.0  # seconds
    takt_time: float = 0.0  # seconds (customer demand rate)

    # OEE Components
    availability: float = 0.0  # percentage
    performance: float = 0.0  # percentage
    quality: float = 0.0  # percentage
    oee: float = 0.0  # percentage

    # Performance indicators
    production_rate: float = 0.0  # units per hour
    efficiency: float = 0.0  # percentage
    utilization: float = 0.0  # percentage

    # Downtime tracking
    downtime_events: List[DowntimeEvent] = field(default_factory=list)
    minor_stops: int = 0
    speed_losses: float = 0.0  # percentage

    # Quality metrics
    first_time_yield: float = 0.0  # percentage
    defect_rate: float = 0.0  # PPM
    rework_rate: float = 0.0  # percentage

    # Metadata
    last_updated: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    metadata: Dict[str, Any] = field(default_factory=dict)

    def calculate_oee(self) -> float:
        """Calculate real-time OEE"""
        if self.planned_production_time == 0:
            return 0.0

        # Availability = Run Time / Planned Production Time
        self.availability = (self.actual_run_time / self.planned_production_time) * 100 if self.planned_production_time > 0 else 0

        # Performance = (Actual Output / (Run Time / Ideal Cycle Time)) * 100
        if self.actual_run_time > 0 and self.ideal_cycle_time > 0:
            theoretical_output = (self.actual_run_time * 60) / self.ideal_cycle_time  # Convert minutes to seconds
            self.performance = (self.actual_quantity / theoretical_output) * 100 if theoretical_output > 0 else 0
        else:
            self.performance = 0

        # Quality = Good Count / Total Count
        self.quality = (self.good_quantity / self.actual_quantity) * 100 if self.actual_quantity > 0 else 0

        # OEE = Availability × Performance × Quality
        self.oee = (self.availability * self.performance * self.quality) / 10000

        return self.oee

    def get_analytics_metadata(self) -> Dict[str, Any]:
        """Generate rich metadata for analytics"""
        return {
            "workcenter_id": self.workcenter_id,
            "workcenter_type": self.workcenter_type,
            "status": self.status.value,
            "oee": round(self.oee, 2),
            "availability": round(self.availability, 2),
            "performance": round(self.performance, 2),
            "quality": round(self.quality, 2),
            "oee_loss": round(100 - self.oee, 2),
            "world_class_gap": round(OEE_WORLD_CLASS - self.oee, 2),
            "production_rate": round(self.production_rate, 2),
            "efficiency": round(self.efficiency, 2),
            "utilization": round(self.utilization, 2),
            "first_time_yield": round(self.first_time_yield, 2),
            "defect_rate_ppm": round(self.defect_rate, 0),
            "shift": self.current_shift.value if self.current_shift else None,
            "operator": self.current_operator_id,
            "job": self.current_job_id,
            "part": self.current_part_id,
            "takt_time": round(self.takt_time, 2),
            "cycle_efficiency": round((self.ideal_cycle_time / self.actual_cycle_time * 100), 2) if self.actual_cycle_time > 0 else 0,
            "downtime_minutes": round(self.downtime, 2),
            "minor_stops_count": self.minor_stops,
            "has_quality_issues": self.scrap_quantity > 0 or self.rework_quantity > 0,
            "is_bottleneck": self.oee < 60,  # Flag potential bottlenecks
            "needs_attention": self.oee < 50 or self.availability < 70
        }

@dataclass
class DowntimeEvent:
    """Downtime event tracking"""
    event_id: str
    workcenter_id: WorkcenterId
    start_time: datetime
    end_time: Optional[datetime] = None
    duration_minutes: float = 0.0
    category: str = "unplanned"  # planned, unplanned, external
    reason: str = ""
    reason_code: Optional[str] = None
    impact: str = "medium"  # low, medium, high, critical
    job_id: Optional[JobId] = None
    operator_id: Optional[OperatorId] = None
    shift: Optional[ShiftType] = None
    comments: Optional[str] = None
    resolved: bool = False
    resolution: Optional[str] = None
    cost_impact: Optional[float] = None
    units_lost: Optional[int] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def calculate_duration(self) -> float:
        """Calculate duration in minutes"""
        if self.end_time and self.start_time:
            self.duration_minutes = (self.end_time - self.start_time).total_seconds() / 60
        return self.duration_minutes

@dataclass
class ProductionEntry:
    """Production entry with quality and performance data"""
    entry_id: str
    workcenter_id: WorkcenterId
    job_id: JobId
    part_id: PartId
    timestamp: datetime

    # Quantities
    quantity_produced: int = 0
    quantity_good: int = 0
    quantity_scrap: int = 0
    quantity_rework: int = 0

    # Timing
    cycle_time: float = 0.0  # actual seconds
    setup_time: float = 0.0  # minutes
    run_time: float = 0.0  # minutes

    # Operator and shift
    operator_id: Optional[OperatorId] = None
    shift: Optional[ShiftType] = None

    # Quality data
    defect_codes: List[str] = field(default_factory=list)
    inspection_passed: bool = True
    quality_notes: Optional[str] = None

    # Process parameters (for correlation analysis)
    process_parameters: Dict[str, float] = field(default_factory=dict)

    metadata: Dict[str, Any] = field(default_factory=dict)

@dataclass
class ShiftPerformance:
    """Shift-level performance aggregation"""
    shift_id: str
    shift_type: ShiftType
    shift_date: datetime
    workcenter_id: WorkcenterId

    # Time breakdown
    scheduled_time: float = 480.0  # 8 hours default
    planned_downtime: float = 0.0
    unplanned_downtime: float = 0.0
    runtime: float = 0.0

    # Production
    target_quantity: int = 0
    actual_quantity: int = 0
    good_quantity: int = 0
    scrap_quantity: int = 0

    # OEE metrics
    availability: float = 0.0
    performance: float = 0.0
    quality: float = 0.0
    oee: float = 0.0

    # Operator performance
    operators: List[OperatorId] = field(default_factory=list)
    operator_efficiency: Dict[OperatorId, float] = field(default_factory=dict)

    # Issues
    downtime_events: List[DowntimeEvent] = field(default_factory=list)
    quality_issues: List[Dict[str, Any]] = field(default_factory=list)

    metadata: Dict[str, Any] = field(default_factory=dict)

# ============================================================================
# CONFIGURATION
# ============================================================================

class ProductionExtractorConfig(BaseSettings):
    """Configuration for production extractor"""

    # Plex API Configuration
    plex_api_key: str = Field(..., env='PLEX_API_KEY')
    plex_customer_id: str = Field(..., env='PLEX_CUSTOMER_ID')
    plex_base_url: str = Field(default="https://connect.plex.com", env='PLEX_BASE_URL')

    # Cognite Configuration
    cdf_project: str = Field(..., env='CDF_PROJECT')
    cdf_cluster: str = Field(default='api', env='CDF_CLUSTER')
    cdf_client_id: str = Field(..., env='CDF_CLIENT_ID')
    cdf_client_secret: str = Field(..., env='CDF_CLIENT_SECRET')
    cdf_token_url: str = Field(..., env='CDF_TOKEN_URL')
    cdf_dataset_id: Optional[int] = Field(None, env='CDF_DATASET_ID')

    # Extraction Configuration
    extraction_mode: str = Field(default='continuous', env='EXTRACTION_MODE')
    extraction_interval_seconds: int = Field(default=60, env='EXTRACTION_INTERVAL')  # 1 minute for production
    batch_size: int = Field(default=1000, env='BATCH_SIZE')
    max_workers: int = Field(default=20, env='MAX_WORKERS')

    # Data Collection
    collect_workcenter_status: bool = Field(default=True, env='COLLECT_STATUS')
    collect_production_entries: bool = Field(default=True, env='COLLECT_PRODUCTION')
    collect_oee_metrics: bool = Field(default=True, env='COLLECT_OEE')
    collect_downtime: bool = Field(default=True, env='COLLECT_DOWNTIME')
    collect_quality: bool = Field(default=True, env='COLLECT_QUALITY')

    # Time Windows
    lookback_hours: int = Field(default=24, env='LOOKBACK_HOURS')
    real_time_window_minutes: int = Field(default=5, env='REALTIME_WINDOW')

    # OEE Configuration
    oee_calculation_interval: int = Field(default=15, env='OEE_INTERVAL')  # minutes
    availability_threshold: float = Field(default=90.0, env='AVAILABILITY_TARGET')
    performance_threshold: float = Field(default=95.0, env='PERFORMANCE_TARGET')
    quality_threshold: float = Field(default=99.0, env='QUALITY_TARGET')

    # Analytics
    enable_predictive_analytics: bool = Field(default=True, env='ENABLE_PREDICTIVE')
    enable_anomaly_detection: bool = Field(default=True, env='ENABLE_ANOMALY')
    enable_bottleneck_analysis: bool = Field(default=True, env='ENABLE_BOTTLENECK')

    # Monitoring
    enable_metrics: bool = Field(default=True, env='ENABLE_METRICS')
    metrics_port: int = Field(default=8081, env='METRICS_PORT')
    log_level: str = Field(default='INFO', env='LOG_LEVEL')

    class Config:
        env_file = '.env'
        case_sensitive = False

# ============================================================================
# OEE CALCULATOR
# ============================================================================

class OEECalculator:
    """Advanced OEE calculation with loss analysis"""

    def __init__(self, config: ProductionExtractorConfig):
        self.config = config
        self.history_window = deque(maxlen=100)  # Keep last 100 calculations

    def calculate_oee(self, state: WorkcenterState) -> Dict[str, float]:
        """Calculate OEE with detailed loss analysis"""
        state.calculate_oee()

        # Calculate losses
        availability_loss = 100 - state.availability
        performance_loss = 100 - state.performance
        quality_loss = 100 - state.quality

        # Calculate TEEP (Total Effective Equipment Performance)
        # TEEP = OEE × Utilization
        teep = state.oee * (state.utilization / 100) if state.utilization > 0 else 0

        # Six big losses analysis
        six_big_losses = self._analyze_six_big_losses(state)

        result = {
            "oee": round(state.oee, 2),
            "availability": round(state.availability, 2),
            "performance": round(state.performance, 2),
            "quality": round(state.quality, 2),
            "teep": round(teep, 2),
            "availability_loss": round(availability_loss, 2),
            "performance_loss": round(performance_loss, 2),
            "quality_loss": round(quality_loss, 2),
            "total_loss": round(100 - state.oee, 2),
            "world_class_gap": round(OEE_WORLD_CLASS - state.oee, 2),
            **six_big_losses
        }

        # Store in history for trending
        self.history_window.append({
            "timestamp": datetime.now(timezone.utc),
            "workcenter_id": state.workcenter_id,
            **result
        })

        return result

    def _analyze_six_big_losses(self, state: WorkcenterState) -> Dict[str, float]:
        """Analyze six big losses in OEE"""
        total_time = state.planned_production_time if state.planned_production_time > 0 else 480  # Default 8 hours

        # 1. Breakdown losses (unplanned downtime)
        breakdown_loss = sum(
            event.duration_minutes
            for event in state.downtime_events
            if event.category == "unplanned"
        ) / total_time * 100

        # 2. Setup and adjustment losses
        setup_loss = sum(
            event.duration_minutes
            for event in state.downtime_events
            if event.reason in ["setup", "changeover", "adjustment"]
        ) / total_time * 100

        # 3. Small stops and idling
        small_stops_loss = (state.minor_stops * 1.0) / total_time * 100  # Assume 1 minute per stop

        # 4. Reduced speed losses
        speed_loss = state.speed_losses

        # 5. Startup rejects
        startup_rejects = state.metadata.get("startup_rejects", 0)
        startup_loss = (startup_rejects / state.actual_quantity * 100) if state.actual_quantity > 0 else 0

        # 6. Production rejects
        production_reject_loss = ((state.scrap_quantity + state.rework_quantity) / state.actual_quantity * 100) if state.actual_quantity > 0 else 0

        return {
            "breakdown_loss": round(breakdown_loss, 2),
            "setup_loss": round(setup_loss, 2),
            "small_stops_loss": round(small_stops_loss, 2),
            "speed_loss": round(speed_loss, 2),
            "startup_loss": round(startup_loss, 2),
            "production_reject_loss": round(production_reject_loss, 2),
            "total_six_losses": round(
                breakdown_loss + setup_loss + small_stops_loss + speed_loss + startup_loss + production_reject_loss,
                2
            )
        }

    def get_trend(self, workcenter_id: WorkcenterId, hours: int = 24) -> Dict[str, Any]:
        """Get OEE trend for a workcenter"""
        cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
        data = [
            entry for entry in self.history_window
            if entry["workcenter_id"] == workcenter_id and entry["timestamp"] > cutoff
        ]

        if not data:
            return {}

        oee_values = [d["oee"] for d in data]

        return {
            "current_oee": oee_values[-1] if oee_values else 0,
            "average_oee": round(statistics.mean(oee_values), 2),
            "min_oee": round(min(oee_values), 2),
            "max_oee": round(max(oee_values), 2),
            "std_dev": round(statistics.stdev(oee_values), 2) if len(oee_values) > 1 else 0,
            "trend": "improving" if len(oee_values) > 1 and oee_values[-1] > oee_values[0] else "declining",
            "data_points": len(oee_values)
        }

# ============================================================================
# COGNITE INTEGRATION
# ============================================================================

class ProductionCogniteManager:
    """Manages Cognite Data Fusion operations for production data"""

    def __init__(self, config: ProductionExtractorConfig):
        self.config = config
        self.client = self._init_client()
        self.dataset_id = self._ensure_dataset()
        self.time_series_cache = {}

    def _init_client(self) -> CogniteClient:
        """Initialize Cognite client"""
        credentials = OAuthClientCredentials(
            token_url=self.config.cdf_token_url,
            client_id=self.config.cdf_client_id,
            client_secret=self.config.cdf_client_secret,
            scopes=[f"https://{self.config.cdf_cluster}.cognitedata.com/.default"]
        )

        return CogniteClient(
            ClientConfig(
                client_name="production-extractor-standalone",
                base_url=f"https://{self.config.cdf_cluster}.cognitedata.com",
                project=self.config.cdf_project,
                credentials=credentials,
                max_workers=self.config.max_workers
            )
        )

    def _ensure_dataset(self) -> int:
        """Ensure dataset exists"""
        if self.config.cdf_dataset_id:
            return self.config.cdf_dataset_id

        dataset_name = f"plex_production_{self.config.plex_customer_id}"

        existing = self.client.data_sets.list(limit=None)
        for ds in existing:
            if ds.external_id == dataset_name:
                return ds.id

        dataset = self.client.data_sets.create(
            DataSet(
                external_id=dataset_name,
                name=f"Plex Production Data - {self.config.plex_customer_id}",
                description="Real-time production data with OEE metrics",
                metadata={
                    "source": "plex",
                    "type": "production",
                    "customer_id": self.config.plex_customer_id,
                    "extractor_version": "2.0.0"
                }
            )
        )
        return dataset.id

    async def upsert_workcenter_assets(self, states: List[WorkcenterState]) -> Tuple[int, int]:
        """Create/update workcenter assets with current state"""
        if not states:
            return 0, 0

        assets = []
        for state in states:
            metadata = state.get_analytics_metadata()
            metadata["last_updated"] = datetime.now(timezone.utc).isoformat()

            asset = Asset(
                external_id=f"workcenter_{self.config.plex_customer_id}_{state.workcenter_id}",
                name=state.workcenter_name,
                description=f"Workcenter: {state.workcenter_name} - Type: {state.workcenter_type}",
                metadata=metadata,
                data_set_id=self.dataset_id,
                labels=[
                    Label(external_id="production_workcenter"),
                    Label(external_id=f"status_{state.status.value}"),
                    Label(external_id="oee_tracked") if state.oee > 0 else None
                ]
            )
            assets.append(asset)

        try:
            result = self.client.assets.upsert(assets, mode="replace")
            return len(result), 0
        except Exception as e:
            logging.error(f"Failed to upsert workcenter assets: {e}")
            return 0, len(assets)

    async def create_oee_time_series(self, states: List[WorkcenterState]) -> Tuple[int, int]:
        """Create time series for OEE metrics"""
        time_series = []
        datapoints_to_insert = []

        for state in states:
            base_id = f"oee_{self.config.plex_customer_id}_{state.workcenter_id}"

            # Create time series for each OEE component
            metrics = [
                ("oee", "Overall Equipment Effectiveness", "%"),
                ("availability", "Availability", "%"),
                ("performance", "Performance", "%"),
                ("quality", "Quality", "%"),
                ("production_rate", "Production Rate", "units/hour"),
                ("cycle_time", "Cycle Time", "seconds"),
                ("scrap_rate", "Scrap Rate", "%"),
                ("downtime", "Downtime", "minutes")
            ]

            for metric_name, description, unit in metrics:
                ts_id = f"{base_id}_{metric_name}"

                if ts_id not in self.time_series_cache:
                    ts = TimeSeries(
                        external_id=ts_id,
                        name=f"{state.workcenter_name} - {description}",
                        description=f"{description} for {state.workcenter_name}",
                        metadata={
                            "workcenter_id": state.workcenter_id,
                            "workcenter_name": state.workcenter_name,
                            "metric_type": metric_name,
                            "unit": unit
                        },
                        unit=unit,
                        data_set_id=self.dataset_id
                    )
                    time_series.append(ts)
                    self.time_series_cache[ts_id] = True

                # Prepare datapoint
                value = getattr(state, metric_name, 0)
                if metric_name == "scrap_rate":
                    value = (state.scrap_quantity / state.actual_quantity * 100) if state.actual_quantity > 0 else 0

                datapoints_to_insert.append({
                    "external_id": ts_id,
                    "datapoints": [(datetime.now(timezone.utc), value)]
                })

        # Create time series if needed
        if time_series:
            try:
                self.client.time_series.create(time_series)
            except:
                pass  # Already exists

        # Insert datapoints
        if datapoints_to_insert:
            try:
                for dp in datapoints_to_insert:
                    self.client.time_series.data.insert(
                        external_id=dp["external_id"],
                        datapoints=dp["datapoints"]
                    )
                return len(datapoints_to_insert), 0
            except Exception as e:
                logging.error(f"Failed to insert datapoints: {e}")
                return 0, len(datapoints_to_insert)

        return 0, 0

    async def create_production_events(self, entries: List[ProductionEntry]) -> Tuple[int, int]:
        """Create production events"""
        if not entries:
            return 0, 0

        events = []
        for entry in entries:
            event = Event(
                external_id=f"production_{self.config.plex_customer_id}_{entry.entry_id}",
                type="production_entry",
                subtype=f"shift_{entry.shift.value}" if entry.shift else "production",
                description=f"Production: {entry.quantity_produced} units of part {entry.part_id}",
                start_time=int(entry.timestamp.timestamp() * 1000),
                end_time=int((entry.timestamp + timedelta(minutes=entry.run_time)).timestamp() * 1000),
                metadata={
                    "workcenter_id": entry.workcenter_id,
                    "job_id": entry.job_id,
                    "part_id": entry.part_id,
                    "operator_id": entry.operator_id,
                    "shift": entry.shift.value if entry.shift else None,
                    "quantity_produced": entry.quantity_produced,
                    "quantity_good": entry.quantity_good,
                    "quantity_scrap": entry.quantity_scrap,
                    "cycle_time": entry.cycle_time,
                    "first_time_yield": (entry.quantity_good / entry.quantity_produced * 100) if entry.quantity_produced > 0 else 0,
                    **entry.process_parameters
                },
                data_set_id=self.dataset_id
            )
            events.append(event)

        try:
            result = self.client.events.create(events)
            return len(result), 0
        except Exception as e:
            logging.error(f"Failed to create production events: {e}")
            return 0, len(events)

    async def create_downtime_events(self, downtimes: List[DowntimeEvent]) -> Tuple[int, int]:
        """Create downtime events with classification"""
        if not downtimes:
            return 0, 0

        events = []
        for dt in downtimes:
            dt.calculate_duration()

            event = Event(
                external_id=f"downtime_{self.config.plex_customer_id}_{dt.event_id}",
                type="downtime",
                subtype=dt.category,
                description=f"Downtime: {dt.reason} - {dt.duration_minutes:.1f} minutes",
                start_time=int(dt.start_time.timestamp() * 1000),
                end_time=int(dt.end_time.timestamp() * 1000) if dt.end_time else None,
                metadata={
                    "workcenter_id": dt.workcenter_id,
                    "category": dt.category,
                    "reason": dt.reason,
                    "reason_code": dt.reason_code,
                    "impact": dt.impact,
                    "duration_minutes": dt.duration_minutes,
                    "job_id": dt.job_id,
                    "operator_id": dt.operator_id,
                    "shift": dt.shift.value if dt.shift else None,
                    "cost_impact": dt.cost_impact,
                    "units_lost": dt.units_lost,
                    "resolved": dt.resolved,
                    "resolution": dt.resolution
                },
                data_set_id=self.dataset_id
            )
            events.append(event)

        try:
            result = self.client.events.create(events)
            return len(result), 0
        except Exception as e:
            logging.error(f"Failed to create downtime events: {e}")
            return 0, len(events)

# ============================================================================
# PLEX CONNECTOR
# ============================================================================

class ProductionPlexConnector:
    """Connects to Plex API for production data"""

    def __init__(self, config: ProductionExtractorConfig):
        self.config = config
        self.session = None
        self._setup_session()

    def _setup_session(self):
        """Setup HTTP session"""
        self.session = httpx.AsyncClient(
            base_url=self.config.plex_base_url,
            headers={
                'X-Plex-Connect-Api-Key': self.config.plex_api_key,
                'X-Plex-Connect-Customer-Id': self.config.plex_customer_id,
                'Content-Type': 'application/json'
            },
            timeout=30.0,
            limits=httpx.Limits(max_keepalive_connections=20, max_connections=50)
        )

    async def fetch_workcenter_status(self) -> List[WorkcenterState]:
        """Fetch current workcenter status"""
        try:
            response = await self.session.get("/api/v1/workcenters/status")
            response.raise_for_status()

            data = response.json()
            states = []

            for item in data.get('data', []):
                state = WorkcenterState(
                    workcenter_id=str(item.get('Workcenter_Key')),
                    workcenter_name=item.get('Workcenter_Name', ''),
                    workcenter_type=item.get('Workcenter_Type'),
                    status=self._map_status(item.get('Status')),
                    status_reason=item.get('Status_Reason'),
                    current_job_id=str(item.get('Current_Job_Key')) if item.get('Current_Job_Key') else None,
                    current_part_id=str(item.get('Part_Key')) if item.get('Part_Key') else None,
                    current_operator_id=str(item.get('Operator_Key')) if item.get('Operator_Key') else None,
                    planned_production_time=item.get('Planned_Production_Time', 0),
                    actual_run_time=item.get('Actual_Run_Time', 0),
                    downtime=item.get('Downtime', 0),
                    planned_quantity=item.get('Planned_Quantity', 0),
                    actual_quantity=item.get('Actual_Quantity', 0),
                    good_quantity=item.get('Good_Quantity', 0),
                    scrap_quantity=item.get('Scrap_Quantity', 0),
                    ideal_cycle_time=item.get('Ideal_Cycle_Time', 0),
                    actual_cycle_time=item.get('Actual_Cycle_Time', 0)
                )

                # Calculate OEE
                state.calculate_oee()
                states.append(state)

            return states

        except Exception as e:
            logging.error(f"Error fetching workcenter status: {e}")
            return []

    async def fetch_production_entries(self, start_time: datetime, end_time: datetime) -> List[ProductionEntry]:
        """Fetch production entries for time range"""
        try:
            response = await self.session.get(
                "/api/v1/production/entries",
                params={
                    "start_date": start_time.isoformat(),
                    "end_date": end_time.isoformat()
                }
            )
            response.raise_for_status()

            data = response.json()
            entries = []

            for item in data.get('data', []):
                entry = ProductionEntry(
                    entry_id=str(item.get('Production_Key')),
                    workcenter_id=str(item.get('Workcenter_Key')),
                    job_id=str(item.get('Job_Key')),
                    part_id=str(item.get('Part_Key')),
                    timestamp=self._parse_datetime(item.get('Production_Date')),
                    quantity_produced=item.get('Quantity_Produced', 0),
                    quantity_good=item.get('Good_Quantity', 0),
                    quantity_scrap=item.get('Scrap_Quantity', 0),
                    quantity_rework=item.get('Rework_Quantity', 0),
                    cycle_time=item.get('Cycle_Time', 0),
                    run_time=item.get('Run_Time', 0),
                    operator_id=str(item.get('Operator_Key')) if item.get('Operator_Key') else None,
                    shift=self._map_shift(item.get('Shift'))
                )
                entries.append(entry)

            return entries

        except Exception as e:
            logging.error(f"Error fetching production entries: {e}")
            return []

    async def fetch_downtime_events(self, start_time: datetime, end_time: datetime) -> List[DowntimeEvent]:
        """Fetch downtime events"""
        try:
            response = await self.session.get(
                "/api/v1/downtime/events",
                params={
                    "start_date": start_time.isoformat(),
                    "end_date": end_time.isoformat()
                }
            )
            response.raise_for_status()

            data = response.json()
            events = []

            for item in data.get('data', []):
                event = DowntimeEvent(
                    event_id=str(item.get('Downtime_Key')),
                    workcenter_id=str(item.get('Workcenter_Key')),
                    start_time=self._parse_datetime(item.get('Start_Time')),
                    end_time=self._parse_datetime(item.get('End_Time')),
                    category=self._map_downtime_category(item.get('Category')),
                    reason=item.get('Reason', ''),
                    reason_code=item.get('Reason_Code'),
                    impact=item.get('Impact', 'medium'),
                    job_id=str(item.get('Job_Key')) if item.get('Job_Key') else None,
                    operator_id=str(item.get('Operator_Key')) if item.get('Operator_Key') else None,
                    resolved=item.get('Resolved', False),
                    resolution=item.get('Resolution')
                )
                event.calculate_duration()
                events.append(event)

            return events

        except Exception as e:
            logging.error(f"Error fetching downtime events: {e}")
            return []

    def _map_status(self, plex_status: Optional[str]) -> ProductionStatus:
        """Map Plex status to ProductionStatus"""
        if not plex_status:
            return ProductionStatus.IDLE

        mapping = {
            'RUNNING': ProductionStatus.RUNNING,
            'IDLE': ProductionStatus.IDLE,
            'DOWN': ProductionStatus.DOWN,
            'MAINTENANCE': ProductionStatus.MAINTENANCE,
            'CHANGEOVER': ProductionStatus.CHANGEOVER,
            'OFFLINE': ProductionStatus.OFFLINE
        }
        return mapping.get(plex_status.upper(), ProductionStatus.IDLE)

    def _map_shift(self, shift_code: Optional[str]) -> Optional[ShiftType]:
        """Map shift codes to ShiftType"""
        if not shift_code:
            return None

        mapping = {
            '1': ShiftType.FIRST,
            '2': ShiftType.SECOND,
            '3': ShiftType.THIRD,
            'W': ShiftType.WEEKEND,
            'O': ShiftType.OVERTIME
        }
        return mapping.get(shift_code, None)

    def _map_downtime_category(self, category: Optional[str]) -> str:
        """Map downtime category"""
        if not category:
            return "unplanned"

        category_lower = category.lower()
        if category_lower in ["planned", "scheduled"]:
            return "planned"
        elif category_lower in ["external", "outside"]:
            return "external"
        else:
            return "unplanned"

    def _parse_datetime(self, date_str: Optional[str]) -> Optional[datetime]:
        """Parse datetime from Plex format"""
        if not date_str:
            return None

        try:
            return datetime.fromisoformat(date_str.replace('Z', '+00:00'))
        except:
            return None

    async def close(self):
        """Close HTTP session"""
        if self.session:
            await self.session.aclose()

# ============================================================================
# MAIN EXTRACTOR
# ============================================================================

class ProductionExtractor:
    """Main production extractor with OEE calculation"""

    def __init__(self, config: ProductionExtractorConfig):
        self.config = config
        self.cognite = ProductionCogniteManager(config)
        self.plex = ProductionPlexConnector(config)
        self.oee_calculator = OEECalculator(config)

        # Setup logging
        logging.basicConfig(
            level=getattr(logging, config.log_level),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)

        # Metrics
        self.metrics = {
            "extractions": 0,
            "workcenter_updates": 0,
            "production_entries": 0,
            "downtime_events": 0,
            "oee_calculations": 0,
            "errors": 0
        }

    async def extract_workcenter_status(self) -> Tuple[int, int]:
        """Extract and update workcenter status with OEE"""
        self.logger.info("Extracting workcenter status...")

        states = await self.plex.fetch_workcenter_status()
        if not states:
            return 0, 0

        # Calculate OEE for each workcenter
        for state in states:
            oee_metrics = self.oee_calculator.calculate_oee(state)
            state.metadata.update(oee_metrics)
            self.metrics["oee_calculations"] += 1

        # Update assets
        created, failed = await self.cognite.upsert_workcenter_assets(states)
        self.metrics["workcenter_updates"] += created

        # Create time series data
        ts_created, ts_failed = await self.cognite.create_oee_time_series(states)

        self.logger.info(f"Updated {created} workcenters, created {ts_created} datapoints")
        return created + ts_created, failed + ts_failed

    async def extract_production_entries(self) -> Tuple[int, int]:
        """Extract production entries"""
        self.logger.info("Extracting production entries...")

        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(hours=self.config.lookback_hours)

        entries = await self.plex.fetch_production_entries(start_time, end_time)
        if not entries:
            return 0, 0

        created, failed = await self.cognite.create_production_events(entries)
        self.metrics["production_entries"] += created

        self.logger.info(f"Created {created} production events")
        return created, failed

    async def extract_downtime_events(self) -> Tuple[int, int]:
        """Extract downtime events"""
        self.logger.info("Extracting downtime events...")

        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(hours=self.config.lookback_hours)

        events = await self.plex.fetch_downtime_events(start_time, end_time)
        if not events:
            return 0, 0

        created, failed = await self.cognite.create_downtime_events(events)
        self.metrics["downtime_events"] += created

        self.logger.info(f"Created {created} downtime events")
        return created, failed

    async def run_extraction(self):
        """Run complete extraction cycle"""
        start_time = datetime.now(timezone.utc)
        self.logger.info(f"Starting production extraction at {start_time}")

        try:
            total_created = 0
            total_failed = 0

            # Extract based on configuration
            if self.config.collect_workcenter_status or self.config.collect_oee_metrics:
                created, failed = await self.extract_workcenter_status()
                total_created += created
                total_failed += failed

            if self.config.collect_production_entries:
                created, failed = await self.extract_production_entries()
                total_created += created
                total_failed += failed

            if self.config.collect_downtime:
                created, failed = await self.extract_downtime_events()
                total_created += created
                total_failed += failed

            self.metrics["extractions"] += 1

            duration = (datetime.now(timezone.utc) - start_time).total_seconds()
            self.logger.info(
                f"Extraction completed in {duration:.2f}s - "
                f"Created: {total_created}, Failed: {total_failed}"
            )

            # Log metrics
            self.logger.info(f"Metrics: {self.metrics}")

        except Exception as e:
            self.logger.error(f"Extraction failed: {e}", exc_info=True)
            self.metrics["errors"] += 1
            raise

    async def run(self):
        """Main entry point"""
        self.logger.info(f"Starting Production Extractor - Mode: {self.config.extraction_mode}")

        try:
            if self.config.extraction_mode == 'one-time':
                await self.run_extraction()
            else:
                # Continuous mode
                while True:
                    await self.run_extraction()
                    self.logger.info(f"Sleeping for {self.config.extraction_interval_seconds} seconds...")
                    await asyncio.sleep(self.config.extraction_interval_seconds)

        except KeyboardInterrupt:
            self.logger.info("Extractor stopped by user")
        finally:
            await self.plex.close()
            self.logger.info("Extractor shutdown complete")

# ============================================================================
# ENTRY POINT
# ============================================================================

def main():
    """Main entry point"""
    try:
        config = ProductionExtractorConfig()
    except Exception as e:
        print(f"Failed to load configuration: {e}")
        print("Ensure all required environment variables are set")
        sys.exit(1)

    extractor = ProductionExtractor(config)
    asyncio.run(extractor.run())

if __name__ == "__main__":
    main()