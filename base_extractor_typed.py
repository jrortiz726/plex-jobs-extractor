#!/usr/bin/env python3
"""
Fully Type-Hinted Base Extractor for Plex-CDF Integration
Complete type annotations for better IDE support and type safety
"""

from __future__ import annotations

import os
import sys
import asyncio
import logging
from typing import (
    Dict, List, Optional, Any, TypeVar, Generic, Union, Tuple, 
    Callable, Awaitable, Protocol, Final, TypeAlias, Literal,
    cast, overload
)
from typing_extensions import Self, TypedDict, NotRequired
from datetime import datetime, timezone, timedelta
from pathlib import Path
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import StrEnum, auto

from cognite.client import CogniteClient, ClientConfig
from cognite.client.credentials import OAuthClientCredentials
from cognite.client.data_classes import (
    Asset, AssetList, AssetUpdate,
    Event, EventList, EventUpdate,
    TimeSeries, TimeSeriesList, TimeSeriesUpdate,
    Sequence, SequenceList, SequenceUpdate,
    DatapointsList
)
from cognite.client.data_classes.data_modeling import Space, View, NodeList
import aiohttp

# Type aliases for clarity
AssetExternalId: TypeAlias = str
EventExternalId: TypeAlias = str
TimeSeriesExternalId: TypeAlias = str
DatasetId: TypeAlias = int
PCN: TypeAlias = str
Timestamp: TypeAlias = int  # milliseconds since epoch

# Generic type variables
T = TypeVar('T')
TExtractor = TypeVar('TExtractor', bound='BaseExtractor')
TConfig = TypeVar('TConfig', bound='BaseExtractorConfig')

# Logging setup
logger: logging.Logger = logging.getLogger(__name__)


class DatasetType(StrEnum):
    """Enumeration of dataset types"""
    MASTER = auto()
    PRODUCTION = auto()
    SCHEDULING = auto()
    QUALITY = auto()
    INVENTORY = auto()
    MAINTENANCE = auto()


class ExtractorState(StrEnum):
    """Extractor state enumeration"""
    IDLE = auto()
    RUNNING = auto()
    PAUSED = auto()
    ERROR = auto()
    STOPPED = auto()


class PlexEndpoint(TypedDict):
    """Type definition for Plex API endpoint configuration"""
    path: str
    method: Literal["GET", "POST", "PUT", "DELETE"]
    params: NotRequired[Dict[str, Any]]
    headers: NotRequired[Dict[str, str]]


@dataclass(frozen=True)
class FacilityConfig:
    """Immutable facility configuration"""
    pcn: PCN
    facility_name: str
    facility_code: str
    timezone: str = "UTC"
    
    def __post_init__(self) -> None:
        """Validate PCN format"""
        if not self.pcn.isdigit() or len(self.pcn) != 6:
            raise ValueError(f"Invalid PCN format: {self.pcn}. Must be 6 digits.")


@dataclass
class CDFConfig:
    """CDF connection configuration"""
    host: str
    project: str
    client_id: str
    client_secret: str
    token_url: str
    dataset_mapping: Dict[DatasetType, DatasetId] = field(default_factory=dict)
    
    def get_dataset_id(self, dataset_type: DatasetType) -> Optional[DatasetId]:
        """Get dataset ID for a given type"""
        return self.dataset_mapping.get(dataset_type)


@dataclass
class PlexConfig:
    """Plex API configuration"""
    api_key: str
    base_url: str = "https://connect.plex.com"
    timeout: int = 30
    max_retries: int = 3
    rate_limit: int = 100  # requests per minute


@dataclass
class BaseExtractorConfig:
    """Complete configuration for an extractor"""
    facility: FacilityConfig
    cdf: CDFConfig
    plex: PlexConfig
    extraction_interval: int = 300  # seconds
    batch_size: int = 1000
    lookback_days: int = 7
    
    @classmethod
    def from_env(cls, extractor_name: str) -> Self:
        """Load configuration from environment variables"""
        facility = FacilityConfig(
            pcn=os.environ["PLEX_CUSTOMER_ID"],
            facility_name=os.environ["FACILITY_NAME"],
            facility_code=os.getenv("FACILITY_CODE", "DEFAULT")
        )
        
        cdf = CDFConfig(
            host=os.environ["CDF_HOST"],
            project=os.environ["CDF_PROJECT"],
            client_id=os.environ["CDF_CLIENT_ID"],
            client_secret=os.environ["CDF_CLIENT_SECRET"],
            token_url=os.environ["CDF_TOKEN_URL"],
            dataset_mapping={
                DatasetType.MASTER: int(os.getenv("CDF_DATASET_PLEXMASTER", "0")),
                DatasetType.PRODUCTION: int(os.getenv("CDF_DATASET_PLEXPRODUCTION", "0")),
                DatasetType.SCHEDULING: int(os.getenv("CDF_DATASET_PLEXSCHEDULING", "0")),
                DatasetType.QUALITY: int(os.getenv("CDF_DATASET_PLEXQUALITY", "0")),
                DatasetType.INVENTORY: int(os.getenv("CDF_DATASET_PLEXINVENTORY", "0")),
                DatasetType.MAINTENANCE: int(os.getenv("CDF_DATASET_PLEXMAINTENANCE", "0"))
            }
        )
        
        plex = PlexConfig(
            api_key=os.environ["PLEX_API_KEY"]
        )
        
        return cls(
            facility=facility,
            cdf=cdf,
            plex=plex,
            extraction_interval=int(os.getenv(f"{extractor_name.upper()}_EXTRACTION_INTERVAL", "300")),
            batch_size=int(os.getenv("BATCH_SIZE", "1000")),
            lookback_days=int(os.getenv("EXTRACTION_DAYS", "7"))
        )


class ExtractorProtocol(Protocol):
    """Protocol defining extractor interface"""
    
    async def extract(self) -> ExtractionResult:
        """Perform extraction"""
        ...
    
    def get_required_datasets(self) -> List[DatasetType]:
        """Get required dataset types"""
        ...
    
    async def validate_connection(self) -> bool:
        """Validate connections to external systems"""
        ...


@dataclass
class ExtractionResult:
    """Result of an extraction operation"""
    success: bool
    items_processed: int
    duration_ms: float
    errors: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def add_error(self, error: str) -> None:
        """Add an error to the result"""
        self.errors.append(error)
        self.success = False
    
    def merge(self, other: ExtractionResult) -> ExtractionResult:
        """Merge with another result"""
        return ExtractionResult(
            success=self.success and other.success,
            items_processed=self.items_processed + other.items_processed,
            duration_ms=self.duration_ms + other.duration_ms,
            errors=self.errors + other.errors,
            metadata={**self.metadata, **other.metadata}
        )


class StateTracker:
    """Track extraction state with type safety"""
    
    def __init__(self, state_file: Path) -> None:
        self.state_file: Final[Path] = state_file
        self._state: Dict[str, Any] = {}
        self._load_state()
    
    def _load_state(self) -> None:
        """Load state from file"""
        if self.state_file.exists():
            import json
            with open(self.state_file, 'r') as f:
                self._state = json.load(f)
    
    def save_state(self) -> None:
        """Save state to file"""
        import json
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        with open(self.state_file, 'w') as f:
            json.dump(self._state, f, indent=2)
    
    def get_last_extraction_time(self, key: str) -> Optional[datetime]:
        """Get last extraction time for a key"""
        timestamp = self._state.get(f"{key}_last_extraction")
        if timestamp:
            return datetime.fromisoformat(timestamp)
        return None
    
    def set_last_extraction_time(self, key: str, time: datetime) -> None:
        """Set last extraction time for a key"""
        self._state[f"{key}_last_extraction"] = time.isoformat()
        self.save_state()
    
    def get_state(self, key: str, default: T = None) -> Union[T, Any]:
        """Get state value with type-safe default"""
        return self._state.get(key, default)
    
    def set_state(self, key: str, value: Any) -> None:
        """Set state value"""
        self._state[key] = value
        self.save_state()


class BaseExtractor(ABC, Generic[TConfig]):
    """Abstract base extractor with full type hints"""
    
    def __init__(
        self,
        config: TConfig,
        extractor_name: str,
        state_file: Optional[Path] = None
    ) -> None:
        self.config: Final[TConfig] = config
        self.extractor_name: Final[str] = extractor_name
        self.state: ExtractorState = ExtractorState.IDLE
        
        # Initialize CDF client
        self.client: Final[CogniteClient] = self._init_cdf_client()
        
        # State tracking
        state_file = state_file or Path(f"state/{extractor_name}_state.json")
        self.state_tracker: Final[StateTracker] = StateTracker(state_file)
        
        # Metrics
        self.metrics: Dict[str, Union[int, float]] = {
            'total_extractions': 0,
            'successful_extractions': 0,
            'failed_extractions': 0,
            'total_items_processed': 0
        }
    
    def _init_cdf_client(self) -> CogniteClient:
        """Initialize CDF client with type safety"""
        credentials = OAuthClientCredentials(
            token_url=self.config.cdf.token_url,
            client_id=self.config.cdf.client_id,
            client_secret=self.config.cdf.client_secret,
            scopes=[f"{self.config.cdf.host}/.default"]
        )
        
        return CogniteClient(
            ClientConfig(
                client_name=f"plex-{self.extractor_name}-extractor",
                base_url=self.config.cdf.host,
                project=self.config.cdf.project,
                credentials=credentials
            )
        )
    
    @abstractmethod
    async def extract(self) -> ExtractionResult:
        """Main extraction method - must be implemented by subclasses"""
        pass
    
    @abstractmethod
    def get_required_datasets(self) -> List[DatasetType]:
        """Get required dataset types - must be implemented by subclasses"""
        pass
    
    async def validate_connection(self) -> bool:
        """Validate connections to CDF and Plex"""
        try:
            # Test CDF connection
            self.client.time_series.list(limit=1)
            
            # Test Plex connection (implement in subclass if needed)
            await self._validate_plex_connection()
            
            return True
        except Exception as e:
            logger.error(f"Connection validation failed: {e}")
            return False
    
    async def _validate_plex_connection(self) -> bool:
        """Validate Plex connection - override in subclass"""
        return True
    
    def get_dataset_id(self, dataset_type: DatasetType) -> Optional[DatasetId]:
        """Get dataset ID for a given type"""
        return self.config.cdf.get_dataset_id(dataset_type)
    
    def create_external_id(self, resource_type: str, identifier: str) -> AssetExternalId:
        """Create external ID with PCN prefix"""
        return f"PCN{self.config.facility.pcn}_{resource_type.upper()}_{identifier}"
    
    def create_asset(
        self,
        external_id: AssetExternalId,
        name: str,
        parent_id: Optional[int] = None,
        parent_external_id: Optional[AssetExternalId] = None,
        metadata: Optional[Dict[str, str]] = None,
        description: Optional[str] = None,
        dataset_id: Optional[DatasetId] = None
    ) -> Asset:
        """Create an Asset object with type safety"""
        return Asset(
            external_id=external_id,
            name=name,
            parent_id=parent_id,
            parent_external_id=parent_external_id,
            metadata=metadata or {},
            description=description,
            data_set_id=dataset_id
        )
    
    def create_event(
        self,
        external_id: EventExternalId,
        event_type: str,
        subtype: Optional[str] = None,
        start_time: Optional[Timestamp] = None,
        end_time: Optional[Timestamp] = None,
        description: Optional[str] = None,
        metadata: Optional[Dict[str, str]] = None,
        asset_ids: Optional[List[int]] = None,
        dataset_id: Optional[DatasetId] = None
    ) -> Event:
        """Create an Event object with type safety"""
        return Event(
            external_id=external_id,
            type=event_type,
            subtype=subtype,
            start_time=start_time or int(datetime.now(timezone.utc).timestamp() * 1000),
            end_time=end_time,
            description=description,
            metadata=metadata or {},
            asset_ids=asset_ids,
            data_set_id=dataset_id
        )
    
    def create_timeseries(
        self,
        external_id: TimeSeriesExternalId,
        name: str,
        unit: Optional[str] = None,
        asset_id: Optional[int] = None,
        description: Optional[str] = None,
        metadata: Optional[Dict[str, str]] = None,
        dataset_id: Optional[DatasetId] = None
    ) -> TimeSeries:
        """Create a TimeSeries object with type safety"""
        return TimeSeries(
            external_id=external_id,
            name=name,
            unit=unit,
            asset_id=asset_id,
            description=description,
            metadata=metadata or {},
            data_set_id=dataset_id
        )
    
    @overload
    def batch_items(self, items: List[T], batch_size: None = None) -> List[List[T]]:
        ...
    
    @overload
    def batch_items(self, items: List[T], batch_size: int) -> List[List[T]]:
        ...
    
    def batch_items(
        self,
        items: List[T],
        batch_size: Optional[int] = None
    ) -> List[List[T]]:
        """Split items into batches with type preservation"""
        batch_size = batch_size or self.config.batch_size
        return [items[i:i + batch_size] for i in range(0, len(items), batch_size)]
    
    def update_metrics(
        self,
        key: str,
        value: Union[int, float],
        operation: Literal["set", "increment"] = "increment"
    ) -> None:
        """Update metrics with type safety"""
        if operation == "increment":
            current = self.metrics.get(key, 0)
            if isinstance(current, (int, float)):
                self.metrics[key] = current + value
        else:
            self.metrics[key] = value
    
    def get_metrics(self) -> Dict[str, Union[int, float]]:
        """Get current metrics"""
        return self.metrics.copy()
    
    async def run_extraction_cycle(self) -> None:
        """Run a complete extraction cycle"""
        self.state = ExtractorState.RUNNING
        start_time = datetime.now(timezone.utc)
        
        try:
            result = await self.extract()
            
            if result.success:
                self.update_metrics('successful_extractions', 1)
                self.state_tracker.set_last_extraction_time(self.extractor_name, start_time)
            else:
                self.update_metrics('failed_extractions', 1)
                logger.error(f"Extraction failed with errors: {result.errors}")
            
            self.update_metrics('total_items_processed', result.items_processed)
            
        except Exception as e:
            self.update_metrics('failed_extractions', 1)
            logger.error(f"Extraction cycle failed: {e}")
            self.state = ExtractorState.ERROR
            raise
        finally:
            self.update_metrics('total_extractions', 1)
            if self.state != ExtractorState.ERROR:
                self.state = ExtractorState.IDLE