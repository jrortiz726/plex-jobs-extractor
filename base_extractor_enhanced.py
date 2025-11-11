#!/usr/bin/env python3
"""
Enhanced Base Extractor with All Improvements Integrated
- Type hints throughout
- Async/await patterns  
- Error handling with retry
- ID resolution for asset linking
- Structured logging
"""

from __future__ import annotations

import os
import sys
import json
import asyncio
import logging
from typing import Dict, List, Optional, Any, TypeVar, Union, Tuple, Final, TypeAlias
from datetime import datetime, timezone, timedelta
from pathlib import Path
from dataclasses import dataclass, field
from abc import ABC, abstractmethod
from enum import StrEnum, auto
from concurrent.futures import ThreadPoolExecutor

import aiohttp
import httpx
from pydantic import BaseModel, Field, validator
from pydantic_settings import BaseSettings
from dotenv import load_dotenv

from cognite.client import CogniteClient, ClientConfig
from cognite.client.credentials import OAuthClientCredentials
from cognite.client.data_classes import (
    Asset, AssetList, Event, EventList, 
    TimeSeries, TimeSeriesList, Sequence, SequenceList
)
from cognite.client.exceptions import CogniteAPIError, CogniteNotFoundError

from multi_facility_config import MultiTenantNamingConvention, FacilityConfig
from cdf_utils import CDFDeduplicationHelper, StateTracker
from id_resolver import AssetIDResolver, EventAssetLinker, get_resolver
from error_handling import (
    RetryHandler, RetryConfig, with_retry, 
    PlexAPIError, PlexRateLimitError, handle_api_response,
    error_aggregator
)

# Load environment variables
load_dotenv()

# Type aliases
AssetExternalId: TypeAlias = str
DatasetId: TypeAlias = int
PCN: TypeAlias = str
Timestamp: TypeAlias = int

# Setup structured logging
import structlog
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
        structlog.processors.JSONRenderer()
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger(__name__)

T = TypeVar('T')


class DatasetType(StrEnum):
    """Dataset type enumeration"""
    MASTER = auto()
    PRODUCTION = auto()
    SCHEDULING = auto()
    QUALITY = auto()
    INVENTORY = auto()
    MAINTENANCE = auto()


@dataclass
class ExtractionResult:
    """Result of an extraction operation"""
    success: bool
    items_processed: int
    duration_ms: float
    errors: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


class BaseExtractorConfig(BaseSettings):
    """Enhanced configuration with Pydantic validation"""
    # Plex API settings
    plex_api_key: str = Field(..., min_length=1, alias='PLEX_API_KEY')
    plex_customer_id: PCN = Field(..., pattern=r'^\d{6}$', alias='PLEX_CUSTOMER_ID')
    
    # CDF credentials
    cdf_host: str = Field(..., min_length=1, alias='CDF_HOST')
    cdf_project: str = Field(..., min_length=1, alias='CDF_PROJECT')
    cdf_client_id: str = Field(..., min_length=1, alias='CDF_CLIENT_ID')
    cdf_client_secret: str = Field(..., min_length=1, alias='CDF_CLIENT_SECRET')
    cdf_token_url: str = Field(..., min_length=1, alias='CDF_TOKEN_URL')
    
    # Facility information
    facility: Optional[FacilityConfig] = None
    
    # Dataset IDs
    dataset_master_id: Optional[DatasetId] = Field(None, gt=0)
    dataset_production_id: Optional[DatasetId] = Field(None, gt=0)
    dataset_scheduling_id: Optional[DatasetId] = Field(None, gt=0)
    dataset_quality_id: Optional[DatasetId] = Field(None, gt=0)
    dataset_inventory_id: Optional[DatasetId] = Field(None, gt=0)
    dataset_maintenance_id: Optional[DatasetId] = Field(None, gt=0)
    
    # Optional settings
    plex_base_url: str = "https://connect.plex.com"
    use_test_env: bool = False
    extraction_interval: int = Field(300, ge=60, le=86400)  # Allow up to 24 hours
    batch_size: int = Field(1000, ge=100, le=5000)
    max_retries: int = Field(3, ge=1, le=10)
    retry_delay: int = Field(5, ge=1, le=60)
    
    class Config:
        arbitrary_types_allowed = True
        env_file = ".env"
        env_file_encoding = "utf-8"
        populate_by_name = True  # Allow both field name and alias
        extra = "ignore"  # Ignore extra fields from environment
    
    def model_post_init(self, __context):
        """Initialize facility config if not provided"""
        if self.facility is None:
            self.facility = FacilityConfig(
                pcn=self.plex_customer_id,
                facility_name=os.getenv('FACILITY_NAME', f'Facility {self.plex_customer_id}'),
                facility_code=os.getenv('FACILITY_CODE', 'DEFAULT'),
                timezone=os.getenv('FACILITY_TIMEZONE', 'UTC'),
                country=os.getenv('FACILITY_COUNTRY', 'US')
            )
    
    @validator('cdf_host')
    def validate_cdf_host(cls, v: str) -> str:
        if 'cognitedata.com' not in v:
            raise ValueError('Invalid CDF host URL')
        return v
    
    @classmethod
    def from_env(cls, extractor_name: str) -> BaseExtractorConfig:
        """Load configuration from environment with validation"""
        pcn = os.environ['PLEX_CUSTOMER_ID']
        
        facility = FacilityConfig(
            pcn=pcn,
            facility_name=os.getenv('FACILITY_NAME', f'Facility {pcn}'),
            facility_code=os.getenv('FACILITY_CODE', f'F{pcn[:3]}'),
            timezone=os.getenv('FACILITY_TIMEZONE', 'UTC'),
            country=os.getenv('FACILITY_COUNTRY', 'US')
        )
        
        def get_int_env(key: str, default: Optional[int] = None) -> Optional[int]:
            value = os.getenv(key)
            if value:
                try:
                    return int(value)
                except ValueError:
                    logger.warning(f"Invalid integer for {key}", key=key, value=value)
            return default
        
        return cls(
            plex_api_key=os.environ['PLEX_API_KEY'],
            plex_customer_id=pcn,
            cdf_host=os.environ['CDF_HOST'],
            cdf_project=os.environ['CDF_PROJECT'],
            cdf_client_id=os.environ['CDF_CLIENT_ID'],
            cdf_client_secret=os.environ['CDF_CLIENT_SECRET'],
            cdf_token_url=os.environ['CDF_TOKEN_URL'],
            facility=facility,
            dataset_master_id=get_int_env('CDF_DATASET_PLEXMASTER'),
            dataset_production_id=get_int_env('CDF_DATASET_PLEXPRODUCTION'),
            dataset_scheduling_id=get_int_env('CDF_DATASET_PLEXSCHEDULING'),
            dataset_quality_id=get_int_env('CDF_DATASET_PLEXQUALITY'),
            dataset_inventory_id=get_int_env('CDF_DATASET_PLEXINVENTORY'),
            dataset_maintenance_id=get_int_env('CDF_DATASET_PLEXMAINTENANCE'),
            extraction_interval=get_int_env(f'{extractor_name.upper()}_EXTRACTION_INTERVAL', 300),
            batch_size=get_int_env('BATCH_SIZE', 1000),
            max_retries=get_int_env('MAX_RETRIES', 3),
            retry_delay=get_int_env('RETRY_DELAY', 5)
        )


class AsyncCDFWrapper:
    """Async wrapper for CDF operations"""
    
    def __init__(self, client: CogniteClient, max_workers: int = 10):
        self.client = client
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
    
    async def create_assets(self, assets: List[Asset]) -> AssetList:
        """Async asset creation"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self._executor,
            self.client.assets.create,
            assets
        )
    
    async def upsert_assets(self, assets: List[Asset]) -> AssetList:
        """Async asset upsert"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self._executor,
            self.client.assets.upsert,
            assets
        )
    
    async def create_events(self, events: List[Event]) -> EventList:
        """Async event creation"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self._executor,
            self.client.events.create,
            events
        )
    
    async def create_time_series(self, time_series: List) -> List:
        """Async time series creation"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self._executor,
            self.client.time_series.create,
            time_series
        )
    
    def cleanup(self):
        """Cleanup thread pool"""
        self._executor.shutdown(wait=True)


class BaseExtractor(ABC):
    """Enhanced base extractor with all improvements"""
    
    def __init__(self, config: BaseExtractorConfig, extractor_name: str):
        """Initialize with enhanced features"""
        self.config: Final[BaseExtractorConfig] = config
        self.extractor_name: Final[str] = extractor_name
        
        # Setup structured logger with context
        self.logger = structlog.get_logger(__name__).bind(
            extractor=extractor_name,
            pcn=config.facility.pcn,
            facility=config.facility.facility_name
        )
        
        # Initialize CDF client
        self.client: Final[CogniteClient] = self._init_cdf_client()
        self.async_cdf = AsyncCDFWrapper(self.client)
        
        # Initialize helpers
        self.naming = MultiTenantNamingConvention(config.facility)
        self.dedup_helper = CDFDeduplicationHelper(self.client)
        self.state_tracker = StateTracker(f"state/{extractor_name}_state.json")
        
        # Initialize ID resolver for asset linking
        self.id_resolver = get_resolver(self.client)
        self.event_linker = EventAssetLinker(self.id_resolver)
        
        # Initialize retry handler
        retry_config = RetryConfig(
            max_attempts=config.max_retries,
            initial_delay=config.retry_delay
        )
        self.retry_handler = RetryHandler(retry_config)
        
        # HTTP client with connection pooling
        self.http_client: Optional[httpx.AsyncClient] = None
        
        # Metrics
        self.metrics: Dict[str, Union[int, float]] = {
            'total_extractions': 0,
            'successful_extractions': 0,
            'failed_extractions': 0,
            'total_items_processed': 0,
            'total_errors': 0
        }
        
        self.logger.info(
            "extractor_initialized",
            config={
                'batch_size': config.batch_size,
                'interval': config.extraction_interval,
                'datasets': self.get_required_datasets()
            }
        )
    
    def _init_cdf_client(self) -> CogniteClient:
        """Initialize CDF client with proper error handling"""
        try:
            credentials = OAuthClientCredentials(
                token_url=self.config.cdf_token_url,
                client_id=self.config.cdf_client_id,
                client_secret=self.config.cdf_client_secret,
                scopes=["user_impersonation"]  # Use same scope as non-enhanced version
            )
            
            return CogniteClient(
                ClientConfig(
                    client_name=f"plex-{self.extractor_name}-extractor",
                    base_url=self.config.cdf_host,
                    project=self.config.cdf_project,
                    credentials=credentials
                )
            )
        except Exception as e:
            self.logger.error("cdf_client_init_failed", error=str(e))
            raise
    
    async def _init_http_client(self):
        """Initialize async HTTP client"""
        if not self.http_client:
            self.http_client = httpx.AsyncClient(
                http2=True,
                limits=httpx.Limits(
                    max_keepalive_connections=20,
                    max_connections=50,
                    keepalive_expiry=30
                ),
                timeout=httpx.Timeout(30.0, pool=5.0),
                headers={
                    'X-Plex-Connect-Api-Key': self.config.plex_api_key,
                    'X-Plex-Connect-Customer-Id': self.config.plex_customer_id,
                    'Content-Type': 'application/json'
                }
            )
    
    @abstractmethod
    def get_required_datasets(self) -> List[str]:
        """Return list of required dataset types"""
        pass
    
    @abstractmethod
    async def extract(self) -> ExtractionResult:
        """Main extraction logic - implement in subclass"""
        pass
    
    def get_dataset_id(self, dataset_type: str) -> Optional[DatasetId]:
        """Get dataset ID by type with validation"""
        mapping = {
            'master': self.config.dataset_master_id,
            'production': self.config.dataset_production_id,
            'scheduling': self.config.dataset_scheduling_id,
            'quality': self.config.dataset_quality_id,
            'inventory': self.config.dataset_inventory_id,
            'maintenance': self.config.dataset_maintenance_id
        }
        dataset_id = mapping.get(dataset_type.lower())
        
        if not dataset_id:
            self.logger.warning(
                "dataset_not_configured",
                dataset_type=dataset_type
            )
        
        return dataset_id
    
    @with_retry(max_attempts=3, initial_delay=1.0)
    async def fetch_plex_data(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None
    ) -> Union[List, Dict]:
        """Fetch data from Plex API with retry and error handling"""
        await self._init_http_client()
        
        url = f"{self.config.plex_base_url}{endpoint}"
        
        self.logger.debug(
            "plex_api_request",
            endpoint=endpoint,
            params=params
        )
        
        try:
            response = await self.http_client.get(url, params=params)
            handle_api_response(response, "Plex API")
            response.raise_for_status()
            
            data = response.json()
            
            # Handle different response formats
            if isinstance(data, dict) and 'data' in data:
                return data['data']
            return data
            
        except httpx.TimeoutException as e:
            self.logger.error("plex_api_timeout", endpoint=endpoint, error=str(e))
            raise PlexAPIError(f"Timeout: {endpoint}")
        except Exception as e:
            self.logger.error("plex_api_error", endpoint=endpoint, error=str(e))
            error_aggregator.add_error(PlexAPIError(str(e)))
            raise
    
    def create_event_external_id(self, event_type: str, identifier: str) -> str:
        """Create external ID for event with PCN prefix"""
        # Use event_id method with a timestamp or just use asset_id for simplicity
        return self.naming.asset_id(event_type, identifier)
    
    def create_asset_external_id(self, asset_type: str, identifier: str) -> str:
        """Create external ID for asset with PCN prefix"""
        return self.naming.asset_id(asset_type, identifier)
    
    async def create_assets_with_retry(
        self,
        assets: List[Asset],
        resolve_parents: bool = True
    ) -> Tuple[List[str], List[str]]:
        """Create assets with deduplication and retry logic"""
        if not assets:
            return [], []
        
        try:
            # Resolve parent IDs if needed
            if resolve_parents:
                for asset in assets:
                    if asset.parent_external_id and not asset.parent_id:
                        parent_id = self.id_resolver.resolve_single(asset.parent_external_id)
                        if parent_id:
                            asset.parent_id = parent_id
            
            # Use retry handler
            result = await self.retry_handler.async_retry(
                self.async_cdf.upsert_assets,
                assets,
                circuit_breaker_name=f"cdf_assets_{self.extractor_name}"
            )
            
            created_ids = [a.external_id for a in result] if result else []
            
            self.logger.info(
                "assets_created",
                count=len(created_ids),
                dataset_id=assets[0].data_set_id if assets else None
            )
            
            return created_ids, []
            
        except Exception as e:
            self.logger.error("asset_creation_failed", error=str(e))
            return [], [a.external_id for a in assets]
    
    async def create_events_with_retry(
        self,
        events: List[Event],
        link_assets: bool = True
    ) -> Tuple[List[str], List[str]]:
        """Create events with asset linking and retry logic"""
        if not events:
            return [], []
        
        try:
            # Link to assets if needed
            if link_assets:
                for event in events:
                    # Convert any asset_external_ids to numeric IDs
                    if hasattr(event, 'asset_external_ids'):
                        ext_ids = event.asset_external_ids
                        delattr(event, 'asset_external_ids')
                        
                        numeric_ids = self.event_linker.prepare_event_asset_ids(ext_ids)
                        if numeric_ids:
                            event.asset_ids = numeric_ids
            
            # Check for duplicates
            external_ids = [e.external_id for e in events]
            existing = set()
            for ext_id in external_ids:
                if self.dedup_helper.event_exists(ext_id):
                    existing.add(ext_id)
            
            new_events = [e for e in events if e.external_id not in existing]
            
            if not new_events:
                self.logger.info("all_events_exist", count=len(events))
                return [], external_ids
            
            # Create with retry
            result = await self.retry_handler.async_retry(
                self.async_cdf.create_events,
                new_events,
                circuit_breaker_name=f"cdf_events_{self.extractor_name}"
            )
            
            created_ids = [e.external_id for e in result] if result else []
            duplicate_ids = [e for e in external_ids if e in existing]
            
            self.logger.info(
                "events_created",
                created=len(created_ids),
                duplicates=len(duplicate_ids),
                dataset_id=events[0].data_set_id if events else None
            )
            
            return created_ids, duplicate_ids
            
        except Exception as e:
            self.logger.error("event_creation_failed", error=str(e))
            return [], [e.external_id for e in events]
    
    async def ensure_facility_asset(self) -> None:
        """Ensure the facility asset exists"""
        try:
            facility_external_id = self.naming.asset_id('facility', self.config.facility.pcn)
            
            # Create facility asset
            facility_asset = Asset(
                external_id=facility_external_id,
                name=f"{self.config.facility.facility_name} - Facility",
                description=f"Facility for PCN {self.config.facility.pcn}",
                metadata={
                    'pcn': self.config.facility.pcn,
                    'facility_name': self.config.facility.facility_name,
                    'facility_code': self.config.facility.facility_code,
                    'timezone': self.config.facility.timezone,
                    'country': self.config.facility.country,
                    'type': 'facility',
                    'created_by': 'enhanced_extractor'
                },
                data_set_id=self.get_dataset_id('master')  # Put in master dataset
            )
            
            # Upsert to ensure it exists
            await self.async_cdf.upsert_assets([facility_asset])
            self.logger.info("facility_asset_ensured", facility_id=facility_external_id)
            
        except Exception as e:
            self.logger.error("facility_asset_creation_error", error=str(e))
            # Don't fail extraction if facility asset creation fails
    
    async def run_extraction_cycle(self) -> None:
        """Run a complete extraction cycle with error handling"""
        start_time = datetime.now(timezone.utc)
        
        # Ensure facility asset exists first
        await self.ensure_facility_asset()
        
        self.logger.info("extraction_cycle_started")
        
        try:
            result = await self.extract()
            
            self.metrics['total_extractions'] += 1
            self.metrics['total_items_processed'] += result.items_processed
            
            if result.success:
                self.metrics['successful_extractions'] += 1
                self.state_tracker.set_last_extraction_time(
                    self.extractor_name,
                    start_time
                )
                
                self.logger.info(
                    "extraction_cycle_completed",
                    items_processed=result.items_processed,
                    duration_ms=result.duration_ms
                )
            else:
                self.metrics['failed_extractions'] += 1
                self.metrics['total_errors'] += len(result.errors)
                
                self.logger.error(
                    "extraction_cycle_failed",
                    errors=result.errors[:5],  # Log first 5 errors
                    total_errors=len(result.errors)
                )
                
                # Add to error aggregator
                for error_msg in result.errors:
                    error_aggregator.add_error(PlexAPIError(error_msg))
                
        except Exception as e:
            self.metrics['failed_extractions'] += 1
            self.metrics['total_errors'] += 1
            
            self.logger.error(
                "extraction_cycle_exception",
                error=str(e),
                exc_info=True
            )
            
            raise
        finally:
            # Check if we should alert
            if error_aggregator.should_alert(threshold=10):
                self.logger.warning(
                    "error_threshold_exceeded",
                    error_summary=error_aggregator.get_error_summary()
                )
    
    async def cleanup(self):
        """Cleanup resources"""
        if self.http_client:
            await self.http_client.aclose()
        self.async_cdf.cleanup()
        
        self.logger.info(
            "extractor_cleanup",
            metrics=self.metrics
        )
    
    def get_metrics(self) -> Dict[str, Union[int, float]]:
        """Get current metrics"""
        return self.metrics.copy()