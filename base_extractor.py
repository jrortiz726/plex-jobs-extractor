#!/usr/bin/env python3
"""
Base Extractor Class for Plex to CDF Integration

Provides common functionality for all extractors:
- API client initialization
- CDF client setup
- Deduplication helpers
- Dataset management
- NO RAW table usage
"""

import os
import sys
import json
import aiohttp
import asyncio
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from abc import ABC, abstractmethod

from dotenv import load_dotenv
from cognite.client import CogniteClient
from cognite.client.config import ClientConfig
from cognite.client.credentials import OAuthClientCredentials
from cognite.client.data_classes import Asset, Event, TimeSeries, Sequence

from multi_facility_config import MultiTenantNamingConvention, FacilityConfig
from cdf_utils import CDFDeduplicationHelper, StateTracker

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class BaseExtractorConfig:
    """Base configuration for all extractors"""
    # Plex API settings
    plex_api_key: str
    plex_customer_id: str  # PCN
    
    # CDF credentials
    cdf_host: str
    cdf_project: str
    cdf_client_id: str
    cdf_client_secret: str
    cdf_token_url: str
    
    # Facility information
    facility: FacilityConfig
    
    # Dataset IDs (from .env) - NO RAW DATABASES
    dataset_master_id: Optional[int] = None      # PLEXMASTER
    dataset_production_id: Optional[int] = None  # PLEXPRODUCTION
    dataset_scheduling_id: Optional[int] = None  # PLEXSCHEDULING
    dataset_quality_id: Optional[int] = None     # PLEXQUALITY
    dataset_inventory_id: Optional[int] = None   # PLEXINVENTORY
    dataset_maintenance_id: Optional[int] = None # PLEXMAINTENANCE
    
    # Optional settings
    plex_base_url: str = "https://connect.plex.com"
    use_test_env: bool = False
    extraction_interval: int = 300  # 5 minutes
    batch_size: int = 1000
    max_retries: int = 3
    retry_delay: int = 5
    
    @classmethod
    def from_env(cls, extractor_name: str) -> 'BaseExtractorConfig':
        """Load configuration from environment variables"""
        pcn = os.getenv('PLEX_CUSTOMER_ID')
        
        facility = FacilityConfig(
            pcn=pcn,
            facility_name=os.getenv('FACILITY_NAME', f'Facility {pcn}'),
            facility_code=os.getenv('FACILITY_CODE', f'F{pcn[:3]}'),
            timezone=os.getenv('FACILITY_TIMEZONE', 'UTC'),
            country=os.getenv('FACILITY_COUNTRY', 'US')
        )
        
        def get_int_env(key: str, default: int = None) -> Optional[int]:
            value = os.getenv(key)
            if value:
                try:
                    return int(value)
                except ValueError:
                    logger.warning(f"Invalid integer value for {key}: {value}")
            return default
        
        # Load dataset IDs from environment
        return cls(
            plex_api_key=os.getenv('PLEX_API_KEY'),
            plex_customer_id=pcn,
            facility=facility,
            cdf_host=os.getenv('CDF_HOST'),
            cdf_project=os.getenv('CDF_PROJECT'),
            cdf_client_id=os.getenv('CDF_CLIENT_ID'),
            cdf_client_secret=os.getenv('CDF_CLIENT_SECRET'),
            cdf_token_url=os.getenv('CDF_TOKEN_URL'),
            dataset_master_id=get_int_env('CDF_DATASET_PLEXMASTER'),
            dataset_production_id=get_int_env('CDF_DATASET_PLEXPRODUCTION'),
            dataset_scheduling_id=get_int_env('CDF_DATASET_PLEXSCHEDULING'),
            dataset_quality_id=get_int_env('CDF_DATASET_PLEXQUALITY'),
            dataset_inventory_id=get_int_env('CDF_DATASET_PLEXINVENTORY'),
            dataset_maintenance_id=get_int_env('CDF_DATASET_PLEXMAINTENANCE'),
            use_test_env=os.getenv('PLEX_USE_TEST', 'false').lower() == 'true',
            extraction_interval=get_int_env(f'{extractor_name.upper()}_EXTRACTION_INTERVAL', 300),
            batch_size=get_int_env(f'{extractor_name.upper()}_BATCH_SIZE', 1000)
        )


class BaseExtractor(ABC):
    """Base class for all Plex to CDF extractors - NO RAW TABLES"""
    
    def __init__(self, config: BaseExtractorConfig, extractor_name: str):
        """Initialize base extractor with common functionality"""
        self.config = config
        self.extractor_name = extractor_name
        self.naming = MultiTenantNamingConvention(config.facility)
        self.cognite_client = self._init_cognite_client()
        
        # Initialize deduplication and state tracking
        self.dedup_helper = CDFDeduplicationHelper(self.cognite_client)
        self.state_tracker = StateTracker(f"{extractor_name}_{config.facility.pcn}.json")
        
        # Refresh dedup cache with facility-specific prefix
        self.dedup_helper.refresh_cache(external_id_prefix=f"{config.facility.pcn}_")
        
        # Plex API headers
        self.plex_headers = {
            'X-Plex-Connect-Api-Key': config.plex_api_key,
            'X-Plex-Connect-Customer-Id': config.plex_customer_id,
            'Content-Type': 'application/json'
        }
        
        # State tracking
        self.running = False
        
        logger.info(f"Initialized {extractor_name} for PCN {config.facility.pcn}")
        logger.info(f"Using datasets - Master: {config.dataset_master_id}, "
                   f"Production: {config.dataset_production_id}, "
                   f"Scheduling: {config.dataset_scheduling_id}")
    
    def _init_cognite_client(self) -> CogniteClient:
        """Initialize Cognite client"""
        creds = OAuthClientCredentials(
            token_url=self.config.cdf_token_url,
            client_id=self.config.cdf_client_id,
            client_secret=self.config.cdf_client_secret,
            scopes=["user_impersonation"]
        )
        
        config = ClientConfig(
            client_name=f"{self.extractor_name}-extractor",
            base_url=self.config.cdf_host,
            project=self.config.cdf_project,
            credentials=creds
        )
        
        return CogniteClient(config)
    
    def get_dataset_id(self, dataset_type: str) -> Optional[int]:
        """Get dataset ID for specific type - NO RAW DATASETS"""
        mapping = {
            'master': self.config.dataset_master_id,
            'production': self.config.dataset_production_id,
            'scheduling': self.config.dataset_scheduling_id,
            'quality': self.config.dataset_quality_id,
            'inventory': self.config.dataset_inventory_id,
            'maintenance': self.config.dataset_maintenance_id
        }
        dataset_id = mapping.get(dataset_type)
        if not dataset_id:
            logger.warning(f"No dataset configured for type: {dataset_type}")
        return dataset_id
    
    async def fetch_plex_data(self, endpoint: str, params: Dict = None) -> Any:
        """Fetch data from Plex REST API"""
        url = f"{self.config.plex_base_url}{endpoint}"
        
        if self.config.use_test_env:
            url = url.replace("connect.plex.com", "test.connect.plex.com")
        
        for attempt in range(self.config.max_retries):
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(
                        url,
                        headers=self.plex_headers,
                        params=params
                    ) as response:
                        if response.status == 200:
                            data = await response.json()
                            # Handle both list and dict responses
                            if isinstance(data, list):
                                return data
                            elif isinstance(data, dict) and 'data' in data:
                                return data['data']
                            else:
                                return data
                        else:
                            error = await response.text()
                            logger.warning(f"API error {response.status}: {error}")
                            if attempt < self.config.max_retries - 1:
                                await asyncio.sleep(self.config.retry_delay * (attempt + 1))
                            else:
                                raise Exception(f"API call failed: {error}")
            
            except aiohttp.ClientError as e:
                logger.error(f"Network error: {e}")
                if attempt < self.config.max_retries - 1:
                    await asyncio.sleep(self.config.retry_delay * (attempt + 1))
                else:
                    raise
    
    def validate_dataset_configuration(self):
        """Validate that required datasets are configured"""
        required_datasets = self.get_required_datasets()
        missing = []
        
        for dataset in required_datasets:
            if not self.get_dataset_id(dataset):
                missing.append(dataset)
        
        if missing:
            raise ValueError(f"Missing required datasets: {missing}")
    
    @abstractmethod
    def get_required_datasets(self) -> List[str]:
        """Return list of required dataset types for this extractor"""
        pass
    
    @abstractmethod
    async def extract(self):
        """Main extraction logic - must be implemented by subclasses"""
        pass
    
    async def run_extraction_cycle(self):
        """Run a single extraction cycle with error handling"""
        try:
            logger.info(f"Starting {self.extractor_name} extraction cycle for PCN {self.config.facility.pcn}")
            
            # Validate datasets before extraction
            self.validate_dataset_configuration()
            
            # Refresh deduplication cache
            self.dedup_helper.refresh_cache(external_id_prefix=f"{self.config.facility.pcn}_")
            
            # Run the extraction
            await self.extract()
            
            # Update state
            self.state_tracker.set_last_extraction_time(
                self.extractor_name, 
                datetime.now(timezone.utc)
            )
            
            logger.info(f"Completed {self.extractor_name} extraction cycle")
            
        except Exception as e:
            logger.error(f"Error in extraction cycle: {e}", exc_info=True)
            raise
    
    async def run(self):
        """Run extraction in a loop"""
        self.running = True
        
        while self.running:
            try:
                await self.run_extraction_cycle()
            except Exception as e:
                logger.error(f"Extraction cycle failed: {e}")
            
            logger.info(f"Waiting {self.config.extraction_interval} seconds until next extraction...")
            await asyncio.sleep(self.config.extraction_interval)
    
    def stop(self):
        """Stop the extraction loop"""
        self.running = False
        logger.info(f"Stopping {self.extractor_name} extractor")
    
    # Helper methods for common operations
    
    def create_asset_external_id(self, asset_type: str, asset_id: str) -> str:
        """Create standardized external ID for assets"""
        return self.naming.asset_id(asset_type, asset_id)
    
    def create_event_external_id(self, event_type: str, event_id: str, timestamp: int = None) -> str:
        """Create standardized external ID for events"""
        return self.naming.event_id(event_type, event_id, timestamp or int(datetime.now(timezone.utc).timestamp()))
    
    def create_sequence_external_id(self, sequence_type: str, sequence_id: str) -> str:
        """Create standardized external ID for sequences"""
        return f"{self.config.facility.pcn}_{sequence_type.upper()}_{sequence_id}"
    
    def create_timeseries_external_id(self, asset_type: str, asset_id: str, metric: str) -> str:
        """Create standardized external ID for time series"""
        return self.naming.timeseries_id(asset_type, asset_id, metric)
    
    def parse_timestamp(self, timestamp_str: str) -> int:
        """Parse timestamp string to milliseconds"""
        if not timestamp_str:
            return None
        
        try:
            # Try ISO format first
            dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            return int(dt.timestamp() * 1000)
        except:
            try:
                # Try other formats
                from dateutil import parser
                dt = parser.parse(timestamp_str)
                return int(dt.timestamp() * 1000)
            except:
                logger.warning(f"Could not parse timestamp: {timestamp_str}")
                return None


# NO RAW TABLE VALIDATION
def validate_no_raw_usage(source_file: str) -> bool:
    """Validate that a file doesn't use RAW tables"""
    prohibited_patterns = [
        'raw.rows.insert',
        'raw.databases',
        'raw.tables',
        'ensure_raw_database',
        'ensure_raw_table',
        'cognite_client.raw',
        'client.raw'
    ]
    
    with open(source_file, 'r') as f:
        content = f.read()
        
    for pattern in prohibited_patterns:
        if pattern in content:
            logger.error(f"RAW table usage found in {source_file}: {pattern}")
            return False
    
    return True