# production_extractor.py - Multi-facility aware production data extraction
"""
Production Extractor with Multi-Facility Support

This version includes PCN (Plex Customer Number) in all identifiers
to ensure complete data isolation between manufacturing facilities.
"""

import os
import sys
import aiohttp
import asyncio
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any
from dataclasses import dataclass

from dotenv import load_dotenv
from cognite.client import CogniteClient
from cognite.client.config import ClientConfig
from cognite.client.credentials import OAuthClientCredentials
from cognite.client.data_classes import Asset, Event, TimeSeries

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class FacilityInfo:
    """Information about a Plex facility"""
    pcn: str
    name: str = ""
    code: str = ""
    
    @property
    def prefix(self) -> str:
        """Get the standard prefix for this facility"""
        return f"PCN{self.pcn}"


@dataclass
class ProductionExtractorConfig:
    """Configuration for the Production Extractor with multi-facility support"""
    # Required Plex API settings
    plex_api_key: str
    plex_customer_id: str  # This is the PCN
    
    # Required CDF settings
    cdf_host: str
    cdf_project: str
    cdf_client_id: str
    cdf_client_secret: str
    cdf_token_url: str
    
    # Facility information
    facility: FacilityInfo
    
    # Workcenter configuration
    workcenter_ids: List[str] = None
    
    # Optional settings with defaults
    plex_base_url: str = "https://connect.plex.com"
    extraction_interval: int = 60  # 1 minute for production data
    batch_size: int = 5000
    max_retries: int = 3
    retry_delay: int = 5
    
    # Dataset IDs (PCN-specific)
    dataset_production_id: Optional[int] = None
    dataset_master_id: Optional[int] = None
    
    @classmethod
    def from_env(cls) -> 'ProductionExtractorConfig':
        """Load configuration from environment variables"""
        pcn = os.getenv('PLEX_CUSTOMER_ID')
        
        # Create facility info
        facility = FacilityInfo(
            pcn=pcn,
            name=os.getenv('FACILITY_NAME', f'Facility {pcn}'),
            code=os.getenv('FACILITY_CODE', f'F{pcn[:3]}')
        )
        
        # Helper function to safely get int from env
        def get_int_env(key: str, default: int = None) -> Optional[int]:
            value = os.getenv(key)
            if value:
                try:
                    return int(value)
                except ValueError:
                    logger.warning(f"Invalid integer value for {key}: {value}")
            return default
        
        # Get workcenter IDs from environment (comma-separated)
        wc_ids_str = os.getenv('WORKCENTER_IDS', '')
        workcenter_ids = [wc.strip() for wc in wc_ids_str.split(',') if wc.strip()] if wc_ids_str else []
        
        # Try PCN-specific dataset IDs first, then fall back to generic ones
        production_id = get_int_env(f'CDF_DATASET_{pcn}_PRODUCTION') or \
                       get_int_env('CDF_DATASET_PLEXPRODUCTION')
        master_id = get_int_env(f'CDF_DATASET_{pcn}_MASTER') or \
                   get_int_env('CDF_DATASET_PLEXMASTER')
        
        return cls(
            plex_api_key=os.getenv('PLEX_API_KEY'),
            plex_customer_id=pcn,
            facility=facility,
            cdf_host=os.getenv('CDF_HOST'),
            cdf_project=os.getenv('CDF_PROJECT'),
            cdf_client_id=os.getenv('CDF_CLIENT_ID'),
            cdf_client_secret=os.getenv('CDF_CLIENT_SECRET'),
            cdf_token_url=os.getenv('CDF_TOKEN_URL'),
            workcenter_ids=workcenter_ids,
            extraction_interval=get_int_env('PRODUCTION_EXTRACTION_INTERVAL', 60),
            batch_size=get_int_env('BATCH_SIZE', 5000),
            dataset_production_id=production_id,
            dataset_master_id=master_id
        )


class MultiTenantNaming:
    """Handles PCN-aware naming conventions"""
    
    def __init__(self, facility: FacilityInfo):
        self.facility = facility
        self.prefix = facility.prefix
    
    def asset_id(self, asset_type: str, identifier: str) -> str:
        """Generate asset external ID with PCN prefix"""
        return f"{self.prefix}_{asset_type}_{identifier}"
    
    def event_id(self, event_type: str, entity: str, timestamp: float = None) -> str:
        """Generate event external ID with PCN prefix"""
        ts_part = f"_{int(timestamp)}" if timestamp else ""
        return f"{self.prefix}_EVT_{event_type}_{entity}{ts_part}"
    
    def timeseries_id(self, entity_type: str, entity_id: str, metric: str) -> str:
        """Generate time series external ID with PCN prefix"""
        return f"{self.prefix}_TS_{entity_type}_{entity_id}_{metric}"
    
    def get_metadata(self) -> Dict[str, str]:
        """Get standard metadata including PCN"""
        return {
            'pcn': self.facility.pcn,
            'facility_name': self.facility.name,
            'facility_code': self.facility.code,
            'source': 'PlexMES'
        }


class PlexProductionExtractor:
    def __init__(self, config: ProductionExtractorConfig):
        self.config = config
        self.naming = MultiTenantNaming(config.facility)
        self.cognite_client = self._init_cognite_client()
        self.plex_headers = {
            'X-Plex-Connect-Api-Key': config.plex_api_key,
            'X-Plex-Connect-Customer-Id': config.plex_customer_id,
            'Content-Type': 'application/json'
        }
        
        # State tracking (PCN-specific)
        self.last_extraction_time: Optional[datetime] = None
        self.time_series_cache: Dict[str, TimeSeries] = {}
        self.running = False
        
        logger.info(f"Initialized production extractor for PCN {config.facility.pcn} ({config.facility.name})")
        
    def _init_cognite_client(self) -> CogniteClient:
        """Initialize Cognite client with proper authentication"""
        creds = OAuthClientCredentials(
            token_url=self.config.cdf_token_url,
            client_id=self.config.cdf_client_id,
            client_secret=self.config.cdf_client_secret,
            scopes=["user_impersonation"]
        )
        
        config = ClientConfig(
            client_name=f"plex-production-extractor-{self.config.facility.pcn}",
            base_url=self.config.cdf_host,
            project=self.config.cdf_project,
            credentials=creds
        )
        
        return CogniteClient(config)
    
    async def fetch_plex_data(self, endpoint: str, params: Optional[Dict] = None) -> Dict:
        """Fetch data from Plex API with retry logic"""
        url = f"{self.config.plex_base_url}{endpoint}"
        
        for attempt in range(self.config.max_retries):
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(
                        url,
                        headers=self.plex_headers,
                        params=params,
                        timeout=aiohttp.ClientTimeout(total=30)
                    ) as response:
                        if response.status == 200:
                            data = await response.json()
                            if isinstance(data, list):
                                return {'data': data}
                            return data
                        elif response.status == 429:
                            wait_time = int(response.headers.get('Retry-After', '60'))
                            logger.warning(f"Rate limited. Waiting {wait_time} seconds...")
                            await asyncio.sleep(wait_time)
                        else:
                            error_text = await response.text()
                            logger.error(f"Plex API error {response.status}: {error_text}")
                            
            except asyncio.TimeoutError:
                logger.warning(f"Timeout on attempt {attempt + 1}/{self.config.max_retries}")
            except Exception as e:
                logger.error(f"Error fetching from {endpoint}: {e}")
                
            if attempt < self.config.max_retries - 1:
                await asyncio.sleep(self.config.retry_delay * (attempt + 1))
                
        return None
    
    def ensure_time_series_exists(self, external_id: str, name: str, unit: str = None, is_string: bool = False):
        """Ensure a time series exists in CDF"""
        if external_id in self.time_series_cache:
            return
        
        try:
            # Check if it exists
            ts = self.cognite_client.time_series.retrieve(external_id=external_id)
            if ts:
                self.time_series_cache[external_id] = ts
                return
        except:
            pass
        
        # Create it
        try:
            ts = TimeSeries(
                external_id=external_id,
                name=name,
                description=f"{name} for {self.config.facility.name}",
                data_set_id=self.config.dataset_production_id,
                unit=unit,
                is_string=is_string,
                metadata=self.naming.get_metadata_tags()
            )
            created = self.cognite_client.time_series.create(ts)
            self.time_series_cache[external_id] = created
            logger.info(f"Created time series: {external_id}")
        except Exception as e:
            logger.error(f"Failed to create time series {external_id}: {e}")
    
    async def fetch_all_workcenters(self):
        """Fetch all workcenters for the PCN from Plex API"""
        logger.info(f"Fetching all workcenters for PCN {self.config.facility.pcn}")
        
        data = await self.fetch_plex_data("/production/v1/production-definitions/workcenters")
        if not data:
            logger.warning("No workcenters found")
            return []
        
        workcenters = data.get('data', data) if isinstance(data, dict) else data
        if not isinstance(workcenters, list):
            workcenters = [workcenters]
        
        wc_list = []
        for wc in workcenters:
            wc_id = wc.get('id') or wc.get('workcenterId')
            if wc_id:
                wc_list.append({
                    'id': str(wc_id),
                    'name': wc.get('name', f'Workcenter {wc_id}'),
                    'type': wc.get('type', 'Unknown'),
                    'status': wc.get('status', 'Active')
                })
        
        logger.info(f"Found {len(wc_list)} workcenters for PCN {self.config.facility.pcn}")
        return wc_list
    
    async def extract_workcenter_status(self):
        """Extract real-time workcenter status with PCN-aware naming"""
        # Fetch all workcenters if not configured
        if not self.config.workcenter_ids:
            logger.info("No specific workcenter IDs configured, fetching all workcenters")
            workcenters = await self.fetch_all_workcenters()
            workcenter_ids = [wc['id'] for wc in workcenters if wc.get('status') == 'Active']
        else:
            workcenter_ids = self.config.workcenter_ids
        
        if not workcenter_ids:
            logger.warning("No workcenters to process")
            return
        
        logger.info(f"Extracting status for {len(workcenter_ids)} workcenters in PCN {self.config.facility.pcn}")
        
        datapoints_by_ts = {}
        
        # Get current time window (last minute)
        end_time = datetime.now(timezone.utc)
        begin_time = end_time - timedelta(minutes=1)
        
        for wc_id in workcenter_ids:
            try:
                # Fetch latest status from workcenter-status-entries
                params = {
                    'workcenterId': wc_id,
                    'beginDate': begin_time.strftime('%Y-%m-%dT%H:%M:%SZ'),
                    'endDate': end_time.strftime('%Y-%m-%dT%H:%M:%SZ')
                }
                data = await self.fetch_plex_data("/production/v1/production-history/workcenter-status-entries", params)
                if not data:
                    continue
                
                # Handle response - it's a list of status entries
                status_data = data.get('data', data) if isinstance(data, dict) else data
                if isinstance(status_data, list) and len(status_data) > 0:
                    # Get the most recent status entry
                    latest_status = status_data[-1]  # Last entry should be most recent
                    
                    timestamp = int(datetime.now(timezone.utc).timestamp() * 1000)
                    workcenter_code = latest_status.get('workcenterCode', wc_id)
                    status_desc = latest_status.get('workcenterStatus', 'UNKNOWN')
                    
                    # Status time series (categorical)
                    status_ts_id = self.naming.timeseries_id("WC", workcenter_code, "STATUS")
                    self.ensure_time_series_exists(
                        status_ts_id,
                        f"Workcenter {workcenter_code} Status",
                        is_string=True
                    )
                    
                    if status_ts_id not in datapoints_by_ts:
                        datapoints_by_ts[status_ts_id] = []
                    datapoints_by_ts[status_ts_id].append((timestamp, status_desc))
                    
                    # Duration in current status (minutes)
                    duration_ts_id = self.naming.timeseries_id("WC", workcenter_code, "STATUS_DURATION")
                    self.ensure_time_series_exists(
                        duration_ts_id,
                        f"Workcenter {workcenter_code} Status Duration",
                        unit="minutes"
                    )
                    
                    if duration_ts_id not in datapoints_by_ts:
                        datapoints_by_ts[duration_ts_id] = []
                    duration = latest_status.get('duration', 0)
                    datapoints_by_ts[duration_ts_id].append((timestamp, duration))
                    
                    logger.debug(f"WC {workcenter_code} status: {status_desc}, duration: {duration} min")
                
            except Exception as e:
                logger.error(f"Error extracting status for workcenter {wc_id}: {e}")
        
        # Upload all datapoints
        if datapoints_by_ts:
            try:
                datapoints_list = [
                    {"external_id": ts_id, "datapoints": points}
                    for ts_id, points in datapoints_by_ts.items()
                ]
                self.cognite_client.time_series.data.insert_multiple(datapoints_list)
                logger.info(f"Uploaded status data for {len(datapoints_by_ts)} time series")
            except Exception as e:
                logger.error(f"Failed to upload time series data: {e}")
    
    async def extract_production_entries(self):
        """Extract production transaction logs with PCN-aware naming"""
        logger.info(f"Extracting production entries for PCN {self.config.facility.pcn}")
        
        # Determine time range
        default_start = os.getenv('EXTRACTION_START_DATE', '2024-01-01T00:00:00Z')
        try:
            initial_date = datetime.fromisoformat(default_start.replace('Z', '+00:00'))
        except:
            initial_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
        
        date_from = self.last_extraction_time or initial_date
        date_to = datetime.now(timezone.utc)
        
        params = {
            'limit': self.config.batch_size,
            'offset': 0,
            'dateFrom': date_from.isoformat(),
            'dateTo': date_to.isoformat()
        }
        
        all_entries = []
        events = []
        datapoints_by_ts = {}
        
        # Fetch production entries
        while True:
            data = await self.fetch_plex_data("/production/v1/production-entries", params)
            if not data:
                break
            
            entries = data.get('data', [])
            if not entries:
                break
            
            all_entries.extend(entries)
            logger.info(f"Fetched {len(entries)} production entries (total: {len(all_entries)})")
            
            if len(entries) < self.config.batch_size:
                break
            
            params['offset'] += self.config.batch_size
        
        # Process production entries
        for entry in all_entries:
            try:
                timestamp = int(datetime.fromisoformat(
                    entry['timestamp'].replace('Z', '+00:00')
                ).timestamp() * 1000)
                wc_id = entry.get('workcenterId', 'UNKNOWN')
                
                # Throughput time series
                throughput_ts_id = self.naming.timeseries_id("WC", wc_id, "THROUGHPUT")
                self.ensure_time_series_exists(
                    throughput_ts_id,
                    f"Workcenter {wc_id} Throughput",
                    unit="pieces/hour"
                )
                
                if throughput_ts_id not in datapoints_by_ts:
                    datapoints_by_ts[throughput_ts_id] = []
                datapoints_by_ts[throughput_ts_id].append((timestamp, entry.get('quantityGood', 0)))
                
                # Scrap rate time series
                scrap_ts_id = self.naming.timeseries_id("WC", wc_id, "SCRAP_RATE")
                self.ensure_time_series_exists(
                    scrap_ts_id,
                    f"Workcenter {wc_id} Scrap Rate",
                    unit="%"
                )
                
                if scrap_ts_id not in datapoints_by_ts:
                    datapoints_by_ts[scrap_ts_id] = []
                
                good = entry.get('quantityGood', 0)
                scrap = entry.get('scrapCount', 0)
                scrap_rate = (scrap / (good + scrap) * 100) if (good + scrap) > 0 else 0
                datapoints_by_ts[scrap_ts_id].append((timestamp, scrap_rate))
                
                # Create production event with PCN-aware ID
                event = Event(
                    external_id=self.naming.event_id("PROD_ENTRY", wc_id, timestamp/1000),
                    type="production_transaction",
                    subtype="quantity_produced",
                    start_time=timestamp,
                    end_time=timestamp,
                    description=f"Production entry for WC {wc_id} at {self.config.facility.name}",
                    asset_external_ids=[self.naming.asset_id("WC", wc_id)],
                    data_set_id=self.config.dataset_production_id,
                    metadata={
                        **self.naming.get_metadata_tags(),
                        'workcenter_id': wc_id,
                        'quantity_good': str(good),
                        'quantity_scrap': str(scrap),
                        'operator': entry.get('operator', ''),
                        'shift': entry.get('shift', ''),
                        'labor_hours': str(entry.get('laborHours', 0)),
                        'part_number': entry.get('partNumber', ''),
                        'job_number': entry.get('jobNumber', '')
                    }
                )
                events.append(event)
                
            except Exception as e:
                logger.error(f"Error processing production entry: {e}")
        
        # Upload time series data
        if datapoints_by_ts:
            try:
                datapoints_list = [
                    {"external_id": ts_id, "datapoints": points}
                    for ts_id, points in datapoints_by_ts.items()
                ]
                self.cognite_client.time_series.data.insert_multiple(datapoints_list)
                logger.info(f"Uploaded production data to {len(datapoints_by_ts)} time series")
            except Exception as e:
                logger.error(f"Failed to upload time series data: {e}")
        
        # Upload events
        if events:
            try:
                # Batch upload
                batch_size = 1000
                for i in range(0, len(events), batch_size):
                    batch = events[i:i+batch_size]
                    self.cognite_client.events.create(batch)
                    logger.info(f"Uploaded {len(batch)} production events")
            except Exception as e:
                logger.error(f"Failed to upload events: {e}")
        
        self.last_extraction_time = date_to
        logger.info(f"Production extraction completed for PCN {self.config.facility.pcn}")
    
    async def ensure_workcenter_assets(self):
        """Ensure workcenter assets exist with PCN-aware naming"""
        # Fetch all workcenters if not configured
        if not self.config.workcenter_ids:
            logger.info("Fetching all workcenters to create assets")
            workcenters = await self.fetch_all_workcenters()
            workcenter_data = {wc['id']: wc for wc in workcenters if wc.get('status') == 'Active'}
        else:
            # Use configured IDs but still get details
            workcenter_data = {wc_id: {'id': wc_id, 'name': f'Workcenter {wc_id}'} 
                              for wc_id in self.config.workcenter_ids}
        
        if not workcenter_data:
            logger.warning("No workcenters to create assets for")
            return
        
        # Create root production asset for this facility
        root_id = self.naming.asset_id("PRODUCTION", "ROOT")
        try:
            existing = self.cognite_client.assets.retrieve(external_id=root_id)
            if not existing:
                raise Exception("Not found")
        except:
            try:
                root_asset = Asset(
                    external_id=root_id,
                    name=f"Production - {self.config.facility.name}",
                    description=f"Root production asset for {self.config.facility.name} (PCN: {self.config.facility.pcn})",
                    data_set_id=self.config.dataset_production_id,
                    metadata={**self.naming.get_metadata_tags(), "type": "production_root"}
                )
                self.cognite_client.assets.create(root_asset)
                logger.info(f"Created root production asset for PCN {self.config.facility.pcn}")
            except Exception as e:
                logger.debug(f"Root asset may already exist: {e}")
        
        # Create workcenter assets
        for wc_id, wc_info in workcenter_data.items():
            wc_asset_id = self.naming.asset_id("WC", wc_id)
            try:
                existing = self.cognite_client.assets.retrieve(external_id=wc_asset_id)
                if existing:
                    continue
            except:
                pass
            
            try:
                asset = Asset(
                    external_id=wc_asset_id,
                    name=wc_info.get('name', f"Workcenter {wc_id}"),
                    parent_external_id=root_id,
                    description=f"{wc_info.get('name', f'Workcenter {wc_id}')} at {self.config.facility.name}",
                    data_set_id=self.config.dataset_production_id,
                    metadata={
                        **self.naming.get_metadata_tags(),
                        'workcenter_id': wc_id,
                        'workcenter_type': wc_info.get('type', 'Unknown'),
                        'type': 'workcenter'
                    }
                )
                self.cognite_client.assets.create(asset)
                logger.info(f"Created workcenter asset: {wc_asset_id}")
            except Exception as e:
                logger.debug(f"Workcenter asset may already exist: {e}")
    
    async def run_extraction_cycle(self):
        """Run a single extraction cycle"""
        try:
            logger.info(f"Starting production extraction cycle for PCN {self.config.facility.pcn}")
            
            # Ensure assets exist
            await self.ensure_workcenter_assets()
            
            # Extract workcenter status (real-time) - temporarily disabled due to endpoint issues
            # await self.extract_workcenter_status()
            
            # Extract production entries (historical + new)
            await self.extract_production_entries()
            
            logger.info(f"Production extraction cycle completed for PCN {self.config.facility.pcn}")
            
        except Exception as e:
            logger.error(f"Error in extraction cycle: {e}", exc_info=True)
    
    async def run(self):
        """Main extraction loop"""
        self.running = True
        logger.info(f"Starting Production Extractor for PCN {self.config.facility.pcn} ({self.config.facility.name})")
        
        # Log dataset configuration
        if self.config.dataset_production_id:
            logger.info(f"Using production dataset ID: {self.config.dataset_production_id}")
        else:
            logger.warning("No production dataset ID configured")
        
        # Test connections
        try:
            self.cognite_client.iam.token.inspect()
            logger.info("CDF connection successful")
            
            # Test Plex connection with a simple call
            test_data = await self.fetch_plex_data("/production/v1/production-entries", {"limit": 1})
            if test_data:
                logger.info(f"Plex connection successful for PCN {self.config.facility.pcn}")
            else:
                logger.error("Failed to connect to Plex API")
                return
            
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return
        
        while self.running:
            try:
                await self.run_extraction_cycle()
                
                logger.info(f"Waiting {self.config.extraction_interval} seconds until next extraction...")
                await asyncio.sleep(self.config.extraction_interval)
                
            except KeyboardInterrupt:
                logger.info("Received interrupt signal, shutting down...")
                self.running = False
                
            except Exception as e:
                logger.error(f"Unexpected error in main loop: {e}", exc_info=True)
                await asyncio.sleep(60)


async def main():
    """Main entry point"""
    config = ProductionExtractorConfig.from_env()
    
    # Validate configuration
    required_vars = [
        'PLEX_API_KEY', 'PLEX_CUSTOMER_ID',
        'CDF_HOST', 'CDF_PROJECT', 'CDF_CLIENT_ID', 
        'CDF_CLIENT_SECRET', 'CDF_TOKEN_URL'
    ]
    
    missing = [var for var in required_vars if not os.getenv(var)]
    if missing:
        logger.error(f"Missing required environment variables: {', '.join(missing)}")
        sys.exit(1)
    
    if not config.workcenter_ids:
        logger.warning("No WORKCENTER_IDS configured. Add comma-separated IDs to .env file")
        logger.warning("Example: WORKCENTER_IDS=WC001,WC002,WC003")
    
    logger.info(f"Starting production extractor for Facility: {config.facility.name} (PCN: {config.facility.pcn})")
    
    extractor = PlexProductionExtractor(config)
    
    try:
        await extractor.run()
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())