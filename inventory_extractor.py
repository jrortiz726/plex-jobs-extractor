# inventory_extractor.py
"""
Inventory Management Extractor for Plex MES

Extracts and tracks inventory data including:
- Containers and bins
- Storage locations  
- Material lots
- Inventory movements and transactions
- Work in Progress (WIP)
- Container status tracking

ALL IDs include PCN prefix for multi-facility support.
"""

import os
import sys
import json
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

from multi_facility_config import MultiTenantNamingConvention, FacilityConfig
from cdf_utils import CDFDeduplicationHelper, StateTracker

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class InventoryConfig:
    """Configuration for Inventory Extractor with PCN support"""
    # Required Plex API settings
    plex_api_key: str
    plex_customer_id: str  # PCN
    
    # Required CDF settings
    cdf_host: str
    cdf_project: str
    cdf_client_id: str
    cdf_client_secret: str
    cdf_token_url: str
    
    # Facility information
    facility: FacilityConfig
    
    # Inventory specific settings
    container_ids: List[str] = None
    location_ids: List[str] = None
    
    # Optional settings
    plex_base_url: str = "https://connect.plex.com"
    extraction_interval: int = 300  # 5 minutes for inventory
    batch_size: int = 5000
    max_retries: int = 3
    retry_delay: int = 5
    
    # Dataset ID
    dataset_inventory_id: Optional[int] = None
    
    @classmethod
    def from_env(cls) -> 'InventoryConfig':
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
        
        # Get container and location IDs from environment
        container_ids_str = os.getenv('CONTAINER_IDS', '')
        container_ids = [c.strip() for c in container_ids_str.split(',') if c.strip()] if container_ids_str else []
        
        location_ids_str = os.getenv('LOCATION_IDS', '')
        location_ids = [l.strip() for l in location_ids_str.split(',') if l.strip()] if location_ids_str else []
        
        inventory_id = get_int_env(f'CDF_DATASET_{pcn}_INVENTORY') or \
                      get_int_env('CDF_DATASET_PLEXINVENTORY')
        
        return cls(
            plex_api_key=os.getenv('PLEX_API_KEY'),
            plex_customer_id=pcn,
            facility=facility,
            cdf_host=os.getenv('CDF_HOST'),
            cdf_project=os.getenv('CDF_PROJECT'),
            cdf_client_id=os.getenv('CDF_CLIENT_ID'),
            cdf_client_secret=os.getenv('CDF_CLIENT_SECRET'),
            cdf_token_url=os.getenv('CDF_TOKEN_URL'),
            container_ids=container_ids,
            location_ids=location_ids,
            extraction_interval=get_int_env('INVENTORY_EXTRACTION_INTERVAL', 300),
            batch_size=get_int_env('BATCH_SIZE', 5000),
            dataset_inventory_id=inventory_id
        )


class InventoryExtractor:
    """Extracts and tracks inventory data in CDF with PCN support"""
    
    def __init__(self, config: InventoryConfig):
        self.config = config
        self.naming = MultiTenantNamingConvention(config.facility)
        self.cognite_client = self._init_cognite_client()
        self.plex_headers = {
            'X-Plex-Connect-Api-Key': config.plex_api_key,
            'X-Plex-Connect-Customer-Id': config.plex_customer_id,
            'Content-Type': 'application/json'
        }
        
        # State tracking
        self.last_movement_extraction: Optional[datetime] = None
        self.time_series_cache: Dict[str, TimeSeries] = {}
        self.running = False
        
        # Initialize deduplication helper and state tracker
        self.dedup_helper = CDFDeduplicationHelper(self.cognite_client)
        self.state_tracker = StateTracker(f"inventory_state_{config.facility.pcn}.json")
        
        # Refresh cache with facility-specific prefix
        self.dedup_helper.refresh_cache(external_id_prefix=f"{config.facility.pcn}_")
        
        logger.info(f"Initialized Inventory Extractor for PCN {config.facility.pcn}")
        
    def _init_cognite_client(self) -> CogniteClient:
        """Initialize Cognite client"""
        creds = OAuthClientCredentials(
            token_url=self.config.cdf_token_url,
            client_id=self.config.cdf_client_id,
            client_secret=self.config.cdf_client_secret,
            scopes=["user_impersonation"]
        )
        
        config = ClientConfig(
            client_name=f"plex-inventory-extractor-{self.config.facility.pcn}",
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
                        elif response.status == 404:
                            logger.warning(f"Endpoint not found: {endpoint}")
                            return {'data': []}
                        else:
                            error_text = await response.text()
                            logger.error(f"API error {response.status}: {error_text}")
                            
            except Exception as e:
                logger.error(f"Error fetching {endpoint}: {e}")
                
            if attempt < self.config.max_retries - 1:
                await asyncio.sleep(self.config.retry_delay * (attempt + 1))
                
        return {'data': []}
    
    def ensure_time_series_exists(self, external_id: str, name: str, unit: str = None, is_string: bool = False):
        """Ensure a time series exists in CDF"""
        if external_id in self.time_series_cache:
            return
        
        try:
            ts = self.cognite_client.time_series.retrieve(external_id=external_id)
            if ts:
                self.time_series_cache[external_id] = ts
                return
        except:
            pass
        
        ts = TimeSeries(
            external_id=external_id,
            name=name,
            description=f"{name} for {self.config.facility.facility_name}",
            data_set_id=self.config.dataset_inventory_id,
            unit=unit,
            is_string=is_string,
            metadata=self.naming.get_metadata_tags()
        )
        
        # Use deduplication helper to create only if it doesn't exist
        result = self.dedup_helper.upsert_timeseries([ts])
        if result['created']:
            self.time_series_cache[external_id] = result['created'][0]
            logger.info(f"Created time series: {external_id}")
        else:
            # Already exists, add to cache
            existing = self.cognite_client.time_series.retrieve(external_id=external_id)
            if existing:
                self.time_series_cache[external_id] = existing
            logger.debug(f"Time series already exists: {external_id}")
    
    async def ensure_inventory_hierarchy(self):
        """Ensure inventory asset hierarchy exists"""
        # Create inventory root
        inventory_root_id = self.naming.asset_id("INVENTORY", "ROOT")
        
        # First ensure facility root exists
        facility_root_id = self.naming.asset_id("FACILITY", "ROOT")
        facility_root = Asset(
            external_id=facility_root_id,
            name=f"{self.config.facility.facility_name} - Facility Root",
            description=f"Root facility asset for {self.config.facility.facility_name}",
            data_set_id=self.config.dataset_inventory_id or self.config.dataset_master_id,
            metadata={**self.naming.get_metadata_tags(), "type": "facility_root"}
        )
        
        # Create facility root first
        try:
            self.cognite_client.assets.retrieve(external_id=facility_root_id)
            logger.debug(f"Facility root exists: {facility_root_id}")
        except:
            try:
                self.cognite_client.assets.create(facility_root)
                logger.info(f"Created facility root: {facility_root_id}")
            except Exception as e:
                if "already exists" not in str(e).lower():
                    logger.error(f"Failed to create facility root: {e}")
        
        # Now create inventory root with facility root as parent
        try:
            self.cognite_client.assets.retrieve(external_id=inventory_root_id)
            logger.debug(f"Inventory root exists: {inventory_root_id}")
        except:
            root_asset = Asset(
                external_id=inventory_root_id,
                name=f"Inventory - {self.config.facility.facility_name}",
                parent_external_id=facility_root_id,
                description=f"Inventory root for {self.config.facility.facility_name}",
                data_set_id=self.config.dataset_inventory_id or self.config.dataset_master_id,
                metadata={**self.naming.get_metadata_tags(), "type": "inventory_root"}
            )
            try:
                self.cognite_client.assets.create(root_asset)
                logger.info(f"Created inventory root: {inventory_root_id}")
            except Exception as e:
                if "already exists" not in str(e).lower():
                    logger.error(f"Failed to create inventory root: {e}")
                    raise
        
        return inventory_root_id
    
    async def extract_containers(self):
        """Extract container/bin information"""
        logger.info(f"Extracting containers for PCN {self.config.facility.pcn}")
        
        inventory_root = await self.ensure_inventory_hierarchy()
        
        # Get all containers or specific ones if configured
        if self.config.container_ids:
            containers = []
            for container_serial in self.config.container_ids:
                # Use serial number to get specific container
                data = await self.fetch_plex_data(f"/inventory/v1/inventory-tracking/containers/{container_serial}")
                if data.get('data'):
                    containers.append(data['data'])
        else:
            data = await self.fetch_plex_data("/inventory/v1/inventory-tracking/containers")
            containers = data.get('data', [])
        
        assets = []
        events = []
        datapoints_by_ts = {}
        
        for container in containers:
            container_id = container.get('containerId')
            if not container_id:
                continue
            
            external_id = self.naming.asset_id("CONTAINER", container_id)
            
            # Create container asset
            location_id = container.get('locationId')
            if location_id:
                parent_id = self.naming.asset_id("LOCATION", location_id)
            else:
                parent_id = inventory_root
            
            asset = Asset(
                external_id=external_id,
                name=container.get('name', f'Container {container_id}'),
                parent_external_id=parent_id,
                description=container.get('description', ''),
                data_set_id=self.config.dataset_inventory_id,
                metadata={
                    **self.naming.get_metadata_tags(),
                    'container_id': str(container_id),
                    'container_type': container.get('type', 'standard'),
                    'location_id': str(location_id) if location_id else '',
                    'part_number': container.get('partNumber', ''),
                    'lot_number': container.get('lotNumber', ''),
                    'quantity': str(container.get('quantity', 0)),
                    'unit_of_measure': container.get('uom', ''),
                    'status': container.get('status', 'active'),
                    'last_updated': datetime.now(timezone.utc).isoformat()
                }
            )
            assets.append(asset)
            
            # Create container status event
            timestamp = int(datetime.now(timezone.utc).timestamp() * 1000)
            event = Event(
                external_id=self.naming.event_id("CONTAINER_STATUS", container_id, timestamp/1000),
                type="inventory_status",
                subtype="container_update",
                start_time=timestamp,
                end_time=timestamp,
                description=f"Container {container_id} status update",
                asset_external_ids=[external_id],
                data_set_id=self.config.dataset_inventory_id,
                metadata={
                    **self.naming.get_metadata_tags(),
                    'container_id': str(container_id),
                    'quantity': str(container.get('quantity', 0)),
                    'part_number': container.get('partNumber', ''),
                    'status': container.get('status', '')
                }
            )
            events.append(event)
            
            # Track container fill level as time series
            fill_ts_id = self.naming.timeseries_id("CONTAINER", container_id, "FILL_LEVEL")
            self.ensure_time_series_exists(
                fill_ts_id,
                f"Container {container_id} Fill Level",
                unit="%"
            )
            
            if fill_ts_id not in datapoints_by_ts:
                datapoints_by_ts[fill_ts_id] = []
            
            max_capacity = container.get('maxCapacity', 100)
            current_qty = container.get('quantity', 0)
            fill_level = (current_qty / max_capacity * 100) if max_capacity > 0 else 0
            datapoints_by_ts[fill_ts_id].append((timestamp, fill_level))
        
        # Upload assets using deduplication
        if assets:
            result = self.dedup_helper.upsert_assets(assets)
            logger.info(f"Containers: {len(result['created'])} created, "
                       f"{len(result['updated'])} updated, {len(result['skipped'])} unchanged")
        
        # Upload events with deduplication
        if events:
            created_count = self.dedup_helper.create_events_batch(events)
            logger.info(f"Created {created_count} new container status events")
        
        # Upload time series data
        if datapoints_by_ts:
            try:
                datapoints_list = [
                    {"external_id": ts_id, "datapoints": points}
                    for ts_id, points in datapoints_by_ts.items()
                ]
                self.cognite_client.time_series.data.insert_multiple(datapoints_list)
                logger.info(f"Uploaded container fill levels for {len(datapoints_by_ts)} containers")
            except Exception as e:
                logger.error(f"Failed to upload time series data: {e}")
    
    async def extract_locations(self):
        """Extract storage location information"""
        logger.info(f"Extracting locations for PCN {self.config.facility.pcn}")
        
        inventory_root = await self.ensure_inventory_hierarchy()
        
        # Get locations
        if self.config.location_ids:
            locations = []
            for location_id in self.config.location_ids:
                # Query for specific location
                params = {'locationId': location_id}
                data = await self.fetch_plex_data("/inventory/v1/inventory-definitions/locations", params)
                if data.get('data'):
                    locations.extend(data.get('data', []))
        else:
            data = await self.fetch_plex_data("/inventory/v1/inventory-definitions/locations")
            locations = data.get('data', [])
        
        assets = []
        datapoints_by_ts = {}
        
        for location in locations:
            location_id = location.get('locationId')
            if not location_id:
                continue
            
            external_id = self.naming.asset_id("LOCATION", location_id)
            
            # Determine parent (building or inventory root)
            building_id = location.get('buildingId')
            if building_id:
                parent_id = self.naming.asset_id("BUILDING", building_id)
            else:
                parent_id = inventory_root
            
            asset = Asset(
                external_id=external_id,
                name=location.get('name', f'Location {location_id}'),
                parent_external_id=parent_id,
                description=location.get('description', ''),
                data_set_id=self.config.dataset_inventory_id,
                metadata={
                    **self.naming.get_metadata_tags(),
                    'location_id': str(location_id),
                    'location_type': location.get('type', 'storage'),
                    'building_id': str(building_id) if building_id else '',
                    'aisle': location.get('aisle', ''),
                    'row': location.get('row', ''),
                    'bin': location.get('bin', ''),
                    'capacity': str(location.get('capacity', 0)),
                    'status': location.get('status', 'active'),
                    'last_updated': datetime.now(timezone.utc).isoformat()
                }
            )
            assets.append(asset)
            
            # Track inventory level at location
            level_ts_id = self.naming.timeseries_id("LOCATION", location_id, "INVENTORY_LEVEL")
            self.ensure_time_series_exists(
                level_ts_id,
                f"Location {location_id} Inventory Level",
                unit="pieces"
            )
            
            timestamp = int(datetime.now(timezone.utc).timestamp() * 1000)
            if level_ts_id not in datapoints_by_ts:
                datapoints_by_ts[level_ts_id] = []
            
            current_inventory = location.get('currentInventory', 0)
            datapoints_by_ts[level_ts_id].append((timestamp, current_inventory))
        
        # Upload assets using deduplication
        if assets:
            result = self.dedup_helper.upsert_assets(assets)
            logger.info(f"Locations: {len(result['created'])} created, "
                       f"{len(result['updated'])} updated, {len(result['skipped'])} unchanged")
        
        # Upload time series data
        if datapoints_by_ts:
            try:
                datapoints_list = [
                    {"external_id": ts_id, "datapoints": points}
                    for ts_id, points in datapoints_by_ts.items()
                ]
                self.cognite_client.time_series.data.insert_multiple(datapoints_list)
                logger.info(f"Uploaded inventory levels for {len(datapoints_by_ts)} locations")
            except Exception as e:
                logger.error(f"Failed to upload time series data: {e}")
    
    async def extract_inventory_movements(self):
        """Extract inventory movement transactions"""
        # TODO: Movements endpoint needs verification
        logger.info("Inventory movements extraction temporarily disabled - endpoint needs verification")
        return
        
    async def extract_inventory_movements_disabled(self):
        """Extract inventory movement transactions"""
        logger.info(f"Extracting inventory movements for PCN {self.config.facility.pcn}")
        
        # Determine time range
        default_start = os.getenv('EXTRACTION_START_DATE', '2024-01-01T00:00:00Z')
        try:
            initial_date = datetime.fromisoformat(default_start.replace('Z', '+00:00'))
        except:
            initial_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
        
        date_from = self.last_movement_extraction or initial_date
        date_to = datetime.now(timezone.utc)
        
        params = {
            'limit': self.config.batch_size,
            'offset': 0,
            'dateFrom': date_from.isoformat(),
            'dateTo': date_to.isoformat()
        }
        
        all_movements = []
        
        # Fetch movements
        while True:
            data = await self.fetch_plex_data("/inventory/v1/movements", params)
            movements = data.get('data', [])
            
            if not movements:
                break
            
            all_movements.extend(movements)
            logger.info(f"Fetched {len(movements)} movements (total: {len(all_movements)})")
            
            if len(movements) < self.config.batch_size:
                break
            
            params['offset'] += self.config.batch_size
        
        # Process movements into events
        events = []
        
        for movement in all_movements:
            movement_id = movement.get('movementId')
            if not movement_id:
                continue
            
            timestamp = int(datetime.fromisoformat(
                movement['timestamp'].replace('Z', '+00:00')
            ).timestamp() * 1000)
            
            # Determine asset references
            asset_refs = []
            if movement.get('containerId'):
                asset_refs.append(self.naming.asset_id("CONTAINER", movement['containerId']))
            if movement.get('fromLocationId'):
                asset_refs.append(self.naming.asset_id("LOCATION", movement['fromLocationId']))
            if movement.get('toLocationId'):
                asset_refs.append(self.naming.asset_id("LOCATION", movement['toLocationId']))
            
            event = Event(
                external_id=self.naming.event_id("INV_MOVEMENT", movement_id, timestamp/1000),
                type="inventory_movement",
                subtype=movement.get('movementType', 'transfer'),
                start_time=timestamp,
                end_time=timestamp,
                description=f"Inventory movement: {movement.get('description', '')}",
                asset_external_ids=asset_refs if asset_refs else None,
                data_set_id=self.config.dataset_inventory_id,
                metadata={
                    **self.naming.get_metadata_tags(),
                    'movement_id': str(movement_id),
                    'movement_type': movement.get('movementType', ''),
                    'part_number': movement.get('partNumber', ''),
                    'quantity': str(movement.get('quantity', 0)),
                    'unit_of_measure': movement.get('uom', ''),
                    'from_location': movement.get('fromLocationId', ''),
                    'to_location': movement.get('toLocationId', ''),
                    'container_id': movement.get('containerId', ''),
                    'lot_number': movement.get('lotNumber', ''),
                    'reason_code': movement.get('reasonCode', ''),
                    'operator': movement.get('operator', ''),
                    'job_number': movement.get('jobNumber', ''),
                    'reference_number': movement.get('referenceNumber', '')
                }
            )
            events.append(event)
        
        # Upload events
        if events:
            batch_size = 1000
            for i in range(0, len(events), batch_size):
                batch = events[i:i+batch_size]
                try:
                    self.cognite_client.events.create(batch)
                    logger.info(f"Created {len(batch)} movement events")
                except Exception as e:
                    logger.error(f"Error creating movement events: {e}")
        
        self.last_movement_extraction = date_to
        logger.info(f"Processed {len(events)} inventory movements")
    
    async def extract_wip(self):
        """Extract work in progress data"""
        # TODO: WIP endpoint needs different approach - will need to derive from other data
        logger.info("WIP extraction temporarily disabled - needs alternative implementation")
        return
        
    async def extract_wip_disabled(self):
        """Extract Work in Progress data"""
        logger.info(f"Extracting WIP data for PCN {self.config.facility.pcn}")
        
        data = await self.fetch_plex_data("/inventory/v1/wip")
        wip_records = data.get('data', [])
        
        datapoints_by_ts = {}
        events = []
        
        for wip in wip_records:
            job_id = wip.get('jobNumber')
            if not job_id:
                continue
            
            # Create WIP time series for each job
            wip_ts_id = self.naming.timeseries_id("JOB", job_id, "WIP_QUANTITY")
            self.ensure_time_series_exists(
                wip_ts_id,
                f"Job {job_id} WIP Quantity",
                unit="pieces"
            )
            
            timestamp = int(datetime.now(timezone.utc).timestamp() * 1000)
            
            if wip_ts_id not in datapoints_by_ts:
                datapoints_by_ts[wip_ts_id] = []
            
            wip_quantity = wip.get('quantity', 0)
            datapoints_by_ts[wip_ts_id].append((timestamp, wip_quantity))
            
            # Create WIP event
            event = Event(
                external_id=self.naming.event_id("WIP_UPDATE", job_id, timestamp/1000),
                type="wip_status",
                subtype="wip_update",
                start_time=timestamp,
                end_time=timestamp,
                description=f"WIP update for job {job_id}",
                asset_external_ids=[self.naming.asset_id("JOB", job_id)],
                data_set_id=self.config.dataset_inventory_id,
                metadata={
                    **self.naming.get_metadata_tags(),
                    'job_number': str(job_id),
                    'wip_quantity': str(wip_quantity),
                    'wip_value': str(wip.get('value', 0)),
                    'workcenter_id': wip.get('workcenterId', ''),
                    'operation_sequence': str(wip.get('operationSequence', '')),
                    'part_number': wip.get('partNumber', ''),
                    'location_id': wip.get('locationId', '')
                }
            )
            events.append(event)
        
        # Upload time series data
        if datapoints_by_ts:
            try:
                datapoints_list = [
                    {"external_id": ts_id, "datapoints": points}
                    for ts_id, points in datapoints_by_ts.items()
                ]
                self.cognite_client.time_series.data.insert_multiple(datapoints_list)
                logger.info(f"Uploaded WIP quantities for {len(datapoints_by_ts)} jobs")
            except Exception as e:
                logger.error(f"Failed to upload WIP time series: {e}")
        
        # Upload events
        if events:
            try:
                self.cognite_client.events.create(events)
                logger.info(f"Created {len(events)} WIP events")
            except Exception as e:
                logger.error(f"Error creating WIP events: {e}")
    
    async def run_extraction_cycle(self):
        """Run a single extraction cycle"""
        try:
            logger.info(f"Starting inventory extraction cycle for PCN {self.config.facility.pcn}")
            
            # Extract in logical order
            await self.extract_locations()  # Locations first (parents of containers)
            await self.extract_containers()  # Containers
            await self.extract_inventory_movements()  # Movement transactions
            await self.extract_wip()  # Work in Progress
            
            logger.info(f"Inventory extraction cycle completed for PCN {self.config.facility.pcn}")
            
        except Exception as e:
            logger.error(f"Error in inventory extraction cycle: {e}", exc_info=True)
    
    async def run(self):
        """Main extraction loop"""
        self.running = True
        logger.info(f"Starting Inventory Extractor for PCN {self.config.facility.pcn}")
        
        if self.config.dataset_inventory_id:
            logger.info(f"Using inventory dataset ID: {self.config.dataset_inventory_id}")
        else:
            logger.warning("No inventory dataset configured")
        
        # Test connection
        try:
            self.cognite_client.iam.token.inspect()
            logger.info("CDF connection successful")
        except Exception as e:
            logger.error(f"CDF connection failed: {e}")
            return
        
        while self.running:
            try:
                await self.run_extraction_cycle()
                
                logger.info(f"Waiting {self.config.extraction_interval} seconds until next extraction...")
                await asyncio.sleep(self.config.extraction_interval)
                
            except KeyboardInterrupt:
                logger.info("Shutting down...")
                self.running = False
            except Exception as e:
                logger.error(f"Unexpected error: {e}", exc_info=True)
                await asyncio.sleep(60)


async def main():
    """Main entry point"""
    config = InventoryConfig.from_env()
    
    # Validate
    required = ['PLEX_API_KEY', 'PLEX_CUSTOMER_ID', 'CDF_HOST',
                'CDF_PROJECT', 'CDF_CLIENT_ID', 'CDF_CLIENT_SECRET', 'CDF_TOKEN_URL']
    missing = [var for var in required if not os.getenv(var)]
    
    if missing:
        logger.error(f"Missing required environment variables: {', '.join(missing)}")
        sys.exit(1)
    
    if not config.container_ids:
        logger.warning("No CONTAINER_IDS configured. Will extract all containers.")
    
    if not config.location_ids:
        logger.warning("No LOCATION_IDS configured. Will extract all locations.")
    
    logger.info(f"Starting Inventory Extractor for {config.facility.facility_name} (PCN: {config.facility.pcn})")
    
    extractor = InventoryExtractor(config)
    
    try:
        await extractor.run()
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())