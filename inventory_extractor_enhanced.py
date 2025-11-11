#!/usr/bin/env python3
"""
Enhanced Inventory Extractor with All Improvements
- Full async/await implementation
- Type hints throughout
- Error handling with retry
- Asset ID resolution
- Structured logging
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any, Tuple, Final, TypeAlias
from dataclasses import dataclass, field
from enum import StrEnum, auto

import structlog
from cognite.client.data_classes import Asset, Event, TimeSeries, Datapoints

from base_extractor_enhanced import (
    BaseExtractor, BaseExtractorConfig, ExtractionResult,
    DatasetType, with_retry
)
from error_handling import PlexAPIError

# Setup structured logging
logger = structlog.get_logger(__name__)

# Type aliases
ContainerId: TypeAlias = str
LocationId: TypeAlias = str
PartId: TypeAlias = str
SerialNumber: TypeAlias = str


class ContainerStatus(StrEnum):
    """Container status enumeration"""
    ACTIVE = auto()
    INACTIVE = auto()
    QUARANTINE = auto()
    SHIPPED = auto()
    EMPTY = auto()


@dataclass
class Container:
    """Container data structure"""
    id: ContainerId
    serial_number: SerialNumber
    status: ContainerStatus
    location_id: Optional[LocationId] = None
    location_name: Optional[str] = None
    part_id: Optional[PartId] = None
    part_name: Optional[str] = None
    quantity: int = 0
    max_quantity: int = 0
    fill_percentage: float = 0.0
    last_updated: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    expiration_date: Optional[datetime] = None
    lot_number: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Location:
    """Storage location data structure"""
    id: LocationId
    name: str
    type: str  # warehouse, production, shipping, etc.
    building: Optional[str] = None
    zone: Optional[str] = None
    aisle: Optional[str] = None
    bin: Optional[str] = None
    capacity: int = 0
    current_occupancy: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class InventoryMovement:
    """Inventory movement/transaction"""
    id: str
    movement_type: str  # receipt, issue, transfer, adjustment
    container_id: Optional[ContainerId] = None
    part_id: Optional[PartId] = None
    quantity: int = 0
    from_location: Optional[LocationId] = None
    to_location: Optional[LocationId] = None
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    operator: Optional[str] = None
    reference_number: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


class InventoryExtractorConfig(BaseExtractorConfig):
    """Configuration specific to inventory extractor"""
    extract_containers: bool = True
    extract_locations: bool = True
    extract_movements: bool = True
    extract_wip: bool = True
    lookback_hours: int = 24
    
    @classmethod
    def from_env(cls) -> InventoryExtractorConfig:
        """Load configuration from environment"""
        import os
        base = BaseExtractorConfig.from_env('inventory')
        return cls(
            **base.dict(),
            extract_containers=os.getenv('EXTRACT_CONTAINERS', 'true').lower() == 'true',
            extract_locations=os.getenv('EXTRACT_LOCATIONS', 'true').lower() == 'true',
            extract_movements=os.getenv('EXTRACT_MOVEMENTS', 'true').lower() == 'true',
            extract_wip=os.getenv('EXTRACT_WIP', 'true').lower() == 'true',
            lookback_hours=int(os.getenv('INVENTORY_LOOKBACK_HOURS', '24'))
        )


class EnhancedInventoryExtractor(BaseExtractor):
    """Enhanced inventory extractor with all improvements"""
    
    def __init__(self, config: Optional[InventoryExtractorConfig] = None):
        """Initialize with enhanced configuration"""
        config = config or InventoryExtractorConfig.from_env()
        super().__init__(config, 'inventory')
        
        self.config: Final[InventoryExtractorConfig] = config
        self.processed_movements: set[str] = set()
        self.location_cache: Dict[LocationId, Location] = {}
        self.container_cache: Dict[ContainerId, Container] = {}
        
        self.logger.info(
            "inventory_extractor_initialized",
            extract_containers=config.extract_containers,
            extract_locations=config.extract_locations,
            extract_movements=config.extract_movements,
            extract_wip=config.extract_wip,
            lookback_hours=config.lookback_hours
        )
    
    def get_required_datasets(self) -> List[str]:
        """Inventory requires inventory and master datasets"""
        return ['inventory', 'master']
    
    async def extract(self) -> ExtractionResult:
        """Main extraction with concurrent operations"""
        start_time = datetime.now(timezone.utc)
        result = ExtractionResult(
            success=True,
            items_processed=0,
            duration_ms=0
        )
        
        try:
            # Ensure inventory hierarchy exists
            await self._ensure_inventory_hierarchy()
            
            tasks = []
            
            # Create extraction tasks based on configuration
            async with asyncio.TaskGroup() as tg:
                if self.config.extract_locations:
                    tasks.append(tg.create_task(self._extract_locations()))
                
                if self.config.extract_containers:
                    tasks.append(tg.create_task(self._extract_containers()))
                
                if self.config.extract_movements:
                    tasks.append(tg.create_task(self._extract_movements()))
                
                if self.config.extract_wip:
                    tasks.append(tg.create_task(self._extract_wip()))
            
            # Aggregate results
            for task in tasks:
                task_result = await task
                result.items_processed += task_result.items_processed
                if not task_result.success:
                    result.success = False
                    result.errors.extend(task_result.errors)
            
            self.logger.info(
                "inventory_extraction_completed",
                items_processed=result.items_processed,
                success=result.success
            )
            
        except Exception as e:
            result.success = False
            result.errors.append(str(e))
            self.logger.error("inventory_extraction_failed", error=str(e), exc_info=True)
        
        result.duration_ms = (datetime.now(timezone.utc) - start_time).total_seconds() * 1000
        return result
    
    async def _ensure_inventory_hierarchy(self) -> None:
        """Ensure inventory asset hierarchy exists"""
        try:
            # Create root assets
            root_assets = [
                Asset(
                    external_id=self.create_asset_external_id('inventory_root', self.config.facility.pcn),
                    name=f"{self.config.facility.facility_name} - Inventory",
                    parent_external_id=self.create_asset_external_id('facility', self.config.facility.pcn),
                    description="Root asset for inventory hierarchy",
                    metadata={
                        **self.naming.get_metadata_tags(),
                        'asset_type': 'inventory_root'
                    },
                    data_set_id=self.get_dataset_id('inventory')
                ),
                Asset(
                    external_id=self.create_asset_external_id('locations_root', self.config.facility.pcn),
                    name=f"{self.config.facility.facility_name} - Locations",
                    parent_external_id=self.create_asset_external_id('inventory_root', self.config.facility.pcn),
                    description="Root asset for storage locations",
                    metadata={
                        **self.naming.get_metadata_tags(),
                        'asset_type': 'locations_root'
                    },
                    data_set_id=self.get_dataset_id('inventory')
                ),
                Asset(
                    external_id=self.create_asset_external_id('containers_root', self.config.facility.pcn),
                    name=f"{self.config.facility.facility_name} - Containers",
                    parent_external_id=self.create_asset_external_id('inventory_root', self.config.facility.pcn),
                    description="Root asset for containers",
                    metadata={
                        **self.naming.get_metadata_tags(),
                        'asset_type': 'containers_root'
                    },
                    data_set_id=self.get_dataset_id('inventory')
                )
            ]
            
            await self.create_assets_with_retry(root_assets, resolve_parents=True)
            
        except Exception as e:
            self.logger.error("inventory_hierarchy_creation_error", error=str(e))
    
    @with_retry(max_attempts=3)
    async def _extract_locations(self) -> ExtractionResult:
        """Extract storage locations"""
        result = ExtractionResult(success=True, items_processed=0, duration_ms=0)
        
        try:
            # Fetch all locations
            locations = await self._fetch_locations()
            
            if not locations:
                self.logger.info("no_locations_found")
                return result
            
            # Create location assets
            assets = []
            for location in locations:
                asset = self._create_location_asset(location)
                if asset:
                    assets.append(asset)
                    # Cache location
                    self.location_cache[location.id] = location
            
            # Create in CDF
            if assets:
                created, failed = await self.create_assets_with_retry(
                    assets,
                    resolve_parents=True
                )
                result.items_processed = len(created)
            
            self.logger.info(
                "locations_extracted",
                locations_found=len(locations),
                assets_created=result.items_processed
            )
            
        except Exception as e:
            result.success = False
            result.errors.append(f"Location extraction failed: {e}")
            self.logger.error("location_extraction_error", error=str(e))
        
        return result
    
    @with_retry(max_attempts=3)
    async def _extract_containers(self) -> ExtractionResult:
        """Extract containers"""
        result = ExtractionResult(success=True, items_processed=0, duration_ms=0)
        
        try:
            # Fetch all containers
            containers = await self._fetch_containers()
            
            if not containers:
                self.logger.info("no_containers_found")
                return result
            
            # Create container assets and status events
            assets = []
            events = []
            
            for container in containers:
                # Create asset
                asset = self._create_container_asset(container)
                if asset:
                    assets.append(asset)
                    # Cache container
                    self.container_cache[container.id] = container
                
                # Create status event
                event = self._create_container_event(container)
                if event:
                    events.append(event)
            
            # Create in CDF
            if assets:
                created_assets, failed_assets = await self.create_assets_with_retry(
                    assets,
                    resolve_parents=True
                )
                result.items_processed += len(created_assets)
            
            if events:
                created_events, duplicate_events = await self.create_events_with_retry(
                    events,
                    link_assets=True
                )
                result.items_processed += len(created_events)
            
            # Create fill level time series
            await self._create_container_timeseries(containers)
            
            self.logger.info(
                "containers_extracted",
                containers_found=len(containers),
                assets_created=len(assets),
                events_created=len(events)
            )
            
        except Exception as e:
            result.success = False
            result.errors.append(f"Container extraction failed: {e}")
            self.logger.error("container_extraction_error", error=str(e))
        
        return result
    
    @with_retry(max_attempts=3)
    async def _extract_movements(self) -> ExtractionResult:
        """Extract inventory movements"""
        result = ExtractionResult(success=True, items_processed=0, duration_ms=0)
        
        try:
            # Calculate time range
            end_time = datetime.now(timezone.utc)
            start_time = end_time - timedelta(hours=self.config.lookback_hours)
            
            # Fetch movements - make this optional as it may not be available
            try:
                movements = await self._fetch_movements(start_time, end_time)
                
                if not movements:
                    self.logger.info("no_movements_found")
                else:
                    # Process movements if we got any
                    pass  # Continue to create movement events below
            except Exception as e:
                self.logger.warning("movements_fetch_skipped", error=str(e), 
                                  reason="Container movements API may not be available")
                movements = []  # Continue without movements
            
            # Create movement events
            events = []
            for movement in movements:
                event = self._create_movement_event(movement)
                if event:
                    events.append(event)
            
            # Create in CDF
            if events:
                created, duplicates = await self.create_events_with_retry(
                    events,
                    link_assets=True
                )
                result.items_processed = len(created)
                
                # Update processed set
                self.processed_movements.update(created)
            
            self.logger.info(
                "movements_extracted",
                movements_found=len(movements),
                events_created=result.items_processed
            )
            
        except Exception as e:
            result.success = False
            result.errors.append(f"Movement extraction failed: {e}")
            self.logger.error("movement_extraction_error", error=str(e))
        
        return result
    
    @with_retry(max_attempts=3)
    async def _extract_wip(self) -> ExtractionResult:
        """Extract work-in-progress inventory"""
        result = ExtractionResult(success=True, items_processed=0, duration_ms=0)
        
        try:
            # Fetch WIP containers
            wip_containers = await self._fetch_wip_containers()
            
            if not wip_containers:
                self.logger.info("no_wip_containers_found")
                return result
            
            # Create WIP events
            events = []
            for container in wip_containers:
                event = self._create_wip_event(container)
                if event:
                    events.append(event)
            
            # Create in CDF
            if events:
                created, duplicates = await self.create_events_with_retry(
                    events,
                    link_assets=True
                )
                result.items_processed = len(created)
            
            self.logger.info(
                "wip_extracted",
                wip_containers=len(wip_containers),
                events_created=result.items_processed
            )
            
        except Exception as e:
            result.success = False
            result.errors.append(f"WIP extraction failed: {e}")
            self.logger.error("wip_extraction_error", error=str(e))
        
        return result
    
    async def _fetch_locations(self) -> List[Location]:
        """Fetch all storage locations"""
        endpoint = "/inventory/v1/inventory-definitions/locations"
        
        try:
            data = await self.fetch_plex_data(endpoint)
            
            locations = []
            for loc_data in data if isinstance(data, list) else data.get('data', []):
                location = self._parse_location(loc_data)
                if location:
                    locations.append(location)
            
            return locations
            
        except Exception as e:
            self.logger.error("fetch_locations_error", error=str(e))
            return []
    
    async def _fetch_containers(self) -> List[Container]:
        """Fetch all containers"""
        endpoint = "/inventory/v1/inventory-tracking/containers"
        
        all_containers = []
        offset = 0
        
        while True:
            params = {'limit': 1000, 'offset': offset}
            
            try:
                data = await self.fetch_plex_data(endpoint, params)
                
                if not data:
                    break
                
                containers_raw = data if isinstance(data, list) else data.get('data', [])
                
                for cont_data in containers_raw:
                    container = self._parse_container(cont_data)
                    if container:
                        all_containers.append(container)
                
                if len(containers_raw) < 1000:
                    break
                
                offset += len(containers_raw)
                
            except Exception as e:
                self.logger.error("fetch_containers_error", error=str(e))
                break
        
        return all_containers
    
    async def _fetch_movements(
        self,
        start_time: datetime,
        end_time: datetime
    ) -> List[InventoryMovement]:
        """Fetch inventory movements in time range"""
        endpoint = "/inventory/v1/inventory-history/container-location-moves"
        
        # beginDate and endDate are REQUIRED parameters
        # Try simpler format without microseconds
        params = {
            'beginDate': start_time.strftime('%Y-%m-%dT%H:%M:%SZ'),
            'endDate': end_time.strftime('%Y-%m-%dT%H:%M:%SZ')
        }
        
        # Note: limit is not mentioned in the API docs, so removing it
        # Other optional params: serialNo, partId, partOperationId, locationId, etc.
        
        try:
            data = await self.fetch_plex_data(endpoint, params)
            
            # Parse movements
            all_movements = []
            movements_raw = data if isinstance(data, list) else data.get('data', [])
            
            for mov_data in movements_raw:
                movement = self._parse_movement(mov_data)
                if movement:
                    all_movements.append(movement)
            
            self.logger.info("movements_fetched", count=len(all_movements))
            return all_movements
            
        except Exception as e:
            self.logger.error("fetch_movements_error", error=str(e))
            raise  # Re-raise to be caught by the optional handling in extract_movements
    
    async def _fetch_wip_containers(self) -> List[Container]:
        """Fetch work-in-progress containers"""
        # WIP is determined by container status - fetch all containers and filter
        endpoint = "/inventory/v1/inventory-tracking/containers"
        
        try:
            data = await self.fetch_plex_data(endpoint)
            
            wip_containers = []
            for cont_data in data if isinstance(data, list) else data.get('data', []):
                container = self._parse_container(cont_data)
                # WIP = containers not in scrap, waste, or shipped status
                if container and container.status not in ['scrap', 'waste', 'shipped']:
                    wip_containers.append(container)
            
            return wip_containers
            
        except Exception as e:
            self.logger.error("fetch_wip_error", error=str(e))
            return []
    
    def _parse_location(self, data: Dict[str, Any]) -> Optional[Location]:
        """Parse location from API response"""
        loc_id = data.get('id') or data.get('locationId')
        if not loc_id:
            return None
        
        return Location(
            id=str(loc_id),
            name=data.get('name', f"Location {loc_id}"),
            type=data.get('type', 'storage'),
            building=data.get('building'),
            zone=data.get('zone'),
            aisle=data.get('aisle'),
            bin=data.get('bin'),
            capacity=int(data.get('capacity', 0)),
            current_occupancy=int(data.get('currentOccupancy', 0)),
            metadata=data
        )
    
    def _parse_container(self, data: Dict[str, Any]) -> Optional[Container]:
        """Parse container from API response"""
        cont_id = data.get('id') or data.get('containerId')
        if not cont_id:
            return None
        
        # Parse status
        status_str = data.get('status', 'active').lower()
        status = ContainerStatus.ACTIVE
        if 'inactive' in status_str:
            status = ContainerStatus.INACTIVE
        elif 'quarantine' in status_str:
            status = ContainerStatus.QUARANTINE
        elif 'shipped' in status_str:
            status = ContainerStatus.SHIPPED
        elif 'empty' in status_str:
            status = ContainerStatus.EMPTY
        
        # Calculate fill percentage
        quantity = int(data.get('quantity', 0))
        max_quantity = int(data.get('maxQuantity', 1))
        fill_percentage = (quantity / max_quantity * 100) if max_quantity > 0 else 0
        
        return Container(
            id=str(cont_id),
            serial_number=data.get('serialNumber', str(cont_id)),
            status=status,
            location_id=data.get('locationId'),
            location_name=data.get('locationName'),
            part_id=data.get('partId'),
            part_name=data.get('partName'),
            quantity=quantity,
            max_quantity=max_quantity,
            fill_percentage=fill_percentage,
            last_updated=datetime.now(timezone.utc),
            lot_number=data.get('lotNumber'),
            metadata=data
        )
    
    def _parse_movement(self, data: Dict[str, Any]) -> Optional[InventoryMovement]:
        """Parse movement from API response"""
        mov_id = data.get('id') or data.get('movementId')
        if not mov_id:
            return None
        
        # Parse timestamp
        timestamp = datetime.now(timezone.utc)
        if data.get('timestamp'):
            try:
                timestamp = datetime.fromisoformat(data['timestamp'].replace('Z', '+00:00'))
            except:
                pass
        
        return InventoryMovement(
            id=str(mov_id),
            movement_type=data.get('type', 'unknown'),
            container_id=data.get('containerId'),
            part_id=data.get('partId'),
            quantity=int(data.get('quantity', 0)),
            from_location=data.get('fromLocation'),
            to_location=data.get('toLocation'),
            timestamp=timestamp,
            operator=data.get('operator'),
            reference_number=data.get('referenceNumber'),
            metadata=data
        )
    
    def _create_location_asset(self, location: Location) -> Asset:
        """Create location asset"""
        external_id = self.create_asset_external_id('location', location.id)
        
        metadata = {
            **self.naming.get_metadata_tags(),
            'location_id': location.id,
            'location_type': location.type,
            'capacity': str(location.capacity),
            'occupancy': str(location.current_occupancy)
        }
        
        # Add optional metadata
        if location.building:
            metadata['building'] = location.building
        if location.zone:
            metadata['zone'] = location.zone
        if location.aisle:
            metadata['aisle'] = location.aisle
        if location.bin:
            metadata['bin'] = location.bin
        
        # Build hierarchical name
        name_parts = [location.name]
        if location.building:
            name_parts.insert(0, location.building)
        
        return Asset(
            external_id=external_id,
            name=" - ".join(name_parts),
            parent_external_id=self.create_asset_external_id('locations_root', self.config.facility.pcn),
            description=f"Storage location {location.name}",
            metadata=metadata,
            data_set_id=self.get_dataset_id('inventory')
        )
    
    def _create_container_asset(self, container: Container) -> Asset:
        """Create container asset"""
        external_id = self.create_asset_external_id('container', container.id)
        
        metadata = {
            **self.naming.get_metadata_tags(),
            'container_id': container.id,
            'serial_number': container.serial_number,
            'status': container.status.value,
            'quantity': str(container.quantity),
            'max_quantity': str(container.max_quantity),
            'fill_percentage': f"{container.fill_percentage:.1f}"
        }
        
        # Add optional metadata
        if container.part_id:
            metadata['part_id'] = container.part_id
        if container.part_name:
            metadata['part_name'] = container.part_name
        if container.location_id:
            metadata['location_id'] = container.location_id
        if container.lot_number:
            metadata['lot_number'] = container.lot_number
        
        return Asset(
            external_id=external_id,
            name=f"Container {container.serial_number}",
            parent_external_id=self.create_asset_external_id('containers_root', self.config.facility.pcn),
            description=f"Container {container.serial_number} - {container.part_name or 'Empty'}",
            metadata=metadata,
            data_set_id=self.get_dataset_id('inventory')
        )
    
    def _create_container_event(self, container: Container) -> Event:
        """Create container status event"""
        external_id = self.create_event_external_id(
            'container_status',
            f"{container.id}_{int(container.last_updated.timestamp())}"
        )
        
        metadata = {
            **self.naming.get_metadata_tags(),
            'container_id': container.id,
            'status': container.status.value,
            'fill_percentage': f"{container.fill_percentage:.1f}",
            'source': 'plex_inventory'
        }
        
        # Build description
        desc_parts = [f"Container {container.serial_number}"]
        if container.part_name:
            desc_parts.append(container.part_name)
        desc_parts.append(f"{container.quantity}/{container.max_quantity} ({container.fill_percentage:.0f}% full)")
        desc_parts.append(f"[{container.status.value}]")
        
        # Prepare asset links
        asset_external_ids = [self.create_asset_external_id('container', container.id)]
        if container.location_id:
            asset_external_ids.append(self.create_asset_external_id('location', container.location_id))
        if container.part_id:
            asset_external_ids.append(self.create_asset_external_id('part', container.part_id))
        
        event = Event(
            external_id=external_id,
            type='container_status',
            subtype=container.status.value,
            description=" | ".join(desc_parts),
            start_time=int(container.last_updated.timestamp() * 1000),
            metadata=metadata,
            data_set_id=self.get_dataset_id('inventory')
        )
        
        event.asset_external_ids = asset_external_ids
        
        return event
    
    def _create_movement_event(self, movement: InventoryMovement) -> Event:
        """Create movement event"""
        external_id = self.create_event_external_id('movement', movement.id)
        
        # Skip if already processed
        if external_id in self.processed_movements:
            return None
        
        metadata = {
            **self.naming.get_metadata_tags(),
            'movement_id': movement.id,
            'movement_type': movement.movement_type,
            'quantity': str(movement.quantity),
            'source': 'plex_inventory'
        }
        
        # Add optional metadata
        if movement.operator:
            metadata['operator'] = movement.operator
        if movement.reference_number:
            metadata['reference'] = movement.reference_number
        
        # Build description
        desc_parts = [f"{movement.movement_type.title()}: {movement.quantity} units"]
        if movement.from_location and movement.to_location:
            desc_parts.append(f"From {movement.from_location} to {movement.to_location}")
        
        # Prepare asset links
        asset_external_ids = []
        if movement.container_id:
            asset_external_ids.append(self.create_asset_external_id('container', movement.container_id))
        if movement.part_id:
            asset_external_ids.append(self.create_asset_external_id('part', movement.part_id))
        
        event = Event(
            external_id=external_id,
            type='inventory_movement',
            subtype=movement.movement_type,
            description=" | ".join(desc_parts),
            start_time=int(movement.timestamp.timestamp() * 1000),
            metadata=metadata,
            data_set_id=self.get_dataset_id('inventory')
        )
        
        if asset_external_ids:
            event.asset_external_ids = asset_external_ids
        
        return event
    
    def _create_wip_event(self, container: Container) -> Event:
        """Create WIP event"""
        external_id = self.create_event_external_id(
            'wip',
            f"{container.id}_{int(datetime.now(timezone.utc).timestamp())}"
        )
        
        metadata = {
            **self.naming.get_metadata_tags(),
            'container_id': container.id,
            'quantity': str(container.quantity),
            'source': 'plex_inventory',
            'wip': 'true'
        }
        
        if container.part_id:
            metadata['part_id'] = container.part_id
        if container.location_id:
            metadata['location_id'] = container.location_id
        
        event = Event(
            external_id=external_id,
            type='wip_inventory',
            subtype='in_progress',
            description=f"WIP: {container.part_name or 'Unknown'} - {container.quantity} units",
            start_time=int(datetime.now(timezone.utc).timestamp() * 1000),
            metadata=metadata,
            data_set_id=self.get_dataset_id('inventory')
        )
        
        # Link to container asset
        event.asset_external_ids = [self.create_asset_external_id('container', container.id)]
        
        return event
    
    async def _create_container_timeseries(self, containers: List[Container]) -> None:
        """Create time series for container fill levels"""
        try:
            timeseries_list = []
            datapoints_to_insert = {}
            
            for container in containers:
                # Create time series for fill level
                ts_external_id = self.create_asset_external_id('container_fill', container.id)
                
                # Get container asset ID for linking
                container_asset_external_id = self.create_asset_external_id('container', container.id)
                container_asset_id = self.id_resolver.resolve_single(container_asset_external_id)
                
                ts = TimeSeries(
                    external_id=ts_external_id,
                    name=f"Fill Level - Container {container.serial_number}",
                    unit='%',
                    asset_id=container_asset_id,
                    description=f"Fill level percentage for container {container.serial_number}",
                    metadata={
                        **self.naming.get_metadata_tags(),
                        'container_id': container.id,
                        'source': 'plex_inventory'
                    },
                    data_set_id=self.get_dataset_id('inventory')
                )
                
                timeseries_list.append(ts)
                datapoints_to_insert[ts_external_id] = container.fill_percentage
            
            # Create time series
            if timeseries_list:
                # Check existing
                external_ids = [ts.external_id for ts in timeseries_list]
                existing = await self.dedup_helper.get_existing_timeseries(external_ids)
                
                new_ts = [ts for ts in timeseries_list if ts.external_id not in existing]
                
                if new_ts:
                    await self.async_cdf.create_time_series(new_ts)
                    self.logger.info("container_timeseries_created", count=len(new_ts))
            
            # Insert datapoints
            if datapoints_to_insert:
                dp_list = []
                timestamp = int(datetime.now(timezone.utc).timestamp() * 1000)
                
                for external_id, value in datapoints_to_insert.items():
                    dp = Datapoints(
                        external_id=external_id,
                        datapoints=[(timestamp, value)]
                    )
                    dp_list.append(dp)
                
                # Insert via CDF client
                if dp_list:
                    loop = asyncio.get_event_loop()
                    await loop.run_in_executor(
                        None,
                        self.client.time_series.data.insert_multiple,
                        dp_list
                    )
                    
                    self.logger.info("container_datapoints_inserted", count=len(dp_list))
                    
        except Exception as e:
            self.logger.error("container_timeseries_error", error=str(e))


async def main():
    """Main entry point for standalone execution"""
    import os
    
    # Setup logging for console
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.dev.ConsoleRenderer()
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )
    
    try:
        extractor = EnhancedInventoryExtractor()
        
        # Run once or continuously
        if os.getenv('RUN_CONTINUOUS', 'false').lower() == 'true':
            while True:
                await extractor.run_extraction_cycle()
                await asyncio.sleep(extractor.config.extraction_interval)
        else:
            await extractor.run_extraction_cycle()
            
    except KeyboardInterrupt:
        logger.info("Extraction stopped by user")
    except Exception as e:
        logger.error("Fatal error", error=str(e), exc_info=True)
        raise
    finally:
        await extractor.cleanup()


if __name__ == "__main__":
    import os
    asyncio.run(main())