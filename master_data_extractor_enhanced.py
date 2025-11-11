#!/usr/bin/env python3
"""
Enhanced Master Data Extractor with All Improvements
- Full async/await implementation
- Type hints throughout
- Error handling with retry
- Asset ID resolution
- Structured logging
- Change detection and incremental updates
"""

from __future__ import annotations

import asyncio
import hashlib
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any, Tuple, Final, TypeAlias, Set
from dataclasses import dataclass, field
from enum import StrEnum, auto

import structlog
from cognite.client.data_classes import Asset, Relationship, RelationshipList

from base_extractor_enhanced import (
    BaseExtractor, BaseExtractorConfig, ExtractionResult,
    DatasetType, with_retry
)
from error_handling import PlexAPIError

# Setup structured logging
logger = structlog.get_logger(__name__)

# Type aliases
PartId: TypeAlias = str
OperationId: TypeAlias = str
ResourceId: TypeAlias = str
BOMId: TypeAlias = str


class ChangeDetectionStrategy(StrEnum):
    """Strategy for detecting changes in master data"""
    HASH = auto()  # Compare hash of data
    TIMESTAMP = auto()  # Use last modified timestamp
    VERSION = auto()  # Use version number
    ALWAYS = auto()  # Always update


@dataclass
class Part:
    """Part master data"""
    id: PartId
    number: str
    name: str
    description: Optional[str] = None
    revision: Optional[str] = None
    part_type: Optional[str] = None
    unit_of_measure: Optional[str] = None
    weight: Optional[float] = None
    cost: Optional[float] = None
    lead_time_days: Optional[int] = None
    min_order_qty: Optional[int] = None
    safety_stock: Optional[int] = None
    active: bool = True
    last_modified: Optional[datetime] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def calculate_hash(self) -> str:
        """Calculate hash for change detection"""
        data_str = f"{self.number}|{self.name}|{self.revision}|{self.active}"
        return hashlib.md5(data_str.encode()).hexdigest()


@dataclass
class Operation:
    """Operation master data"""
    id: OperationId
    code: str
    name: str
    description: Optional[str] = None
    workcenter_id: Optional[str] = None
    setup_time_minutes: Optional[float] = None
    cycle_time_minutes: Optional[float] = None
    operators_required: int = 1
    active: bool = True
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class BillOfMaterials:
    """Bill of Materials (BOM) data"""
    id: BOMId
    parent_part_id: PartId
    child_part_id: PartId
    quantity: float
    unit_of_measure: str
    operation_id: Optional[OperationId] = None
    sequence: int = 0
    active: bool = True
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Routing:
    """Routing data"""
    id: str
    part_id: PartId
    operation_id: OperationId
    sequence: int
    workcenter_id: Optional[str] = None
    setup_time: Optional[float] = None
    cycle_time: Optional[float] = None
    move_time: Optional[float] = None
    queue_time: Optional[float] = None
    active: bool = True
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Resource:
    """Resource/Equipment master data"""
    id: ResourceId
    code: str
    name: str
    resource_type: str  # machine, tool, fixture, etc.
    workcenter_id: Optional[str] = None
    capacity: Optional[float] = None
    efficiency: Optional[float] = None
    cost_per_hour: Optional[float] = None
    active: bool = True
    metadata: Dict[str, Any] = field(default_factory=dict)


class MasterDataExtractorConfig(BaseExtractorConfig):
    """Configuration specific to master data extractor"""
    extract_parts: bool = True
    extract_operations: bool = True
    extract_boms: bool = True
    extract_routings: bool = True
    extract_resources: bool = True
    change_detection_strategy: ChangeDetectionStrategy = ChangeDetectionStrategy.HASH
    full_refresh_interval_hours: int = 24
    incremental_update: bool = True
    
    @classmethod
    def from_env(cls) -> MasterDataExtractorConfig:
        """Load configuration from environment"""
        import os
        base = BaseExtractorConfig.from_env('master')
        
        strategy_str = os.getenv('MASTER_CHANGE_DETECTION', 'HASH').upper()
        strategy = ChangeDetectionStrategy[strategy_str] if strategy_str in ChangeDetectionStrategy.__members__ else ChangeDetectionStrategy.HASH
        
        return cls(
            **base.dict(),
            extract_parts=os.getenv('EXTRACT_PARTS', 'true').lower() == 'true',
            extract_operations=os.getenv('EXTRACT_OPERATIONS', 'true').lower() == 'true',
            extract_boms=os.getenv('EXTRACT_BOMS', 'true').lower() == 'true',
            extract_routings=os.getenv('EXTRACT_ROUTINGS', 'true').lower() == 'true',
            extract_resources=os.getenv('EXTRACT_RESOURCES', 'true').lower() == 'true',
            change_detection_strategy=strategy,
            full_refresh_interval_hours=int(os.getenv('MASTER_FULL_REFRESH_HOURS', '24')),
            incremental_update=os.getenv('MASTER_INCREMENTAL_UPDATE', 'true').lower() == 'true'
        )


class EnhancedMasterDataExtractor(BaseExtractor):
    """Enhanced master data extractor with all improvements"""
    
    def __init__(self, config: Optional[MasterDataExtractorConfig] = None):
        """Initialize with enhanced configuration"""
        config = config or MasterDataExtractorConfig.from_env()
        super().__init__(config, 'master_data')
        
        self.config: Final[MasterDataExtractorConfig] = config
        
        # Caches for change detection
        self.part_hashes: Dict[PartId, str] = {}
        self.operation_hashes: Dict[OperationId, str] = {}
        self.processed_parts: Set[PartId] = set()
        self.processed_operations: Set[OperationId] = set()
        
        # Track last full refresh
        last_refresh_time = self.state_tracker.get_last_extraction_time('master_full_refresh')
        self.last_full_refresh = last_refresh_time or (
            datetime.now(timezone.utc) - timedelta(hours=config.full_refresh_interval_hours + 1)
        )
        
        self.logger.info(
            "master_data_extractor_initialized",
            extract_parts=config.extract_parts,
            extract_operations=config.extract_operations,
            extract_boms=config.extract_boms,
            extract_routings=config.extract_routings,
            extract_resources=config.extract_resources,
            change_detection=config.change_detection_strategy.value,
            incremental=config.incremental_update
        )
    
    def get_required_datasets(self) -> List[str]:
        """Master data requires master dataset"""
        return ['master']
    
    async def extract(self) -> ExtractionResult:
        """Main extraction with concurrent operations"""
        start_time = datetime.now(timezone.utc)
        result = ExtractionResult(
            success=True,
            items_processed=0,
            duration_ms=0
        )
        
        try:
            # Check if full refresh needed
            hours_since_refresh = (datetime.now(timezone.utc) - self.last_full_refresh).total_seconds() / 3600
            is_full_refresh = hours_since_refresh >= self.config.full_refresh_interval_hours
            
            if is_full_refresh:
                self.logger.info("performing_full_refresh")
                self.part_hashes.clear()
                self.operation_hashes.clear()
                self.processed_parts.clear()
                self.processed_operations.clear()
            
            # Ensure master data hierarchy exists
            await self._ensure_master_hierarchy()
            
            tasks = []
            
            # Create extraction tasks based on configuration
            async with asyncio.TaskGroup() as tg:
                if self.config.extract_parts:
                    tasks.append(tg.create_task(self._extract_parts(is_full_refresh)))
                
                if self.config.extract_operations:
                    tasks.append(tg.create_task(self._extract_operations(is_full_refresh)))
                
                if self.config.extract_resources:
                    tasks.append(tg.create_task(self._extract_resources()))
                
                # BOM and Routing depend on Parts and Operations
                if self.config.extract_boms and self.config.extract_parts:
                    tasks.append(tg.create_task(self._extract_boms()))
                
                if self.config.extract_routings and self.config.extract_parts and self.config.extract_operations:
                    tasks.append(tg.create_task(self._extract_routings()))
            
            # Aggregate results
            for task in tasks:
                task_result = await task
                result.items_processed += task_result.items_processed
                if not task_result.success:
                    result.success = False
                    result.errors.extend(task_result.errors)
            
            # Update last full refresh time if successful
            if is_full_refresh and result.success:
                self.last_full_refresh = datetime.now(timezone.utc)
                self.state_tracker.set_last_extraction_time('master_full_refresh', self.last_full_refresh)
            
            self.logger.info(
                "master_data_extraction_completed",
                items_processed=result.items_processed,
                success=result.success,
                was_full_refresh=is_full_refresh
            )
            
        except Exception as e:
            result.success = False
            result.errors.append(str(e))
            self.logger.error("master_data_extraction_failed", error=str(e), exc_info=True)
        
        result.duration_ms = (datetime.now(timezone.utc) - start_time).total_seconds() * 1000
        return result
    
    async def _ensure_master_hierarchy(self) -> None:
        """Ensure master data asset hierarchy exists"""
        try:
            root_assets = [
                Asset(
                    external_id=self.create_asset_external_id('master_root', self.config.facility.pcn),
                    name=f"{self.config.facility.facility_name} - Master Data",
                    parent_external_id=self.create_asset_external_id('facility', self.config.facility.pcn),
                    description="Root asset for master data",
                    metadata={
                        **self.naming.get_metadata_tags(),
                        'asset_type': 'master_root'
                    },
                    data_set_id=self.get_dataset_id('master')
                ),
                Asset(
                    external_id=self.create_asset_external_id('parts_library', self.config.facility.pcn),
                    name=f"{self.config.facility.facility_name} - Parts Library",
                    parent_external_id=self.create_asset_external_id('master_root', self.config.facility.pcn),
                    description="Parts master data library",
                    metadata={
                        **self.naming.get_metadata_tags(),
                        'asset_type': 'parts_library'
                    },
                    data_set_id=self.get_dataset_id('master')
                ),
                Asset(
                    external_id=self.create_asset_external_id('operations_library', self.config.facility.pcn),
                    name=f"{self.config.facility.facility_name} - Operations Library",
                    parent_external_id=self.create_asset_external_id('master_root', self.config.facility.pcn),
                    description="Operations master data library",
                    metadata={
                        **self.naming.get_metadata_tags(),
                        'asset_type': 'operations_library'
                    },
                    data_set_id=self.get_dataset_id('master')
                ),
                Asset(
                    external_id=self.create_asset_external_id('resources_library', self.config.facility.pcn),
                    name=f"{self.config.facility.facility_name} - Resources Library",
                    parent_external_id=self.create_asset_external_id('master_root', self.config.facility.pcn),
                    description="Resources and equipment library",
                    metadata={
                        **self.naming.get_metadata_tags(),
                        'asset_type': 'resources_library'
                    },
                    data_set_id=self.get_dataset_id('master')
                )
            ]
            
            await self.create_assets_with_retry(root_assets, resolve_parents=True)
            
        except Exception as e:
            self.logger.error("master_hierarchy_creation_error", error=str(e))
    
    @with_retry(max_attempts=3)
    async def _extract_parts(self, full_refresh: bool) -> ExtractionResult:
        """Extract parts master data with change detection"""
        result = ExtractionResult(success=True, items_processed=0, duration_ms=0)
        
        try:
            # Fetch parts
            parts = await self._fetch_parts(full_refresh)
            
            if not parts:
                self.logger.info("no_parts_found")
                return result
            
            # Filter changed parts if incremental
            if self.config.incremental_update and not full_refresh:
                changed_parts = []
                for part in parts:
                    if self._has_part_changed(part):
                        changed_parts.append(part)
                parts = changed_parts
                
                self.logger.info(
                    "parts_change_detection",
                    total_parts=len(parts),
                    changed_parts=len(changed_parts)
                )
            
            # Create/update part assets
            assets = []
            for part in parts:
                asset = self._create_part_asset(part)
                if asset:
                    assets.append(asset)
                    # Update hash cache
                    self.part_hashes[part.id] = part.calculate_hash()
                    self.processed_parts.add(part.id)
            
            # Create in CDF
            if assets:
                created, failed = await self.create_assets_with_retry(
                    assets,
                    resolve_parents=True
                )
                result.items_processed = len(created)
            
            self.logger.info(
                "parts_extracted",
                parts_found=len(parts),
                assets_created=result.items_processed
            )
            
        except Exception as e:
            result.success = False
            result.errors.append(f"Parts extraction failed: {e}")
            self.logger.error("parts_extraction_error", error=str(e))
        
        return result
    
    @with_retry(max_attempts=3)
    async def _extract_operations(self, full_refresh: bool) -> ExtractionResult:
        """Extract operations master data"""
        result = ExtractionResult(success=True, items_processed=0, duration_ms=0)
        
        try:
            # Fetch operations
            operations = await self._fetch_operations()
            
            if not operations:
                self.logger.info("no_operations_found")
                return result
            
            # Create operation assets
            assets = []
            for operation in operations:
                asset = self._create_operation_asset(operation)
                if asset:
                    assets.append(asset)
                    self.processed_operations.add(operation.id)
            
            # Create in CDF
            if assets:
                created, failed = await self.create_assets_with_retry(
                    assets,
                    resolve_parents=True
                )
                result.items_processed = len(created)
            
            self.logger.info(
                "operations_extracted",
                operations_found=len(operations),
                assets_created=result.items_processed
            )
            
        except Exception as e:
            result.success = False
            result.errors.append(f"Operations extraction failed: {e}")
            self.logger.error("operations_extraction_error", error=str(e))
        
        return result
    
    @with_retry(max_attempts=3)
    async def _extract_boms(self) -> ExtractionResult:
        """Extract BOMs and create relationships"""
        result = ExtractionResult(success=True, items_processed=0, duration_ms=0)
        
        try:
            # Fetch BOMs
            boms = await self._fetch_boms()
            
            if not boms:
                self.logger.info("no_boms_found")
                return result
            
            # Create BOM relationships
            relationships = []
            for bom in boms:
                relationship = self._create_bom_relationship(bom)
                if relationship:
                    relationships.append(relationship)
            
            # Create relationships in CDF
            if relationships:
                created = await self._create_relationships_batch(relationships)
                result.items_processed = len(created)
            
            self.logger.info(
                "boms_extracted",
                boms_found=len(boms),
                relationships_created=result.items_processed
            )
            
        except Exception as e:
            result.success = False
            result.errors.append(f"BOM extraction failed: {e}")
            self.logger.error("bom_extraction_error", error=str(e))
        
        return result
    
    @with_retry(max_attempts=3)
    async def _extract_routings(self) -> ExtractionResult:
        """Extract routings and create relationships"""
        result = ExtractionResult(success=True, items_processed=0, duration_ms=0)
        
        try:
            # Fetch routings
            routings = await self._fetch_routings()
            
            if not routings:
                self.logger.info("no_routings_found")
                return result
            
            # Create routing relationships
            relationships = []
            for routing in routings:
                relationship = self._create_routing_relationship(routing)
                if relationship:
                    relationships.append(relationship)
            
            # Create relationships in CDF
            if relationships:
                created = await self._create_relationships_batch(relationships)
                result.items_processed = len(created)
            
            self.logger.info(
                "routings_extracted",
                routings_found=len(routings),
                relationships_created=result.items_processed
            )
            
        except Exception as e:
            result.success = False
            result.errors.append(f"Routing extraction failed: {e}")
            self.logger.error("routing_extraction_error", error=str(e))
        
        return result
    
    @with_retry(max_attempts=3)
    async def _extract_resources(self) -> ExtractionResult:
        """Extract resources/equipment master data"""
        result = ExtractionResult(success=True, items_processed=0, duration_ms=0)
        
        try:
            # Fetch resources
            resources = await self._fetch_resources()
            
            if not resources:
                self.logger.info("no_resources_found")
                return result
            
            # Create resource assets
            assets = []
            for resource in resources:
                asset = self._create_resource_asset(resource)
                if asset:
                    assets.append(asset)
            
            # Create in CDF
            if assets:
                created, failed = await self.create_assets_with_retry(
                    assets,
                    resolve_parents=True
                )
                result.items_processed = len(created)
            
            self.logger.info(
                "resources_extracted",
                resources_found=len(resources),
                assets_created=result.items_processed
            )
            
        except Exception as e:
            result.success = False
            result.errors.append(f"Resources extraction failed: {e}")
            self.logger.error("resources_extraction_error", error=str(e))
        
        return result
    
    async def _fetch_parts(self, full_refresh: bool) -> List[Part]:
        """Fetch parts from Plex API"""
        endpoint = "/mdm/v1/parts"
        
        all_parts = []
        offset = 0
        
        # If incremental, fetch only recently modified
        params = {'limit': 1000}
        if not full_refresh and self.config.incremental_update:
            last_sync = self.state_tracker.get_last_extraction_time('parts')
            if last_sync:
                params['modifiedAfter'] = last_sync.isoformat()
        
        while True:
            params['offset'] = offset
            
            try:
                data = await self.fetch_plex_data(endpoint, params)
                
                if not data:
                    break
                
                parts_raw = data if isinstance(data, list) else data.get('data', [])
                
                for part_data in parts_raw:
                    part = self._parse_part(part_data)
                    if part:
                        all_parts.append(part)
                
                if len(parts_raw) < 1000:
                    break
                
                offset += len(parts_raw)
                
            except Exception as e:
                self.logger.error("fetch_parts_error", error=str(e))
                break
        
        return all_parts
    
    async def _fetch_operations(self) -> List[Operation]:
        """Fetch operations from Plex API"""
        endpoint = "/mdm/v1/operations"
        
        try:
            data = await self.fetch_plex_data(endpoint)
            
            operations = []
            for op_data in data if isinstance(data, list) else data.get('data', []):
                operation = self._parse_operation(op_data)
                if operation:
                    operations.append(operation)
            
            return operations
            
        except Exception as e:
            self.logger.error("fetch_operations_error", error=str(e))
            return []
    
    async def _fetch_boms(self) -> List[BillOfMaterials]:
        """Fetch BOMs from Plex API"""
        # BOMs need to be fetched per part
        # First get parts, then fetch BOMs for each
        try:
            parts = await self._fetch_parts(full_refresh=False)
            if not parts:
                self.logger.info("no_parts_for_boms")
                return []
            
            all_boms = []
            for part in parts[:10]:  # Limit to first 10 parts to avoid too many requests
                try:
                    endpoint = "/engineering/v1/boms"
                    params = {'partId': part.id}
                    data = await self.fetch_plex_data(endpoint, params)
                    
                    if data:
                        if isinstance(data, list):
                            for bom_data in data:
                                bom = self._parse_bom(bom_data)
                                if bom:
                                    all_boms.append(bom)
                        elif isinstance(data, dict) and 'data' in data:
                            for bom_data in data['data']:
                                bom = self._parse_bom(bom_data)
                                if bom:
                                    all_boms.append(bom)
                except Exception as e:
                    self.logger.warning("fetch_bom_for_part_error", part_id=part.id, error=str(e))
                    continue
            
            return all_boms
            
        except Exception as e:
            self.logger.error("fetch_boms_error", error=str(e))
            return []
    
    async def _fetch_routings(self) -> List[Routing]:
        """Fetch routings from Plex API"""
        # Note: Routings may not have a direct API endpoint
        # Return empty for now - would need Data Source API
        self.logger.warning("routings_not_available", reason="No REST API endpoint for routings")
        return []
        
        try:
            data = await self.fetch_plex_data(endpoint)
            
            routings = []
            for routing_data in data if isinstance(data, list) else data.get('data', []):
                routing = self._parse_routing(routing_data)
                if routing:
                    routings.append(routing)
            
            return routings
            
        except Exception as e:
            self.logger.error("fetch_routings_error", error=str(e))
            return []
    
    async def _fetch_resources(self) -> List[Resource]:
        """Fetch resources from Plex API"""
        # Resources are workcenters in production definitions
        endpoint = "/production/v1/production-definitions/workcenters"
        
        try:
            data = await self.fetch_plex_data(endpoint)
            
            resources = []
            for res_data in data if isinstance(data, list) else data.get('data', []):
                resource = self._parse_resource(res_data)
                if resource:
                    resources.append(resource)
            
            return resources
            
        except Exception as e:
            self.logger.error("fetch_resources_error", error=str(e))
            return []
    
    def _parse_part(self, data: Dict[str, Any]) -> Optional[Part]:
        """Parse part from API response"""
        part_id = data.get('id') or data.get('partId')
        if not part_id:
            return None
        
        return Part(
            id=str(part_id),
            number=data.get('partNumber', ''),
            name=data.get('partName', ''),
            description=data.get('description'),
            revision=data.get('revision'),
            part_type=data.get('partType'),
            unit_of_measure=data.get('unitOfMeasure'),
            weight=data.get('weight'),
            cost=data.get('cost'),
            lead_time_days=data.get('leadTimeDays'),
            min_order_qty=data.get('minOrderQty'),
            safety_stock=data.get('safetyStock'),
            active=data.get('active', True),
            metadata=data
        )
    
    def _parse_operation(self, data: Dict[str, Any]) -> Optional[Operation]:
        """Parse operation from API response"""
        op_id = data.get('id') or data.get('operationId')
        if not op_id:
            return None
        
        return Operation(
            id=str(op_id),
            code=data.get('operationCode', ''),
            name=data.get('operationName', ''),
            description=data.get('description'),
            workcenter_id=data.get('workcenterId'),
            setup_time_minutes=data.get('setupTime'),
            cycle_time_minutes=data.get('cycleTime'),
            operators_required=data.get('operatorsRequired', 1),
            active=data.get('active', True),
            metadata=data
        )
    
    def _parse_bom(self, data: Dict[str, Any]) -> Optional[BillOfMaterials]:
        """Parse BOM from API response"""
        bom_id = data.get('id') or data.get('bomId')
        if not bom_id:
            return None
        
        return BillOfMaterials(
            id=str(bom_id),
            parent_part_id=str(data.get('parentPartId', '')),
            child_part_id=str(data.get('childPartId', '')),
            quantity=float(data.get('quantity', 1)),
            unit_of_measure=data.get('unitOfMeasure', 'EA'),
            operation_id=data.get('operationId'),
            sequence=data.get('sequence', 0),
            active=data.get('active', True),
            metadata=data
        )
    
    def _parse_routing(self, data: Dict[str, Any]) -> Optional[Routing]:
        """Parse routing from API response"""
        routing_id = data.get('id') or data.get('routingId')
        if not routing_id:
            return None
        
        return Routing(
            id=str(routing_id),
            part_id=str(data.get('partId', '')),
            operation_id=str(data.get('operationId', '')),
            sequence=data.get('sequence', 0),
            workcenter_id=data.get('workcenterId'),
            setup_time=data.get('setupTime'),
            cycle_time=data.get('cycleTime'),
            move_time=data.get('moveTime'),
            queue_time=data.get('queueTime'),
            active=data.get('active', True),
            metadata=data
        )
    
    def _parse_resource(self, data: Dict[str, Any]) -> Optional[Resource]:
        """Parse resource from API response"""
        res_id = data.get('id') or data.get('resourceId')
        if not res_id:
            return None
        
        return Resource(
            id=str(res_id),
            code=data.get('resourceCode', ''),
            name=data.get('resourceName', ''),
            resource_type=data.get('resourceType', 'machine'),
            workcenter_id=data.get('workcenterId'),
            capacity=data.get('capacity'),
            efficiency=data.get('efficiency'),
            cost_per_hour=data.get('costPerHour'),
            active=data.get('active', True),
            metadata=data
        )
    
    def _has_part_changed(self, part: Part) -> bool:
        """Check if part has changed using configured strategy"""
        if self.config.change_detection_strategy == ChangeDetectionStrategy.ALWAYS:
            return True
        
        if self.config.change_detection_strategy == ChangeDetectionStrategy.HASH:
            current_hash = part.calculate_hash()
            previous_hash = self.part_hashes.get(part.id)
            return current_hash != previous_hash
        
        # For TIMESTAMP and VERSION strategies, would need additional logic
        return True
    
    def _create_part_asset(self, part: Part) -> Asset:
        """Create part asset"""
        external_id = self.create_asset_external_id('part', part.id)
        
        metadata = {
            **self.naming.get_metadata_tags(),
            'part_id': part.id,
            'part_number': part.number,
            'revision': part.revision or '',
            'part_type': part.part_type or '',
            'unit_of_measure': part.unit_of_measure or '',
            'active': str(part.active)
        }
        
        # Add optional metadata
        if part.weight is not None:
            metadata['weight'] = str(part.weight)
        if part.cost is not None:
            metadata['cost'] = str(part.cost)
        if part.lead_time_days is not None:
            metadata['lead_time_days'] = str(part.lead_time_days)
        if part.safety_stock is not None:
            metadata['safety_stock'] = str(part.safety_stock)
        
        return Asset(
            external_id=external_id,
            name=f"{part.number} - {part.name}",
            parent_external_id=self.create_asset_external_id('parts_library', self.config.facility.pcn),
            description=part.description or f"Part {part.number}",
            metadata=metadata,
            data_set_id=self.get_dataset_id('master')
        )
    
    def _create_operation_asset(self, operation: Operation) -> Asset:
        """Create operation asset"""
        external_id = self.create_asset_external_id('operation', operation.id)
        
        metadata = {
            **self.naming.get_metadata_tags(),
            'operation_id': operation.id,
            'operation_code': operation.code,
            'operators_required': str(operation.operators_required),
            'active': str(operation.active)
        }
        
        # Add optional metadata
        if operation.workcenter_id:
            metadata['workcenter_id'] = operation.workcenter_id
        if operation.setup_time_minutes is not None:
            metadata['setup_time_minutes'] = str(operation.setup_time_minutes)
        if operation.cycle_time_minutes is not None:
            metadata['cycle_time_minutes'] = str(operation.cycle_time_minutes)
        
        return Asset(
            external_id=external_id,
            name=f"{operation.code} - {operation.name}",
            parent_external_id=self.create_asset_external_id('operations_library', self.config.facility.pcn),
            description=operation.description or f"Operation {operation.code}",
            metadata=metadata,
            data_set_id=self.get_dataset_id('master')
        )
    
    def _create_resource_asset(self, resource: Resource) -> Asset:
        """Create resource asset"""
        external_id = self.create_asset_external_id('resource', resource.id)
        
        metadata = {
            **self.naming.get_metadata_tags(),
            'resource_id': resource.id,
            'resource_code': resource.code,
            'resource_type': resource.resource_type,
            'active': str(resource.active)
        }
        
        # Add optional metadata
        if resource.workcenter_id:
            metadata['workcenter_id'] = resource.workcenter_id
        if resource.capacity is not None:
            metadata['capacity'] = str(resource.capacity)
        if resource.efficiency is not None:
            metadata['efficiency'] = str(resource.efficiency)
        if resource.cost_per_hour is not None:
            metadata['cost_per_hour'] = str(resource.cost_per_hour)
        
        return Asset(
            external_id=external_id,
            name=f"{resource.code} - {resource.name}",
            parent_external_id=self.create_asset_external_id('resources_library', self.config.facility.pcn),
            description=f"{resource.resource_type} - {resource.name}",
            metadata=metadata,
            data_set_id=self.get_dataset_id('master')
        )
    
    def _create_bom_relationship(self, bom: BillOfMaterials) -> Optional[Relationship]:
        """Create BOM relationship between parts"""
        if not bom.parent_part_id or not bom.child_part_id:
            return None
        
        source_external_id = self.create_asset_external_id('part', bom.parent_part_id)
        target_external_id = self.create_asset_external_id('part', bom.child_part_id)
        
        return Relationship(
            external_id=self.create_asset_external_id('bom', bom.id),
            source_external_id=source_external_id,
            source_type='asset',
            target_external_id=target_external_id,
            target_type='asset',
            relationship_type='BOM',
            confidence=1.0,
            data_set_id=self.get_dataset_id('master'),
            labels=['BOM', 'parent-child'],
            metadata={
                'quantity': str(bom.quantity),
                'unit_of_measure': bom.unit_of_measure,
                'sequence': str(bom.sequence)
            }
        )
    
    def _create_routing_relationship(self, routing: Routing) -> Optional[Relationship]:
        """Create routing relationship between part and operation"""
        if not routing.part_id or not routing.operation_id:
            return None
        
        source_external_id = self.create_asset_external_id('part', routing.part_id)
        target_external_id = self.create_asset_external_id('operation', routing.operation_id)
        
        return Relationship(
            external_id=self.create_asset_external_id('routing', routing.id),
            source_external_id=source_external_id,
            source_type='asset',
            target_external_id=target_external_id,
            target_type='asset',
            relationship_type='ROUTING',
            confidence=1.0,
            data_set_id=self.get_dataset_id('master'),
            labels=['routing', 'part-operation'],
            metadata={
                'sequence': str(routing.sequence),
                'setup_time': str(routing.setup_time) if routing.setup_time else '',
                'cycle_time': str(routing.cycle_time) if routing.cycle_time else ''
            }
        )
    
    async def _create_relationships_batch(self, relationships: List[Relationship]) -> List[Relationship]:
        """Create relationships in CDF"""
        try:
            # Create via CDF client
            loop = asyncio.get_event_loop()
            created = await loop.run_in_executor(
                None,
                self.client.relationships.create,
                relationships
            )
            
            if isinstance(created, RelationshipList):
                return list(created)
            elif isinstance(created, Relationship):
                return [created]
            return []
            
        except Exception as e:
            self.logger.error("relationships_creation_error", error=str(e))
            return []


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
        extractor = EnhancedMasterDataExtractor()
        
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