#!/usr/bin/env python3
"""
Master Data Extractor for Plex MES

Extracts and maintains master data including:
- Workcenters and equipment
- Buildings and facilities  
- Parts and items
- BOMs (Bill of Materials)
- Routings and process definitions

ALL IDs include PCN prefix for multi-facility support.
NO RAW TABLES - Uses proper CDF Assets in PLEXMASTER dataset.
"""

import os
import sys
import asyncio
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any

from dotenv import load_dotenv
from cognite.client.data_classes import Asset, Sequence

from base_extractor import BaseExtractor, BaseExtractorConfig
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


class MasterDataConfig(BaseExtractorConfig):
    """Configuration for Master Data Extractor - inherits from base"""
    
    @classmethod
    def from_env(cls) -> 'MasterDataConfig':
        """Load configuration from environment variables"""
        # Use base class method to get common config
        base_config = BaseExtractorConfig.from_env('master_data')
        
        # Master data extractor uses the base config as-is
        return cls(**base_config.__dict__)


class MasterDataExtractor(BaseExtractor):
    """Extracts and maintains master data in CDF - NO RAW TABLES"""
    
    def __init__(self, config: MasterDataConfig):
        super().__init__(config, 'master_data')
        
        # Track extracted items for this session
        self.extracted_workcenters = set()
        self.extracted_parts = set()
        self.extracted_boms = set()
        
        logger.info(f"Master Data Extractor initialized for PCN {config.facility.pcn}")
    
    def get_required_datasets(self) -> List[str]:
        """Return required dataset types for master data"""
        return ['master']  # Only needs PLEXMASTER dataset
    
    async def ensure_facility_root(self) -> str:
        """Ensure facility root asset exists"""
        facility_root_id = self.naming.asset_id("FACILITY", "ROOT")
        
        root_asset = Asset(
            external_id=facility_root_id,
            name=f"{self.config.facility.facility_name} - Facility Root",
            description=f"Root facility asset for {self.config.facility.facility_name}",
            data_set_id=self.config.dataset_master_id,
            metadata={**self.naming.get_metadata_tags(), "type": "facility_root"}
        )
        
        result = self.dedup_helper.upsert_assets([root_asset])
        if result['created']:
            logger.info(f"Created facility root: {facility_root_id}")
        
        return facility_root_id
    
    async def extract_workcenters(self) -> List[Asset]:
        """Extract workcenter master data and create assets"""
        logger.info(f"Extracting workcenters for PCN {self.config.facility.pcn}")
        
        # Fetch all workcenters from production definitions API
        data = await self.fetch_plex_data("/production/v1/production-definitions/workcenters")
        if not data:
            logger.warning("No workcenter data received")
            return []
        
        # Handle different response formats
        if isinstance(data, list):
            workcenters = data
        elif isinstance(data, dict):
            workcenters = data.get('data', data.get('workcenters', []))
        else:
            workcenters = []
        
        # Ensure facility root exists
        facility_root_id = self.naming.asset_id("FACILITY", "ROOT")
        try:
            self.cognite_client.assets.retrieve(external_id=facility_root_id)
        except:
            root_asset = Asset(
                external_id=facility_root_id,
                name=f"{self.config.facility.facility_name} - Facility Root",
                description=f"Root facility asset for {self.config.facility.facility_name}",
                data_set_id=self.config.dataset_master_id,
                metadata={**self.naming.get_metadata_tags(), "type": "facility_root"}
            )
            # Use deduplication helper
            result = self.dedup_helper.upsert_assets([root_asset])
            if result['created']:
                logger.info(f"Created facility root: {facility_root_id}")
            else:
                logger.debug(f"Facility root already exists: {facility_root_id}")
        
        assets = []
        raw_rows = []
        
        for wc in workcenters:
            wc_id = wc.get('id') or wc.get('workcenterId')
            if not wc_id:
                continue
                
            # Create asset with PCN prefix
            external_id = self.naming.asset_id("WC", wc_id)
            
            if external_id not in self.extracted_workcenters:
                # Determine parent (building or facility root)
                building_id = wc.get('buildingId')
                if building_id:
                    parent_id = self.naming.asset_id("BUILDING", building_id)
                else:
                    parent_id = facility_root_id
                
                asset = Asset(
                    external_id=external_id,
                    name=wc.get('name', f'Workcenter {wc_id}'),
                    parent_external_id=parent_id,
                    description=wc.get('description', ''),
                    data_set_id=self.config.dataset_master_id,
                    metadata={
                        **self.naming.get_metadata_tags(),
                        'workcenter_id': str(wc_id),
                        'workcenter_type': wc.get('type', 'standard'),
                        'building_id': str(building_id) if building_id else '',
                        'department': wc.get('department', ''),
                        'cost_center': wc.get('costCenter', ''),
                        'capacity': str(wc.get('capacity', 0)),
                        'status': wc.get('status', 'active'),
                        'last_updated': datetime.now(timezone.utc).isoformat()
                    }
                )
                assets.append(asset)
                self.extracted_workcenters.add(external_id)
        
        # Upload assets using deduplication
        if assets:
            result = self.dedup_helper.upsert_assets(assets)
            logger.info(f"Workcenters: {len(result['created'])} created, "
                       f"{len(result['updated'])} updated, {len(result['skipped'])} unchanged")
        
        return assets
    
    async def extract_parts(self) -> List[Asset]:
        """Extract parts/items master data as Assets in PLEXMASTER dataset"""
        logger.info(f"Extracting parts for PCN {self.config.facility.pcn}")
        
        # Use MDM parts endpoint to list all parts
        data = await self.fetch_plex_data("/mdm/v1/parts")
        # Handle both list and dict response formats
        if isinstance(data, list):
            parts = data
        elif isinstance(data, dict):
            parts = data.get('data', [])
        else:
            parts = []
        
        # Create parts library root asset
        parts_root_id = self.naming.asset_id("PARTS", "LIBRARY")
        facility_root_id = self.naming.asset_id("FACILITY", "ROOT")
        
        root_asset = Asset(
            external_id=parts_root_id,
            name=f"Parts Library - {self.config.facility.facility_name}",
            parent_external_id=facility_root_id,
            description=f"Parts master data for {self.config.facility.facility_name}",
            data_set_id=self.config.dataset_master_id,
            metadata={**self.naming.get_metadata_tags(), "type": "parts_library"}
        )
        self.dedup_helper.upsert_assets([root_asset])
        
        assets = []
        
        for part in parts:
            part_id = part.get('id') or part.get('partId') or part.get('partNumber')
            if not part_id:
                continue
            
            # Create part asset with PCN prefix
            external_id = self.naming.asset_id("PART", part_id)
            
            if external_id not in self.extracted_parts:
                asset = Asset(
                    external_id=external_id,
                    name=part.get('name', part.get('partNumber', f'Part {part_id}')),
                    parent_external_id=parts_root_id,
                    description=part.get('description', ''),
                    data_set_id=self.config.dataset_master_id,
                    metadata={
                        **self.naming.get_metadata_tags(),
                        'part_id': str(part_id),
                        'part_number': part.get('partNumber', ''),
                        'revision': part.get('revision', ''),
                        'unit_of_measure': part.get('unitOfMeasure', ''),
                        'part_type': part.get('partType', ''),
                        'material_type': part.get('materialType', ''),
                        'weight': str(part.get('weight', 0)),
                        'standard_cost': str(part.get('standardCost', 0)),
                        'active': str(part.get('active', True)),
                        'last_updated': datetime.now(timezone.utc).isoformat()
                    }
                )
                assets.append(asset)
                self.extracted_parts.add(external_id)
        
        # Upload assets using deduplication
        if assets:
            result = self.dedup_helper.upsert_assets(assets)
            logger.info(f"Parts: {len(result['created'])} created, "
                       f"{len(result['updated'])} updated, {len(result['skipped'])} unchanged")
        
        return assets
    
    async def extract_boms(self):
        """Extract BOM (Bill of Materials) data"""
        logger.info(f"Extracting BOMs for PCN {self.config.facility.pcn}")
        
        # First get all parts to know which parts might have BOMs
        parts_data = await self.fetch_plex_data("/mdm/v1/parts")
        # Handle both list and dict response formats
        if isinstance(parts_data, list):
            parts = parts_data
        elif isinstance(parts_data, dict):
            parts = parts_data.get('data', [])
        else:
            parts = []
        
        if not parts:
            logger.warning("No parts found, skipping BOM extraction")
            return
        
        all_boms = []
        # Fetch BOMs for each part
        for part in parts[:10]:  # Limit to first 10 parts for testing
            part_id = part.get('id')
            if not part_id:
                continue
                
            # Fetch BOM for this part
            params = {'partId': part_id}
            bom_data = await self.fetch_plex_data("/engineering/v1/boms", params)
            if bom_data:
                # Handle both list and dict response formats
                if isinstance(bom_data, list):
                    all_boms.extend(bom_data)
                elif isinstance(bom_data, dict) and bom_data.get('data'):
                    all_boms.extend(bom_data.get('data', []))
        
        boms = all_boms
        
        assets = []
        raw_rows = []
        
        # Create BOM root using deduplication
        bom_root_id = self.naming.asset_id("BOM", "ROOT")
        root_asset = Asset(
            external_id=bom_root_id,
            name=f"BOM Hierarchy - {self.config.facility.facility_name}",
            description=f"Bill of Materials root for {self.config.facility.facility_name}",
            data_set_id=self.config.dataset_master_id,
            metadata={**self.naming.get_metadata_tags(), "type": "bom_root"}
        )
        self.dedup_helper.upsert_assets([root_asset])
        
        for bom in boms:
            bom_id = bom.get('bomId')
            if not bom_id:
                continue
            
            # Create BOM asset with PCN prefix
            external_id = self.naming.asset_id("BOM", bom_id)
            
            if external_id not in self.extracted_boms:
                # Determine parent BOM if exists
                parent_bom = bom.get('parentBomId')
                if parent_bom:
                    parent_id = self.naming.asset_id("BOM", parent_bom)
                else:
                    parent_id = bom_root_id
                
                asset = Asset(
                    external_id=external_id,
                    name=f"BOM - {bom.get('partNumber', bom_id)}",
                    parent_external_id=parent_id,
                    description=bom.get('description', ''),
                    data_set_id=self.config.dataset_master_id,
                    metadata={
                        **self.naming.get_metadata_tags(),
                        'bom_id': str(bom_id),
                        'part_number': bom.get('partNumber', ''),
                        'revision': bom.get('revision', ''),
                        'status': bom.get('status', 'active'),
                        'effective_date': bom.get('effectiveDate', ''),
                        'quantity': str(bom.get('quantity', 1)),
                        'unit_of_measure': bom.get('uom', ''),
                        'last_updated': datetime.now(timezone.utc).isoformat()
                    }
                )
                assets.append(asset)
                self.extracted_boms.add(external_id)
        
        # Upload assets using deduplication
        if assets:
            result = self.dedup_helper.upsert_assets(assets)
            logger.info(f"BOMs: {len(result['created'])} created, "
                       f"{len(result['updated'])} updated, {len(result['skipped'])} unchanged")
    
    async def extract_operations(self) -> List[Asset]:
        """Extract operation definitions as Assets"""
        logger.info(f"Extracting operations for PCN {self.config.facility.pcn}")
        
        # Fetch operation definitions
        data = await self.fetch_plex_data("/mdm/v1/operations")
        # Handle both list and dict response formats
        if isinstance(data, list):
            operations = data
        elif isinstance(data, dict):
            operations = data.get('data', [])
        else:
            operations = []
        
        # Create operations root asset
        ops_root_id = self.naming.asset_id("OPERATIONS", "ROOT")
        facility_root_id = self.naming.asset_id("FACILITY", "ROOT")
        
        root_asset = Asset(
            external_id=ops_root_id,
            name=f"Operations - {self.config.facility.facility_name}",
            parent_external_id=facility_root_id,
            description=f"Operation definitions for {self.config.facility.facility_name}",
            data_set_id=self.config.dataset_master_id,
            metadata={**self.naming.get_metadata_tags(), "type": "operations_root"}
        )
        self.dedup_helper.upsert_assets([root_asset])
        
        assets = []
        
        for op in operations:
            op_id = op.get('id') or op.get('operationId')
            if not op_id:
                continue
            
            external_id = self.naming.asset_id("OPERATION", op_id)
            
            asset = Asset(
                external_id=external_id,
                name=op.get('name', f'Operation {op_id}'),
                parent_external_id=ops_root_id,
                description=op.get('description', ''),
                data_set_id=self.config.dataset_master_id,
                metadata={
                    **self.naming.get_metadata_tags(),
                    'operation_id': str(op_id),
                    'operation_code': op.get('operationCode', ''),
                    'operation_type': op.get('operationType', ''),
                    'setup_time': str(op.get('setupTime', 0)),
                    'cycle_time': str(op.get('cycleTime', 0)),
                    'active': str(op.get('active', True)),
                    'last_updated': datetime.now(timezone.utc).isoformat()
                }
            )
            assets.append(asset)
        
        # Upload assets using deduplication
        if assets:
            result = self.dedup_helper.upsert_assets(assets)
            logger.info(f"Operations: {len(result['created'])} created, "
                       f"{len(result['updated'])} updated, {len(result['skipped'])} unchanged")
        
        return assets
    
    async def extract_buildings(self):
        """Extract building/facility structure"""
        logger.info(f"Extracting buildings for PCN {self.config.facility.pcn}")
        
        data = await self.fetch_plex_data("/mdm/v1/buildings")
        # API returns list directly
        buildings = data if isinstance(data, list) else data.get('data', [])
        
        assets = []
        facility_root_id = self.naming.asset_id("FACILITY", "ROOT")
        
        for building in buildings:
            # Use 'id' field for building ID
            building_id = building.get('id')
            if not building_id:
                continue
            
            external_id = self.naming.asset_id("BUILDING", building_id)
            
            # Use building code as name if name is empty
            building_name = building.get('name') or building.get('buildingCode', f'Building {building_id}')
            
            asset = Asset(
                external_id=external_id,
                name=building_name,
                parent_external_id=facility_root_id,
                description=f"{building.get('buildingType', '')} building in {building.get('city', '')}",
                data_set_id=self.config.dataset_master_id,
                metadata={
                    **self.naming.get_metadata_tags(),
                    'building_id': str(building_id),
                    'building_code': building.get('buildingCode', ''),
                    'building_type': building.get('buildingType', ''),
                    'address': building.get('address', ''),
                    'city': building.get('city', ''),
                    'state': building.get('state', ''),
                    'country': building.get('country', ''),
                    'zip_code': building.get('zipCode', ''),
                    'active': str(building.get('active', True)),
                    'last_updated': datetime.now(timezone.utc).isoformat()
                }
            )
            assets.append(asset)
        
        # Upload assets using deduplication
        if assets:
            result = self.dedup_helper.upsert_assets(assets)
            logger.info(f"Buildings: {len(result['created'])} created, "
                       f"{len(result['updated'])} updated, {len(result['skipped'])} unchanged")
        
        return assets
    
    async def extract(self):
        """Main extraction logic - implements abstract method from BaseExtractor"""
        logger.info(f"Starting master data extraction for PCN {self.config.facility.pcn}")
        
        # Ensure facility root exists first
        await self.ensure_facility_root()
        
        # Extract in dependency order
        await self.extract_buildings()    # Buildings first (parents of workcenters)
        await self.extract_workcenters()  # Workcenters
        await self.extract_parts()        # Parts as Assets
        await self.extract_operations()   # Operation definitions
        await self.extract_boms()         # BOMs
        
        logger.info(f"Master data extraction completed for PCN {self.config.facility.pcn}")
        logger.info(f"Extracted: {len(self.extracted_workcenters)} workcenters, "
                   f"{len(self.extracted_parts)} parts, {len(self.extracted_boms)} BOMs")
    


async def main():
    """Main entry point"""
    config = MasterDataConfig.from_env()
    
    # Validate
    required = ['PLEX_API_KEY', 'PLEX_CUSTOMER_ID', 'CDF_HOST', 
                'CDF_PROJECT', 'CDF_CLIENT_ID', 'CDF_CLIENT_SECRET', 'CDF_TOKEN_URL']
    missing = [var for var in required if not os.getenv(var)]
    
    if missing:
        logger.error(f"Missing required environment variables: {', '.join(missing)}")
        sys.exit(1)
    
    logger.info(f"Starting Master Data Extractor for {config.facility.facility_name} (PCN: {config.facility.pcn})")
    
    extractor = MasterDataExtractor(config)
    
    try:
        await extractor.run()
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())