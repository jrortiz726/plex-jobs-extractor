#!/usr/bin/env python3
"""
Create root assets for all extractors to ensure proper hierarchy
"""

import os
import sys
import logging
from dotenv import load_dotenv
from cognite.client import CogniteClient
from cognite.client.config import ClientConfig
from cognite.client.credentials import OAuthClientCredentials
from cognite.client.data_classes import Asset

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def init_cognite_client():
    """Initialize Cognite client"""
    creds = OAuthClientCredentials(
        token_url=os.getenv('CDF_TOKEN_URL'),
        client_id=os.getenv('CDF_CLIENT_ID'),
        client_secret=os.getenv('CDF_CLIENT_SECRET'),
        scopes=["user_impersonation"]
    )
    
    config = ClientConfig(
        client_name="root-asset-creator",
        base_url=os.getenv('CDF_HOST'),
        project=os.getenv('CDF_PROJECT'),
        credentials=creds
    )
    
    return CogniteClient(config)


def create_root_assets():
    """Create all necessary root assets"""
    client = init_cognite_client()
    pcn = os.getenv('PLEX_CUSTOMER_ID')
    facility_name = os.getenv('FACILITY_NAME', f'Facility {pcn}')
    
    # Get dataset IDs
    dataset_master_id = int(os.getenv('CDF_DATASET_PLEXMASTER', '0')) or None
    dataset_inventory_id = int(os.getenv('CDF_DATASET_PLEXINVENTORY', '0')) or None
    dataset_scheduling_id = int(os.getenv('CDF_DATASET_PLEXSCHEDULING', '0')) or None
    dataset_production_id = int(os.getenv('CDF_DATASET_PLEXPRODUCTION', '0')) or None
    
    # Define root assets to create
    root_assets = [
        {
            'external_id': f'PCN{pcn}_FACILITY_ROOT',
            'name': f'{facility_name} - Facility Root',
            'description': f'Root facility asset for {facility_name}',
            'dataset_id': dataset_master_id,
            'parent': None
        },
        {
            'external_id': f'PCN{pcn}_INVENTORY_ROOT',
            'name': f'Inventory - {facility_name}',
            'description': f'Inventory root for {facility_name}',
            'dataset_id': dataset_inventory_id or dataset_master_id,
            'parent': f'PCN{pcn}_FACILITY_ROOT'
        },
        {
            'external_id': f'PCN{pcn}_PARTS_LIBRARY',
            'name': f'Parts Library - {facility_name}',
            'description': f'Parts master data for {facility_name}',
            'dataset_id': dataset_master_id,
            'parent': f'PCN{pcn}_FACILITY_ROOT'
        },
        {
            'external_id': f'PCN{pcn}_OPERATIONS_ROOT',
            'name': f'Operations - {facility_name}',
            'description': f'Operation definitions for {facility_name}',
            'dataset_id': dataset_master_id,
            'parent': f'PCN{pcn}_FACILITY_ROOT'
        },
        {
            'external_id': f'PCN{pcn}_BOM_ROOT',
            'name': f'BOM Hierarchy - {facility_name}',
            'description': f'Bill of Materials root for {facility_name}',
            'dataset_id': dataset_master_id,
            'parent': f'PCN{pcn}_FACILITY_ROOT'
        },
        {
            'external_id': f'PCN{pcn}_PRODUCTION_SCHEDULE_ROOT',
            'name': f'Production Schedule - {facility_name}',
            'description': f'Root asset for production jobs at {facility_name}',
            'dataset_id': dataset_scheduling_id or dataset_master_id,
            'parent': f'PCN{pcn}_FACILITY_ROOT'
        }
    ]
    
    created_count = 0
    existing_count = 0
    
    for asset_def in root_assets:
        try:
            # Check if asset already exists
            existing = client.assets.retrieve(external_id=asset_def['external_id'])
            if existing:
                logger.info(f"✓ Asset already exists: {asset_def['external_id']}")
                existing_count += 1
                continue
        except:
            pass  # Asset doesn't exist, we'll create it
        
        # Create the asset
        asset = Asset(
            external_id=asset_def['external_id'],
            name=asset_def['name'],
            description=asset_def['description'],
            data_set_id=asset_def['dataset_id'],
            parent_external_id=asset_def['parent'],
            metadata={
                'pcn': pcn,
                'facility_name': facility_name,
                'source': 'PlexMES',
                'type': 'root_asset'
            }
        )
        
        try:
            client.assets.create(asset)
            logger.info(f"✅ Created root asset: {asset_def['external_id']}")
            created_count += 1
        except Exception as e:
            if "already exists" in str(e).lower():
                logger.info(f"✓ Asset already exists: {asset_def['external_id']}")
                existing_count += 1
            else:
                logger.error(f"❌ Failed to create {asset_def['external_id']}: {e}")
    
    logger.info(f"\nSummary: {created_count} created, {existing_count} already existed")


if __name__ == "__main__":
    logger.info("Creating root assets for Plex to CDF integration...")
    
    # Validate required environment variables
    required = ['PLEX_CUSTOMER_ID', 'CDF_HOST', 'CDF_PROJECT', 
                'CDF_CLIENT_ID', 'CDF_CLIENT_SECRET', 'CDF_TOKEN_URL']
    missing = [var for var in required if not os.getenv(var)]
    
    if missing:
        logger.error(f"Missing required environment variables: {', '.join(missing)}")
        sys.exit(1)
    
    try:
        create_root_assets()
        logger.info("✅ Root asset creation complete!")
    except Exception as e:
        logger.error(f"Failed to create root assets: {e}", exc_info=True)
        sys.exit(1)