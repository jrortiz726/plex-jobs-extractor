#!/usr/bin/env python3
"""
Test creating facility asset
"""

import os
import asyncio
from dotenv import load_dotenv
from cognite.client import CogniteClient
from cognite.client.config import ClientConfig
from cognite.client.credentials import OAuthClientCredentials
from cognite.client.data_classes import Asset

# Load environment variables
load_dotenv()

def init_cdf_client():
    """Initialize CDF client"""
    creds = OAuthClientCredentials(
        token_url=os.getenv("CDF_TOKEN_URL"),
        client_id=os.getenv("CDF_CLIENT_ID"),
        client_secret=os.getenv("CDF_CLIENT_SECRET"),
        scopes=["user_impersonation"]
    )
    
    return CogniteClient(
        ClientConfig(
            client_name="test-facility",
            base_url=os.getenv("CDF_HOST"),
            project=os.getenv("CDF_PROJECT"),
            credentials=creds
        )
    )

def main():
    """Create facility asset"""
    print("Creating facility asset...")
    
    client = init_cdf_client()
    pcn = os.getenv("PLEX_CUSTOMER_ID", "340884")
    
    # Create facility asset
    facility_asset = Asset(
        external_id=f"PCN{pcn}_facility_{pcn}",
        name=f"RADEMO - Facility",
        description=f"Facility for PCN {pcn}",
        metadata={
            'pcn': pcn,
            'facility_name': 'RADEMO',
            'facility_code': 'DEFAULT',
            'timezone': 'UTC',
            'country': 'US',
            'type': 'facility',
            'created_by': 'test_script'
        },
        data_set_id=int(os.getenv("CDF_DATASET_PLEXMASTER"))
    )
    
    try:
        # Upsert facility asset
        result = client.assets.upsert([facility_asset])
        print(f"✓ Created facility asset: {result[0].external_id}")
        
        # Now create master root with facility as parent
        master_root = Asset(
            external_id=f"PCN{pcn}_master_root_{pcn}",
            name=f"RADEMO - Master Data",
            parent_external_id=f"PCN{pcn}_facility_{pcn}",
            metadata={'type': 'master_root'},
            data_set_id=int(os.getenv("CDF_DATASET_PLEXMASTER"))
        )
        
        result = client.assets.upsert([master_root])
        print(f"✓ Created master root: {result[0].external_id}")
        
    except Exception as e:
        print(f"✗ Error: {e}")

if __name__ == "__main__":
    main()