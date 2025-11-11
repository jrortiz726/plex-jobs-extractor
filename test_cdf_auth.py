#!/usr/bin/env python3
"""
Simple CDF authentication test using the default (non-enhanced) method
"""

import os
from dotenv import load_dotenv
from cognite.client import CogniteClient
from cognite.client.config import ClientConfig
from cognite.client.credentials import OAuthClientCredentials

# Load environment variables
load_dotenv()

def test_cdf_auth():
    """Test CDF authentication"""
    print("Testing CDF authentication...")
    print(f"CDF_HOST: {os.getenv('CDF_HOST')}")
    print(f"CDF_PROJECT: {os.getenv('CDF_PROJECT')}")
    print(f"CDF_CLIENT_ID: {os.getenv('CDF_CLIENT_ID')[:10]}...")
    print(f"CDF_TOKEN_URL: {os.getenv('CDF_TOKEN_URL')}")

    try:
        # Use the same authentication as base_extractor.py
        creds = OAuthClientCredentials(
            token_url=os.getenv('CDF_TOKEN_URL'),
            client_id=os.getenv('CDF_CLIENT_ID'),
            client_secret=os.getenv('CDF_CLIENT_SECRET'),
            scopes=["user_impersonation"]
        )

        config = ClientConfig(
            client_name="test-auth-client",
            base_url=os.getenv('CDF_HOST'),
            project=os.getenv('CDF_PROJECT'),
            credentials=creds
        )

        client = CogniteClient(config)

        # Test the connection
        print("\n✓ Client initialized")

        # Try to list datasets
        datasets = client.data_sets.list(limit=5)
        print(f"✓ Successfully connected to CDF")
        print(f"✓ Found {len(datasets)} datasets:")
        for ds in datasets:
            print(f"  - {ds.name} (ID: {ds.id})")

        return True

    except Exception as e:
        print(f"\n✗ Authentication failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_cdf_auth()
    exit(0 if success else 1)
