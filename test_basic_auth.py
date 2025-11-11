#!/usr/bin/env python3
"""
Basic authentication test for Plex API
"""

import os
import httpx
from dotenv import load_dotenv

load_dotenv()

def test_plex_auth():
    """Test basic Plex API authentication"""
    api_key = os.getenv("PLEX_API_KEY")
    customer_id = os.getenv("PLEX_CUSTOMER_ID", "340884")
    
    if not api_key:
        print("ERROR: PLEX_API_KEY not configured")
        return
    
    print(f"Testing Plex API with customer ID: {customer_id}")
    print(f"API Key (first 10 chars): {api_key[:10]}...")
    
    # Try different endpoint variations
    endpoints = [
        f"https://connect.plex.com/scheduling/v1/jobs?limit=1",
        f"https://connect.plex.com/production/v1/scheduling/jobs?limit=1",
        f"https://connect.plex.com/inventory/v1/inventory-tracking/containers?limit=1",
        f"https://connect.plex.com/mdm/v1/parts?limit=1",
    ]
    
    for url in endpoints:
        print(f"\nTrying: {url}")
        try:
            response = httpx.get(
                url,
                headers={
                    "X-Plex-Connect-Api-Key": api_key,
                    "X-Plex-Connect-Customer-Id": customer_id,
                    "Content-Type": "application/json"
                },
                timeout=10.0
            )
            print(f"  Status: {response.status_code}")
            if response.status_code == 200:
                print("  ✓ Success!")
                data = response.json()
                if isinstance(data, dict):
                    print(f"  Response keys: {list(data.keys())[:5]}")
                break
            elif response.status_code == 401:
                print("  ✗ Authentication failed")
            elif response.status_code == 404:
                print("  ✗ Endpoint not found")
            else:
                print(f"  Response: {response.text[:200]}")
        except Exception as e:
            print(f"  Error: {e}")

if __name__ == "__main__":
    test_plex_auth()