#!/usr/bin/env python3
"""
Test Master Data and Quality API endpoints specifically
"""

import httpx
import os
from dotenv import load_dotenv
import json

load_dotenv()

def test_endpoints():
    """Test various API endpoints to find what's available"""
    
    api_key = os.getenv("PLEX_API_KEY")
    customer_id = os.getenv("PLEX_CUSTOMER_ID", "340884")
    
    headers = {
        "X-Plex-Connect-Api-Key": api_key,
        "X-Plex-Connect-Customer-Id": customer_id,
        "Content-Type": "application/json"
    }
    
    print("="*60)
    print("MASTER DATA & QUALITY API ENDPOINT DISCOVERY")
    print("="*60)
    
    # Master Data endpoints to test
    master_endpoints = [
        "/mdm/v1/parts",
        "/mdm/v1/part-operations",
        "/mdm/v1/boms",
        "/mdm/v1/operations",
        "/production/v1/workcenters",
        "/production/v1/resources",
        "/inventory/v1/inventory-tracking/parts",
        "/mdm/v1/suppliers",
        "/mdm/v1/customers",
    ]
    
    print("\n--- Master Data Endpoints ---")
    for endpoint in master_endpoints:
        url = f"https://connect.plex.com{endpoint}"
        try:
            resp = httpx.get(url, params={"limit": 1}, headers=headers, timeout=5.0)
            if resp.status_code == 200:
                data = resp.json()
                if isinstance(data, list):
                    print(f"✓ {endpoint}: OK (list with {len(data)} items)")
                elif isinstance(data, dict):
                    keys = list(data.keys())[:3]
                    print(f"✓ {endpoint}: OK (dict with keys: {keys})")
                else:
                    print(f"✓ {endpoint}: OK ({type(data).__name__})")
            else:
                print(f"✗ {endpoint}: {resp.status_code}")
        except Exception as e:
            print(f"✗ {endpoint}: Error - {str(e)[:50]}")
    
    # Quality endpoints to test
    quality_endpoints = [
        "/quality/v1/defects",
        "/quality/v1/inspections", 
        "/quality/v1/ncrs",
        "/quality/v1/quality-checks",
        "/production/v1/quality/inspections",
        "/production/v1/quality/defects",
        "/mdm/v1/quality/specifications",
        "/mdm/v1/quality/checksheets",
    ]
    
    print("\n--- Quality Endpoints ---")
    for endpoint in quality_endpoints:
        url = f"https://connect.plex.com{endpoint}"
        try:
            resp = httpx.get(url, params={"limit": 1}, headers=headers, timeout=5.0)
            if resp.status_code == 200:
                data = resp.json()
                if isinstance(data, list):
                    print(f"✓ {endpoint}: OK (list with {len(data)} items)")
                elif isinstance(data, dict):
                    keys = list(data.keys())[:3]
                    print(f"✓ {endpoint}: OK (dict with keys: {keys})")
                else:
                    print(f"✓ {endpoint}: OK ({type(data).__name__})")
            else:
                print(f"✗ {endpoint}: {resp.status_code}")
        except Exception as e:
            print(f"✗ {endpoint}: Error - {str(e)[:50]}")
    
    # Test production endpoints that might have quality data
    production_endpoints = [
        "/production/v1/scheduling/jobs",
        "/production/v1/production-history/production-entries",
        "/production/v1/production-history/production-summaries",
        "/production/v1/workcenters/status",
    ]
    
    print("\n--- Production Endpoints (for context) ---")
    for endpoint in production_endpoints:
        url = f"https://connect.plex.com{endpoint}"
        try:
            resp = httpx.get(url, params={"limit": 1}, headers=headers, timeout=5.0)
            if resp.status_code == 200:
                data = resp.json()
                if isinstance(data, list):
                    print(f"✓ {endpoint}: OK (list with {len(data)} items)")
                elif isinstance(data, dict):
                    keys = list(data.keys())[:3]
                    print(f"✓ {endpoint}: OK (dict with keys: {keys})")
                else:
                    print(f"✓ {endpoint}: OK ({type(data).__name__})")
            else:
                print(f"✗ {endpoint}: {resp.status_code}")
        except Exception as e:
            print(f"✗ {endpoint}: Error - {str(e)[:50]}")

if __name__ == "__main__":
    test_endpoints()