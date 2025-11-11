#!/usr/bin/env python3
"""
Test connectivity for enhanced extractors
Validates that Master Data and available endpoints are working
"""

import asyncio
import os
from datetime import datetime, timedelta
import httpx
from dotenv import load_dotenv
import json

load_dotenv()

class EnhancedExtractorTester:
    """Test enhanced extractor connectivity"""
    
    def __init__(self):
        self.api_key = os.getenv("PLEX_API_KEY")
        self.customer_id = os.getenv("PLEX_CUSTOMER_ID", "340884")
        self.base_url = "https://connect.plex.com"
        
        self.headers = {
            "X-Plex-Connect-Api-Key": self.api_key,
            "X-Plex-Connect-Customer-Id": self.customer_id,
            "Content-Type": "application/json"
        }
        
        self.results = {
            "master_data": {},
            "production": {},
            "inventory": {},
            "quality": {},
            "cdf": {}
        }
    
    async def test_master_data_extractor(self):
        """Test Master Data Extractor endpoints"""
        print("\n" + "="*60)
        print("MASTER DATA EXTRACTOR - Connectivity Test")
        print("="*60)
        
        endpoints = [
            ("/mdm/v1/parts", "Parts"),
            ("/mdm/v1/part-operations", "Operations/Routings"),
            ("/mdm/v1/operations", "Operations"),
            ("/mdm/v1/suppliers", "Suppliers"),
            ("/mdm/v1/customers", "Customers"),
        ]
        
        async with httpx.AsyncClient() as client:
            for endpoint, name in endpoints:
                try:
                    response = await client.get(
                        f"{self.base_url}{endpoint}",
                        params={"limit": 5},
                        headers=self.headers,
                        timeout=10.0
                    )
                    
                    if response.status_code == 200:
                        data = response.json()
                        count = len(data) if isinstance(data, list) else len(data.get("data", []))
                        print(f"✓ {name}: {count} records fetched")
                        self.results["master_data"][name] = {"status": "OK", "count": count}
                        
                        # Show sample data structure
                        if count > 0:
                            sample = data[0] if isinstance(data, list) else data["data"][0]
                            print(f"  Sample fields: {list(sample.keys())[:5]}")
                    else:
                        print(f"✗ {name}: Status {response.status_code}")
                        self.results["master_data"][name] = {"status": f"Error {response.status_code}"}
                        
                except Exception as e:
                    print(f"✗ {name}: {str(e)[:50]}")
                    self.results["master_data"][name] = {"status": "Exception", "error": str(e)[:50]}
    
    async def test_production_extractor(self):
        """Test Production Extractor endpoints"""
        print("\n" + "="*60)
        print("PRODUCTION EXTRACTOR - Connectivity Test")
        print("="*60)
        
        endpoints = [
            ("/production/v1/production-history/production-entries", "Production Entries"),
            ("/production/v1/production-history/production-summaries", "Production Summaries"),
            ("/production/v1/workcenters", "Workcenters"),
        ]
        
        async with httpx.AsyncClient() as client:
            for endpoint, name in endpoints:
                try:
                    # Production endpoints often need date parameters
                    params = {"limit": 5}
                    if "production-history" in endpoint:
                        end_date = datetime.now()
                        start_date = end_date - timedelta(days=7)
                        params.update({
                            "startDate": start_date.isoformat(),
                            "endDate": end_date.isoformat()
                        })
                    
                    response = await client.get(
                        f"{self.base_url}{endpoint}",
                        params=params,
                        headers=self.headers,
                        timeout=10.0
                    )
                    
                    if response.status_code == 200:
                        data = response.json()
                        count = len(data) if isinstance(data, list) else len(data.get("data", []))
                        print(f"✓ {name}: {count} records fetched")
                        self.results["production"][name] = {"status": "OK", "count": count}
                    elif response.status_code == 400:
                        print(f"⚠ {name}: Needs parameters (Status 400)")
                        self.results["production"][name] = {"status": "Needs params"}
                    else:
                        print(f"✗ {name}: Status {response.status_code}")
                        self.results["production"][name] = {"status": f"Error {response.status_code}"}
                        
                except Exception as e:
                    print(f"✗ {name}: {str(e)[:50]}")
                    self.results["production"][name] = {"status": "Exception", "error": str(e)[:50]}
    
    async def test_inventory_extractor(self):
        """Test Inventory Extractor endpoints"""
        print("\n" + "="*60)
        print("INVENTORY EXTRACTOR - Connectivity Test")
        print("="*60)
        
        endpoints = [
            ("/inventory/v1/inventory-tracking/containers", "Containers"),
            ("/inventory/v1/inventory-tracking/locations", "Locations"),
            ("/inventory/v1/inventory-tracking/movements", "Movements"),
        ]
        
        async with httpx.AsyncClient() as client:
            for endpoint, name in endpoints:
                try:
                    response = await client.get(
                        f"{self.base_url}{endpoint}",
                        params={"limit": 5},
                        headers=self.headers,
                        timeout=10.0
                    )
                    
                    if response.status_code == 200:
                        data = response.json()
                        count = len(data) if isinstance(data, list) else len(data.get("data", []))
                        print(f"✓ {name}: {count} records fetched")
                        self.results["inventory"][name] = {"status": "OK", "count": count}
                    else:
                        print(f"✗ {name}: Status {response.status_code}")
                        self.results["inventory"][name] = {"status": f"Error {response.status_code}"}
                        
                except Exception as e:
                    print(f"✗ {name}: {str(e)[:50]}")
                    self.results["inventory"][name] = {"status": "Exception", "error": str(e)[:50]}
    
    async def test_jobs_extractor(self):
        """Test Jobs/Scheduling Extractor endpoints"""
        print("\n" + "="*60)
        print("JOBS EXTRACTOR - Connectivity Test")
        print("="*60)
        
        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(
                    f"{self.base_url}/scheduling/v1/jobs",
                    params={"limit": 5},
                    headers=self.headers,
                    timeout=10.0
                )
                
                if response.status_code == 200:
                    data = response.json()
                    count = len(data) if isinstance(data, list) else len(data.get("data", []))
                    print(f"✓ Jobs: {count} records fetched")
                    
                    if count > 0:
                        sample = data[0] if isinstance(data, list) else data["data"][0]
                        print(f"  Sample fields: {list(sample.keys())[:8]}")
                        
                        # Check job statuses
                        statuses = set()
                        jobs = data if isinstance(data, list) else data.get("data", [])
                        for job in jobs:
                            if "status" in job:
                                statuses.add(job["status"])
                        if statuses:
                            print(f"  Job statuses found: {statuses}")
                else:
                    print(f"✗ Jobs: Status {response.status_code}")
                    
            except Exception as e:
                print(f"✗ Jobs: {str(e)[:50]}")
    
    async def test_cdf_basics(self):
        """Test basic CDF connectivity"""
        print("\n" + "="*60)
        print("CDF - Basic Connectivity Test")
        print("="*60)
        
        try:
            from cognite.client import CogniteClient
            from cognite.client.credentials import OAuthClientCredentials
            from cognite.client.config import ClientConfig
            
            print("✓ CDF SDK imported successfully")
            
            # Check environment variables
            required_vars = ["CDF_HOST", "CDF_PROJECT", "CDF_CLIENT_ID", "CDF_CLIENT_SECRET", "CDF_TOKEN_URL"]
            missing = [var for var in required_vars if not os.getenv(var)]
            
            if missing:
                print(f"✗ Missing environment variables: {missing}")
                self.results["cdf"]["config"] = {"status": "Missing vars", "missing": missing}
            else:
                print("✓ All CDF environment variables configured")
                self.results["cdf"]["config"] = {"status": "OK"}
                
                # Check dataset IDs
                datasets = {
                    "Master": os.getenv("CDF_DATASET_PLEXMASTER"),
                    "Quality": os.getenv("CDF_DATASET_PLEXQUALITY"),
                    "Production": os.getenv("CDF_DATASET_PLEXPRODUCTION"),
                    "Inventory": os.getenv("CDF_DATASET_PLEXINVENTORY"),
                    "Scheduling": os.getenv("CDF_DATASET_PLEXSCHEDULING"),
                }
                
                configured = {k: v for k, v in datasets.items() if v}
                print(f"✓ Datasets configured: {list(configured.keys())}")
                self.results["cdf"]["datasets"] = configured
                
        except ImportError as e:
            print(f"✗ CDF SDK not installed: {e}")
            self.results["cdf"]["sdk"] = {"status": "Not installed"}
    
    async def run_all_tests(self):
        """Run all extractor tests"""
        print("="*60)
        print("ENHANCED EXTRACTOR CONNECTIVITY TEST SUITE")
        print("="*60)
        print(f"Testing with Customer ID: {self.customer_id}")
        print(f"Base URL: {self.base_url}")
        
        # Run tests
        await self.test_master_data_extractor()
        await self.test_jobs_extractor()
        await self.test_production_extractor()
        await self.test_inventory_extractor()
        await self.test_cdf_basics()
        
        # Print summary
        print("\n" + "="*60)
        print("SUMMARY")
        print("="*60)
        
        # Count successes
        total_ok = 0
        total_failed = 0
        
        for category, results in self.results.items():
            if isinstance(results, dict):
                ok_count = sum(1 for r in results.values() if isinstance(r, dict) and r.get("status") == "OK")
                fail_count = sum(1 for r in results.values() if isinstance(r, dict) and r.get("status") not in ["OK", None])
                
                if ok_count > 0 or fail_count > 0:
                    print(f"{category.upper()}: {ok_count} OK, {fail_count} Failed")
                    total_ok += ok_count
                    total_failed += fail_count
        
        print(f"\nTOTAL: {total_ok} endpoints working, {total_failed} failed")
        
        # Recommendations
        print("\n" + "="*60)
        print("RECOMMENDATIONS")
        print("="*60)
        
        if total_ok > 0:
            print("✓ Basic connectivity is working for master data and jobs")
            print("✓ Enhanced extractors can be tested with available endpoints")
        
        if total_failed > 0:
            print("⚠ Some endpoints are not available or need different parameters")
            print("⚠ Quality endpoints may require Data Source API access")
        
        print("\nNext steps:")
        print("1. Run master_data_extractor_enhanced.py to test master data extraction")
        print("2. Run jobs_extractor_enhanced.py to test job extraction")
        print("3. Configure Data Source API credentials for quality data")
        print("4. Set up CDF authentication for full pipeline testing")

async def main():
    """Main test runner"""
    tester = EnhancedExtractorTester()
    await tester.run_all_tests()

if __name__ == "__main__":
    asyncio.run(main())