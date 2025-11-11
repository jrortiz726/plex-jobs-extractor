#!/usr/bin/env python3
"""
Test script to validate Master Data and Quality extractor connections and functionality.
Tests API endpoints, authentication, and basic data retrieval.
"""

import asyncio
import os
import sys
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
import httpx
from dotenv import load_dotenv
import json
import base64

# Load environment variables
load_dotenv()

class TestResults:
    """Track test results"""
    def __init__(self):
        self.passed: List[str] = []
        self.failed: List[Dict[str, Any]] = []
        self.warnings: List[str] = []
    
    def add_pass(self, test_name: str):
        self.passed.append(test_name)
        print(f"✓ {test_name}")
    
    def add_fail(self, test_name: str, error: str):
        self.failed.append({"test": test_name, "error": error})
        print(f"✗ {test_name}: {error}")
    
    def add_warning(self, message: str):
        self.warnings.append(message)
        print(f"⚠ {message}")
    
    def print_summary(self):
        print("\n" + "="*60)
        print("TEST SUMMARY")
        print("="*60)
        print(f"Passed: {len(self.passed)}")
        print(f"Failed: {len(self.failed)}")
        print(f"Warnings: {len(self.warnings)}")
        
        if self.failed:
            print("\nFailed Tests:")
            for fail in self.failed:
                print(f"  - {fail['test']}: {fail['error']}")
        
        if self.warnings:
            print("\nWarnings:")
            for warning in self.warnings:
                print(f"  - {warning}")
        
        return len(self.failed) == 0

class PlexAPITester:
    """Test Plex API connections"""
    
    def __init__(self):
        self.api_key = os.getenv("PLEX_API_KEY")
        self.customer_id = os.getenv("PLEX_CUSTOMER_ID", "340884")
        self.base_url = "https://connect.plex.com"
        self.results = TestResults()
        
        # Plex API headers
        self.plex_headers = {
            "X-Plex-Connect-Api-Key": self.api_key,
            "X-Plex-Connect-Customer-Id": self.customer_id,
            "Content-Type": "application/json"
        }
        
        # Data Source API credentials
        self.ds_username = os.getenv("PLEX_DS_USERNAME")
        self.ds_password = os.getenv("PLEX_DS_PASSWORD")
    
    async def test_basic_auth(self) -> bool:
        """Test basic Plex API authentication"""
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    f"{self.base_url}/scheduling/v1/jobs",
                    params={"limit": 1},
                    headers=self.plex_headers
                )
                if response.status_code == 200:
                    self.results.add_pass("Basic API Authentication")
                    return True
                else:
                    self.results.add_fail("Basic API Authentication", f"Status {response.status_code}")
                    return False
        except Exception as e:
            self.results.add_fail("Basic API Authentication", str(e))
            return False
    
    async def test_master_data_endpoints(self):
        """Test Master Data API endpoints"""
        print("\n--- Testing Master Data Endpoints ---")
        
        endpoints = [
            ("/mdm/v1/parts", "Parts API"),
            ("/mdm/v1/part-operations", "Operations API"),
            ("/mdm/v1/boms", "BOM API"),
            ("/mdm/v1/routings", "Routing API"),
            ("/mdm/v1/resources", "Resources API"),
        ]
        
        async with httpx.AsyncClient() as client:
            for endpoint, name in endpoints:
                try:
                    # Test with limit=1 to avoid large responses
                    response = await client.get(
                        f"{self.base_url}{endpoint}",
                        params={"limit": 1},
                        headers=self.plex_headers,
                        timeout=10.0
                    )
                    
                    if response.status_code == 200:
                        data = response.json()
                        self.results.add_pass(f"{name} - Connection OK")
                        
                        # Handle both list and dict responses
                        if isinstance(data, list):
                            if data:
                                self.results.add_pass(f"{name} - Has data ({len(data)} items)")
                            else:
                                self.results.add_warning(f"{name} - No data returned")
                        elif isinstance(data, dict):
                            if data.get("data"):
                                self.results.add_pass(f"{name} - Has data")
                            else:
                                self.results.add_warning(f"{name} - No data in response")
                        else:
                            self.results.add_warning(f"{name} - Unexpected response type: {type(data)}")
                    else:
                        self.results.add_fail(name, f"Status {response.status_code}")
                        
                except httpx.TimeoutException:
                    self.results.add_fail(name, "Timeout")
                except Exception as e:
                    self.results.add_fail(name, str(e))
    
    async def test_quality_endpoints(self):
        """Test Quality API endpoints"""
        print("\n--- Testing Quality Endpoints ---")
        
        # Regular API endpoints - using mdm prefix for quality
        endpoints = [
            ("/quality/v1/defects", "NCR/Defects API"),
            ("/quality/v1/quality-checks", "Quality Checks API"),
            ("/mdm/v1/quality/checksheets", "Checksheets API"),
            ("/mdm/v1/quality/inspections", "Inspections API"),
            ("/quality/v1/issues", "Quality Issues API"),
        ]
        
        async with httpx.AsyncClient() as client:
            for endpoint, name in endpoints:
                try:
                    # Calculate date range for NCRs
                    if "ncrs" in endpoint:
                        end_date = datetime.now()
                        start_date = end_date - timedelta(days=7)
                        params = {
                            "startDate": start_date.isoformat(),
                            "endDate": end_date.isoformat(),
                            "limit": 1
                        }
                    else:
                        params = {"limit": 1}
                    
                    response = await client.get(
                        f"{self.base_url}{endpoint}",
                        params=params,
                        headers=self.plex_headers,
                        timeout=10.0
                    )
                    
                    if response.status_code == 200:
                        data = response.json()
                        self.results.add_pass(f"{name} - Connection OK")
                        if data:
                            self.results.add_pass(f"{name} - Has data")
                        else:
                            self.results.add_warning(f"{name} - No data returned")
                    else:
                        self.results.add_fail(name, f"Status {response.status_code}")
                        
                except httpx.TimeoutException:
                    self.results.add_fail(name, "Timeout")
                except Exception as e:
                    self.results.add_fail(name, str(e))
    
    async def test_datasource_api(self):
        """Test Data Source API authentication and endpoints"""
        print("\n--- Testing Data Source API ---")
        
        if not self.ds_username or not self.ds_password:
            self.results.add_warning("Data Source API credentials not configured")
            return
        
        # Create auth header
        credentials = f"{self.ds_username}:{self.ds_password}"
        encoded = base64.b64encode(credentials.encode('utf-8')).decode('ascii')
        auth_header = f"Basic {encoded}"
        
        # Data source IDs to test
        datasources = [
            (6429, "Specification_Get"),
            (4142, "Checksheets_Get"),
            (4760, "Inspection_Modes_Get"),
        ]
        
        async with httpx.AsyncClient() as client:
            for ds_id, name in datasources:
                try:
                    url = f"https://cloud.plex.com/api/datasource/execute/{self.customer_id}/{ds_id}"
                    
                    # Test with minimal parameters
                    inputs = {
                        "Format Type": 2,  # JSON format
                        "Company Code": "MANUTENCAO"  # Default company
                    }
                    
                    response = await client.post(
                        url,
                        json={"inputs": inputs},
                        headers={"Authorization": auth_header},
                        timeout=15.0
                    )
                    
                    if response.status_code == 200:
                        data = response.json()
                        self.results.add_pass(f"Data Source API - {name}")
                        
                        # Check if we got data
                        if "outputs" in data:
                            self.results.add_pass(f"Data Source API - {name} has outputs")
                        else:
                            self.results.add_warning(f"Data Source API - {name} no outputs")
                    elif response.status_code == 401:
                        self.results.add_fail(f"Data Source API - {name}", "Authentication failed")
                    else:
                        self.results.add_fail(f"Data Source API - {name}", f"Status {response.status_code}")
                        
                except httpx.TimeoutException:
                    self.results.add_fail(f"Data Source API - {name}", "Timeout")
                except Exception as e:
                    self.results.add_fail(f"Data Source API - {name}", str(e))
    
    async def test_cdf_connection(self):
        """Test CDF connection"""
        print("\n--- Testing CDF Connection ---")
        
        try:
            from cognite.client import CogniteClient
            from cognite.client.credentials import OAuthClientCredentials
            from cognite.client.config import ClientConfig
            
            creds = OAuthClientCredentials(
                token_url=os.getenv("CDF_TOKEN_URL"),
                client_id=os.getenv("CDF_CLIENT_ID"),
                client_secret=os.getenv("CDF_CLIENT_SECRET"),
                scopes=[f"{os.getenv('CDF_HOST')}/.default"]
            )
            
            client = CogniteClient(
                ClientConfig(
                    client_name="plex-test",
                    base_url=os.getenv("CDF_HOST"),
                    project=os.getenv("CDF_PROJECT"),
                    credentials=creds
                )
            )
            
            # Test basic connection
            project = client.iam.token.inspect()
            if project:
                self.results.add_pass("CDF Authentication")
                self.results.add_pass(f"CDF Project: {project.projects[0]}")
            else:
                self.results.add_fail("CDF Authentication", "Failed to get project info")
                
            # Test dataset access
            dataset_ids = {
                "Master": os.getenv("CDF_DATASET_PLEXMASTER"),
                "Quality": os.getenv("CDF_DATASET_PLEXQUALITY"),
            }
            
            for name, dataset_id in dataset_ids.items():
                if dataset_id:
                    try:
                        dataset = client.data_sets.retrieve(id=int(dataset_id))
                        if dataset:
                            self.results.add_pass(f"CDF Dataset {name}: {dataset.name}")
                        else:
                            self.results.add_fail(f"CDF Dataset {name}", "Not found")
                    except Exception as e:
                        self.results.add_fail(f"CDF Dataset {name}", str(e))
                else:
                    self.results.add_warning(f"CDF Dataset {name} ID not configured")
                    
        except ImportError:
            self.results.add_fail("CDF Connection", "cognite-sdk not installed")
        except Exception as e:
            self.results.add_fail("CDF Connection", str(e))
    
    async def run_all_tests(self):
        """Run all connection tests"""
        print("="*60)
        print("PLEX-COGNITE CONNECTION TEST")
        print("="*60)
        
        # Check basic configuration
        if not self.api_key:
            print("ERROR: PLEX_API_KEY not configured")
            return False
        
        # Run tests in sequence
        auth_ok = await self.test_basic_auth()
        
        if auth_ok:
            # Test API endpoints
            await self.test_master_data_endpoints()
            await self.test_quality_endpoints()
            await self.test_datasource_api()
        
        # Test CDF connection independently
        await self.test_cdf_connection()
        
        # Print summary
        return self.results.print_summary()

async def main():
    """Main test runner"""
    tester = PlexAPITester()
    success = await tester.run_all_tests()
    
    # Exit with appropriate code
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    # Check for cognite-sdk
    try:
        from cognite.client.config import ClientConfig
    except ImportError:
        print("Warning: cognite-sdk not installed. CDF tests will be skipped.")
        print("Install with: pip install cognite-sdk")
    
    asyncio.run(main())