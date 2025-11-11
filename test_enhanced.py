#!/usr/bin/env python3
"""Quick test of enhanced extractors"""

import asyncio
import os
from jobs_extractor_enhanced import EnhancedJobsExtractor
from master_data_extractor_enhanced import EnhancedMasterDataExtractor
from production_extractor_enhanced import EnhancedProductionExtractor
from inventory_extractor_enhanced import EnhancedInventoryExtractor

async def test_extractors():
    """Test each extractor individually"""
    
    print("Testing Jobs Extractor...")
    try:
        jobs = EnhancedJobsExtractor()
        result = await jobs.extract()
        print(f"  Jobs: {result.success}, Items: {result.items_processed}, Events: {result.events_created}")
        if result.errors:
            print(f"  Errors: {result.errors[:2]}")  # First 2 errors only
    except Exception as e:
        print(f"  Jobs error: {e}")
    
    print("\nTesting Master Data Extractor...")
    try:
        master = EnhancedMasterDataExtractor()
        result = await master.extract()
        print(f"  Master: {result.success}, Items: {result.items_processed}, Assets: {result.assets_created}")
        if result.errors:
            print(f"  Errors: {result.errors[:2]}")
    except Exception as e:
        print(f"  Master error: {e}")
    
    print("\nTesting Production Extractor...")
    try:
        prod = EnhancedProductionExtractor()
        result = await prod.extract()
        print(f"  Production: {result.success}, Items: {result.items_processed}, Events: {result.events_created}")
        if result.errors:
            print(f"  Errors: {result.errors[:2]}")
    except Exception as e:
        print(f"  Production error: {e}")
    
    print("\nTesting Inventory Extractor...")
    try:
        inv = EnhancedInventoryExtractor()
        result = await inv.extract()
        print(f"  Inventory: {result.success}, Items: {result.items_processed}, Assets: {result.assets_created}")
        if result.errors:
            print(f"  Errors: {result.errors[:2]}")
    except Exception as e:
        print(f"  Inventory error: {e}")

if __name__ == "__main__":
    asyncio.run(test_extractors())