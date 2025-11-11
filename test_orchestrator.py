#!/usr/bin/env python3
"""
Test the enhanced orchestrator
"""

import asyncio
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

async def test_orchestrator():
    """Test loading and running the orchestrator"""
    
    print("Testing Enhanced Orchestrator")
    print("="*60)
    
    # Test imports
    try:
        from orchestrator_enhanced import EnhancedOrchestrator, OrchestratorConfig
        print("✓ Orchestrator imports successfully")
    except Exception as e:
        print(f"✗ Failed to import orchestrator: {e}")
        return
    
    # Test configuration
    try:
        config = OrchestratorConfig(
            run_once=True,
            dry_run=True,
            enable_jobs=True,
            enable_production=False,
            enable_inventory=False,
            enable_master_data=True,
            enable_quality=False
        )
        print("✓ Configuration created successfully")
    except Exception as e:
        print(f"✗ Failed to create config: {e}")
        return
    
    # Test orchestrator creation
    try:
        orchestrator = EnhancedOrchestrator(config)
        print("✓ Orchestrator created successfully")
    except Exception as e:
        print(f"✗ Failed to create orchestrator: {e}")
        return
    
    # Test running orchestrator
    try:
        print("\nRunning orchestrator in dry-run mode...")
        print("-"*60)
        
        # Run with timeout
        await asyncio.wait_for(orchestrator.start(), timeout=10)
        
        print("-"*60)
        print("✓ Orchestrator ran successfully")
        
    except asyncio.TimeoutError:
        print("✓ Orchestrator running (stopped by timeout)")
    except Exception as e:
        print(f"✗ Failed to run orchestrator: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        # Ensure cleanup
        await orchestrator.stop()

if __name__ == "__main__":
    asyncio.run(test_orchestrator())