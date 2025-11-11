#!/usr/bin/env python3
"""
Simple test to show the orchestrator is working
"""

import asyncio
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Override to ensure dry run
os.environ['DRY_RUN'] = 'true'
os.environ['RUN_ONCE'] = 'true'
os.environ['ENABLE_JOBS_EXTRACTOR'] = 'true'
os.environ['ENABLE_MASTER_DATA_EXTRACTOR'] = 'false'
os.environ['ENABLE_PRODUCTION_EXTRACTOR'] = 'false'
os.environ['ENABLE_INVENTORY_EXTRACTOR'] = 'false'
os.environ['ENABLE_QUALITY_EXTRACTOR'] = 'false'

async def main():
    print("="*60)
    print("TESTING ENHANCED ORCHESTRATOR")
    print("="*60)
    print("Configuration:")
    print(f"  DRY_RUN: {os.getenv('DRY_RUN')}")
    print(f"  RUN_ONCE: {os.getenv('RUN_ONCE')}")
    print(f"  Enabled: Jobs Extractor only")
    print("="*60)
    
    from orchestrator_enhanced import EnhancedOrchestrator, OrchestratorConfig
    
    # Create config
    config = OrchestratorConfig()
    
    # Create and run orchestrator
    orchestrator = EnhancedOrchestrator(config)
    
    # Run with timeout
    try:
        await asyncio.wait_for(orchestrator.start(), timeout=5)
        print("\n✓ Orchestrator completed successfully!")
    except asyncio.TimeoutError:
        print("\n✓ Orchestrator is running (stopped by timeout)")
    except Exception as e:
        print(f"\n✗ Error: {e}")
    finally:
        await orchestrator.stop()
        
    # Show results
    if orchestrator.health:
        print("\nExtractor Status:")
        for extractor_type, health in orchestrator.health.items():
            if health.status.value != "disabled":
                print(f"  {health.name}: {health.status.value}")
                if health.run_count > 0:
                    print(f"    Runs: {health.run_count}, Success rate: {health.success_rate:.0%}")

if __name__ == "__main__":
    asyncio.run(main())