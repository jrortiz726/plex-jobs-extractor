#!/usr/bin/env python3
"""
Plex-CDF Extractor Orchestrator

Manages and coordinates all extractors with proper scheduling,
error handling, and monitoring.
"""

import os
import sys
import asyncio
import logging
import signal
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from enum import Enum
import concurrent.futures
from threading import Thread
import time

from dotenv import load_dotenv
from cognite.client import CogniteClient
from cognite.client.config import ClientConfig
from cognite.client.credentials import OAuthClientCredentials

# Import all extractors
from jobs_extractor import PlexJobsExtractor, ExtractorConfig as JobsConfig
from production_extractor import PlexProductionExtractor, ProductionExtractorConfig as ProductionConfig
from master_data_extractor import MasterDataExtractor, MasterDataConfig
from inventory_extractor import InventoryExtractor, InventoryConfig
from quality_extractor import QualityExtractor, QualityConfig

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('orchestrator.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class ExtractorType(Enum):
    """Types of extractors with their schedules"""
    MASTER_DATA = ("master_data", 86400)      # Daily (24 hours)
    JOBS = ("jobs", 300)                      # Every 5 minutes
    PRODUCTION = ("production", 300)          # Every 5 minutes
    INVENTORY = ("inventory", 300)            # Every 5 minutes
    QUALITY = ("quality", 300)                # Every 5 minutes


@dataclass
class ExtractorStatus:
    """Track status of each extractor"""
    name: str
    last_run: Optional[datetime] = None
    last_success: Optional[datetime] = None
    last_error: Optional[str] = None
    run_count: int = 0
    error_count: int = 0
    is_running: bool = False


class ExtractorOrchestrator:
    """Orchestrates multiple extractors with scheduling and monitoring"""
    
    def __init__(self):
        self.extractors: Dict[str, Any] = {}
        self.statuses: Dict[str, ExtractorStatus] = {}
        self.shutdown_event = asyncio.Event()
        self.cognite_client = self._init_cognite_client()
        self.pcn = os.getenv('PLEX_CUSTOMER_ID')
        
        # Initialize extractors
        self._initialize_extractors()
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _init_cognite_client(self) -> CogniteClient:
        """Initialize Cognite client"""
        creds = OAuthClientCredentials(
            token_url=os.getenv('CDF_TOKEN_URL'),
            client_id=os.getenv('CDF_CLIENT_ID'),
            client_secret=os.getenv('CDF_CLIENT_SECRET'),
            scopes=["user_impersonation"]
        )
        
        config = ClientConfig(
            client_name="plex-orchestrator",
            base_url=os.getenv('CDF_HOST'),
            project=os.getenv('CDF_PROJECT'),
            credentials=creds
        )
        
        return CogniteClient(config)
    
    def _initialize_extractors(self):
        """Initialize all extractors with their configurations"""
        logger.info(f"Initializing extractors for PCN: {self.pcn}")
        
        # Master Data Extractor
        try:
            master_config = MasterDataConfig.from_env()
            self.extractors['master_data'] = MasterDataExtractor(master_config)
            self.statuses['master_data'] = ExtractorStatus('master_data')
            logger.info("✓ Master Data Extractor initialized")
        except Exception as e:
            logger.error(f"Failed to initialize Master Data Extractor: {e}")
        
        # Jobs Extractor
        try:
            jobs_config = JobsConfig.from_env()
            self.extractors['jobs'] = PlexJobsExtractor(jobs_config)
            self.statuses['jobs'] = ExtractorStatus('jobs')
            logger.info("✓ Jobs Extractor initialized")
        except Exception as e:
            logger.error(f"Failed to initialize Jobs Extractor: {e}")
        
        # Production Extractor
        try:
            prod_config = ProductionConfig.from_env()
            self.extractors['production'] = PlexProductionExtractor(prod_config)
            self.statuses['production'] = ExtractorStatus('production')
            logger.info("✓ Production Extractor initialized")
        except Exception as e:
            logger.error(f"Failed to initialize Production Extractor: {e}")
        
        # Inventory Extractor
        try:
            inv_config = InventoryConfig.from_env()
            self.extractors['inventory'] = InventoryExtractor(inv_config)
            self.statuses['inventory'] = ExtractorStatus('inventory')
            logger.info("✓ Inventory Extractor initialized")
        except Exception as e:
            logger.error(f"Failed to initialize Inventory Extractor: {e}")
        
        # Quality Extractor
        try:
            quality_config = QualityConfig.from_env()
            self.extractors['quality'] = QualityExtractor(quality_config)
            self.statuses['quality'] = ExtractorStatus('quality')
            logger.info("✓ Quality Extractor initialized")
        except Exception as e:
            logger.error(f"Failed to initialize Quality Extractor: {e}")
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        logger.info(f"Received signal {signum}, initiating graceful shutdown...")
        self.shutdown_event.set()
    
    async def run_extractor(self, extractor_name: str):
        """Run a single extractor"""
        if extractor_name not in self.extractors:
            logger.error(f"Extractor {extractor_name} not found")
            return
        
        status = self.statuses[extractor_name]
        
        if status.is_running:
            logger.warning(f"Extractor {extractor_name} is already running, skipping...")
            return
        
        status.is_running = True
        status.last_run = datetime.now(timezone.utc)
        status.run_count += 1
        
        try:
            logger.info(f"Starting {extractor_name} extraction (run #{status.run_count})")
            extractor = self.extractors[extractor_name]
            
            # Run the extractor
            await extractor.run()
            
            status.last_success = datetime.now(timezone.utc)
            logger.info(f"✓ {extractor_name} extraction completed successfully")
            
        except Exception as e:
            status.error_count += 1
            status.last_error = str(e)
            logger.error(f"✗ {extractor_name} extraction failed: {e}")
            
            # Send alert to CDF events if too many errors
            if status.error_count > 5:
                await self._send_alert(extractor_name, str(e))
        
        finally:
            status.is_running = False
    
    async def _send_alert(self, extractor_name: str, error: str):
        """Send alert event to CDF"""
        try:
            event = {
                'external_id': f"PCN{self.pcn}_ALERT_{extractor_name}_{int(time.time())}",
                'type': 'extractor_error',
                'subtype': 'repeated_failure',
                'metadata': {
                    'extractor': extractor_name,
                    'error': error,
                    'pcn': self.pcn,
                    'error_count': str(self.statuses[extractor_name].error_count)
                }
            }
            self.cognite_client.events.create([event])
            logger.warning(f"Alert sent to CDF for {extractor_name}")
        except Exception as e:
            logger.error(f"Failed to send alert: {e}")
    
    async def schedule_extractor(self, extractor_name: str, interval_seconds: int):
        """Schedule an extractor to run at regular intervals"""
        while not self.shutdown_event.is_set():
            try:
                await self.run_extractor(extractor_name)
                
                # Wait for interval or shutdown
                try:
                    await asyncio.wait_for(
                        self.shutdown_event.wait(),
                        timeout=interval_seconds
                    )
                    break  # Shutdown requested
                except asyncio.TimeoutError:
                    continue  # Continue to next iteration
                    
            except Exception as e:
                logger.error(f"Unexpected error in scheduler for {extractor_name}: {e}")
                await asyncio.sleep(60)  # Wait before retry
    
    async def run_once(self, extractors: Optional[List[str]] = None):
        """Run extractors once (for testing or one-time execution)"""
        if extractors is None:
            extractors = list(self.extractors.keys())
        
        tasks = []
        for name in extractors:
            if name in self.extractors:
                tasks.append(self.run_extractor(name))
        
        await asyncio.gather(*tasks, return_exceptions=True)
    
    async def run_continuous(self):
        """Run all extractors continuously with their schedules"""
        logger.info("Starting continuous extraction mode")
        
        # Create scheduling tasks
        tasks = []
        
        # Master data - daily
        if 'master_data' in self.extractors:
            interval = int(os.getenv('MASTER_EXTRACTION_INTERVAL', 86400))
            tasks.append(self.schedule_extractor('master_data', interval))
        
        # Jobs - every 5 minutes
        if 'jobs' in self.extractors:
            interval = int(os.getenv('JOBS_EXTRACTION_INTERVAL', 300))
            tasks.append(self.schedule_extractor('jobs', interval))
        
        # Production - every 5 minutes
        if 'production' in self.extractors:
            interval = int(os.getenv('PRODUCTION_EXTRACTION_INTERVAL', 300))
            tasks.append(self.schedule_extractor('production', interval))
        
        # Inventory - every 5 minutes
        if 'inventory' in self.extractors:
            interval = int(os.getenv('INVENTORY_EXTRACTION_INTERVAL', 300))
            tasks.append(self.schedule_extractor('inventory', interval))
        
        # Quality - every 5 minutes
        if 'quality' in self.extractors:
            interval = int(os.getenv('QUALITY_EXTRACTION_INTERVAL', 300))
            tasks.append(self.schedule_extractor('quality', interval))
        
        # Start status reporter
        tasks.append(self._status_reporter())
        
        # Run all tasks
        await asyncio.gather(*tasks, return_exceptions=True)
        
        logger.info("All extractors stopped")
    
    async def _status_reporter(self):
        """Periodically report status to logs and CDF"""
        while not self.shutdown_event.is_set():
            try:
                # Log status every 5 minutes
                await asyncio.sleep(300)
                
                logger.info("=== Extractor Status Report ===")
                for name, status in self.statuses.items():
                    logger.info(
                        f"{name}: Runs={status.run_count}, "
                        f"Errors={status.error_count}, "
                        f"Running={status.is_running}, "
                        f"LastRun={status.last_run}"
                    )
                
                # Send heartbeat to CDF
                await self._send_heartbeat()
                
            except Exception as e:
                logger.error(f"Error in status reporter: {e}")
    
    async def _send_heartbeat(self):
        """Send heartbeat event to CDF"""
        try:
            event = {
                'external_id': f"PCN{self.pcn}_HEARTBEAT_{int(time.time())}",
                'type': 'orchestrator_heartbeat',
                'metadata': {
                    'pcn': self.pcn,
                    'extractors_active': str(len(self.extractors)),
                    'total_runs': str(sum(s.run_count for s in self.statuses.values())),
                    'total_errors': str(sum(s.error_count for s in self.statuses.values()))
                }
            }
            self.cognite_client.events.create([event])
        except Exception as e:
            logger.error(f"Failed to send heartbeat: {e}")
    
    def get_status(self) -> Dict[str, Any]:
        """Get current status of all extractors"""
        return {
            name: {
                'last_run': status.last_run.isoformat() if status.last_run else None,
                'last_success': status.last_success.isoformat() if status.last_success else None,
                'last_error': status.last_error,
                'run_count': status.run_count,
                'error_count': status.error_count,
                'is_running': status.is_running
            }
            for name, status in self.statuses.items()
        }


async def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Plex-CDF Extractor Orchestrator')
    parser.add_argument(
        '--mode', 
        choices=['continuous', 'once', 'test'],
        default='continuous',
        help='Execution mode'
    )
    parser.add_argument(
        '--extractors',
        nargs='+',
        choices=['master_data', 'jobs', 'production', 'inventory', 'quality'],
        help='Specific extractors to run (for once/test mode)'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Initialize but don\'t run extractors'
    )
    
    args = parser.parse_args()
    
    # Initialize orchestrator
    orchestrator = ExtractorOrchestrator()
    
    if args.dry_run:
        logger.info("Dry run mode - initialized extractors:")
        for name in orchestrator.extractors.keys():
            logger.info(f"  - {name}")
        return
    
    if args.mode == 'once':
        # Run once and exit
        await orchestrator.run_once(args.extractors)
        
    elif args.mode == 'test':
        # Test mode - run each extractor once
        logger.info("Test mode - running each extractor once")
        await orchestrator.run_once()
        
        # Print status
        status = orchestrator.get_status()
        logger.info("\n=== Test Results ===")
        for name, info in status.items():
            success = info['error_count'] == 0
            symbol = "✓" if success else "✗"
            logger.info(f"{symbol} {name}: {info}")
    
    else:
        # Continuous mode
        await orchestrator.run_continuous()


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Orchestrator stopped by user")
    except Exception as e:
        logger.error(f"Orchestrator failed: {e}")
        sys.exit(1)