#!/usr/bin/env python3
"""
Jobs Extractor for Plex MES - Event-Based Model

This extractor creates Events for ALL jobs (not just active ones):
- Scheduled jobs -> Events with subtype "scheduled"
- In-progress jobs -> Events with subtype "in_progress"  
- Completed jobs -> Events with subtype "completed"
- Cancelled jobs -> Events with subtype "cancelled"

NO RAW TABLES - Uses Events in PLEXSCHEDULING dataset.
"""

import os
import sys
import asyncio
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, Tuple

from dotenv import load_dotenv
from cognite.client.data_classes import Event

from base_extractor import BaseExtractor, BaseExtractorConfig
from multi_facility_config import MultiTenantNamingConvention, FacilityConfig
from id_resolver import AssetIDResolver, EventAssetLinker, get_resolver

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class JobsConfig(BaseExtractorConfig):
    """Configuration for Jobs Extractor - inherits from base"""
    
    @classmethod
    def from_env(cls) -> 'JobsConfig':
        """Load configuration from environment variables"""
        # Use base class method to get common config
        base_config = BaseExtractorConfig.from_env('jobs')
        
        # Jobs extractor uses the base config as-is
        return cls(**base_config.__dict__)


# Create alias for backward compatibility with orchestrator.py
ExtractorConfig = JobsConfig


class PlexJobsExtractor(BaseExtractor):
    """Extract jobs as Events - ALL jobs, not just active ones"""
    
    def __init__(self, config: JobsConfig):
        super().__init__(config, 'jobs')
        
        # Track processed jobs to avoid duplicates
        self.processed_job_events = set()
        
        # Initialize ID resolver for asset linking
        self.id_resolver = get_resolver(self.client)
        self.event_linker = EventAssetLinker(self.id_resolver)
        
        logger.info(f"Jobs Extractor initialized for PCN {config.facility.pcn} with asset ID resolver")
    
    def get_required_datasets(self) -> List[str]:
        """Return required dataset types for jobs"""
        return ['scheduling', 'master']  # Needs PLEXSCHEDULING and PLEXMASTER (for asset links)
    
    async def fetch_all_jobs(self, params: Optional[Dict] = None) -> List[Dict]:
        """Fetch all jobs from Plex"""
        endpoint = "/scheduling/v1/jobs"
        
        if params is None:
            params = {
                'limit': self.config.batch_size,
                'offset': 0
            }
        
        all_jobs = []
        
        while True:
            try:
                data = await self.fetch_plex_data(endpoint, params)
                # Handle both list and dict response formats
                if isinstance(data, list):
                    jobs = data
                elif isinstance(data, dict):
                    jobs = data.get('data', [])
                else:
                    jobs = []
                
                if not jobs:
                    break
                
                all_jobs.extend(jobs)
                logger.info(f"Fetched {len(jobs)} jobs (total: {len(all_jobs)})")
                
                if len(jobs) < self.config.batch_size:
                    break
                
                params['offset'] += self.config.batch_size
                
            except Exception as e:
                logger.error(f"Error fetching jobs: {e}")
                break
        
        return all_jobs
    
    def determine_job_subtype(self, job: Dict) -> str:
        """Determine the event subtype based on job status"""
        status = job.get('status', '').upper()
        
        # Map various status values to our standard subtypes
        if status in ['SCHEDULED', 'PLANNED', 'PENDING', 'NOT_STARTED']:
            return 'scheduled'
        elif status in ['IN_PROGRESS', 'STARTED', 'RUNNING', 'ACTIVE']:
            return 'in_progress'
        elif status in ['COMPLETED', 'FINISHED', 'DONE', 'CLOSED']:
            return 'completed'
        elif status in ['CANCELLED', 'CANCELED', 'ABORTED', 'TERMINATED']:
            return 'cancelled'
        elif status in ['HOLD', 'PAUSED', 'SUSPENDED']:
            return 'on_hold'
        else:
            # Default to scheduled if unknown
            return 'scheduled'
    
    def parse_job_timestamps(self, job: Dict) -> tuple:
        """Parse job timestamps and return (start_time, end_time) in milliseconds"""
        # Try different date fields
        start_time = None
        end_time = None
        
        # For start time, prefer actual over scheduled
        start_date = job.get('actualStartDate') or job.get('scheduledStartDate') or job.get('startDate')
        if start_date:
            try:
                dt = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
                start_time = int(dt.timestamp() * 1000)
            except:
                pass
        
        # For end time, prefer actual over scheduled
        end_date = job.get('actualEndDate') or job.get('scheduledEndDate') or job.get('dueDate') or job.get('endDate')
        if end_date:
            try:
                dt = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
                end_time = int(dt.timestamp() * 1000)
            except:
                pass
        
        # If no timestamps, use current time for scheduled jobs
        if not start_time and not end_time:
            now = int(datetime.now(timezone.utc).timestamp() * 1000)
            start_time = now
        
        return start_time, end_time
    
    def create_job_events(self, jobs: List[Dict]) -> List[Event]:
        """Create Events for ALL jobs, regardless of status"""
        events = []
        
        for job in jobs:
            try:
                # Get job identifier
                job_id = job.get('jobNumber') or job.get('jobId') or job.get('id')
                if not job_id:
                    logger.warning(f"Job missing identifier: {job}")
                    continue
                
                # Create event external ID
                external_id = self.create_event_external_id('job', str(job_id))
                
                # Skip if already processed
                if external_id in self.processed_job_events:
                    continue
                
                # Determine event subtype
                subtype = self.determine_job_subtype(job)
                
                # Parse timestamps
                start_time, end_time = self.parse_job_timestamps(job)
                
                # Build asset links using numeric IDs
                asset_external_ids = []
                
                # Get workcenter and part IDs for metadata and linking
                wc_id = job.get('workcenterId') or job.get('workcenterCode') or ''
                part_id = job.get('partId') or job.get('partNumber') or ''
                
                # Add workcenter asset link if available
                if wc_id:
                    wc_external_id = self.naming.create_external_id('workcenter', str(wc_id))
                    asset_external_ids.append(wc_external_id)
                
                # Add part asset link if available  
                if part_id:
                    part_external_id = self.naming.create_external_id('part', str(part_id))
                    asset_external_ids.append(part_external_id)
                
                # Resolve external IDs to numeric IDs
                asset_ids = self.event_linker.prepare_event_asset_ids(asset_external_ids)
                
                # Create the event
                event = Event(
                    external_id=external_id,
                    type='job',
                    subtype=subtype,
                    start_time=start_time,
                    end_time=end_time,
                    description=f"Job {job_id} - {subtype}",
                    data_set_id=self.get_dataset_id('scheduling'),
                    asset_ids=asset_ids if asset_ids else None,
                    metadata={
                        **self.naming.get_metadata_tags(),
                        'job_id': str(job_id),
                        'job_number': job.get('jobNumber', ''),
                        'part_number': job.get('partNumber', ''),
                        'part_id': str(part_id),
                        'part_description': job.get('partDescription', ''),
                        'quantity_ordered': str(job.get('quantity', 0)),
                        'quantity_completed': str(job.get('quantityCompleted', 0)),
                        'quantity_remaining': str(job.get('quantityRemaining', 0)),
                        'status': job.get('jobStatus', ''),
                        'priority': str(job.get('priority', '')),
                        'customer_order': job.get('customerOrder', ''),
                        'due_date': job.get('dueDate', ''),
                        'workcenter_id': str(wc_id),
                        'operation_number': str(job.get('operationNumber', '')),
                        'last_updated': datetime.now(timezone.utc).isoformat()
                    }
                )
                
                events.append(event)
                self.processed_job_events.add(external_id)
                
            except Exception as e:
                logger.error(f"Error creating event for job {job}: {e}")
        
        return events
    
    async def extract(self):
        """Main extraction logic - implements abstract method from BaseExtractor"""
        logger.info(f"Starting job extraction for PCN {self.config.facility.pcn}")
        
        # Fetch all jobs
        jobs = await self.fetch_all_jobs()
        
        if not jobs:
            logger.info("No jobs found")
            return
        
        logger.info(f"Processing {len(jobs)} jobs")
        
        # Count jobs by status for logging
        status_counts = {}
        for job in jobs:
            subtype = self.determine_job_subtype(job)
            status_counts[subtype] = status_counts.get(subtype, 0) + 1
        
        logger.info(f"Job status breakdown: {status_counts}")
        
        # Create events for ALL jobs
        events = self.create_job_events(jobs)
        
        if events:
            # Use deduplication helper to create events
            result = self.dedup_helper.create_events_batch(events)
            logger.info(f"Job events: {len(result['created'])} created, "
                       f"{len(result['duplicates'])} duplicates skipped")
        else:
            logger.warning("No events created from jobs")
        
        logger.info(f"Job extraction completed for PCN {self.config.facility.pcn}")


async def main():
    """Main entry point"""
    config = JobsConfig.from_env()
    
    # Validate configuration
    required_vars = [
        'PLEX_API_KEY', 'PLEX_CUSTOMER_ID',
        'CDF_HOST', 'CDF_PROJECT', 'CDF_CLIENT_ID',
        'CDF_CLIENT_SECRET', 'CDF_TOKEN_URL'
    ]
    
    missing = [var for var in required_vars if not os.getenv(var)]
    if missing:
        logger.error(f"Missing required environment variables: {', '.join(missing)}")
        sys.exit(1)
    
    logger.info(f"Starting Jobs Extractor for {config.facility.facility_name} (PCN: {config.facility.pcn})")
    
    extractor = PlexJobsExtractor(config)
    
    try:
        await extractor.run()
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())