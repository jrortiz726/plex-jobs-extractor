#!/usr/bin/env python3
"""
Standalone Jobs Extractor for Plex to CDF
Extracts production jobs as events in Cognite Data Fusion
Designed to run as a Cognite Function with built-in CDF logging
"""

import asyncio
import aiohttp
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any
from cognite.client import CogniteClient, ClientConfig
from cognite.client.credentials import OAuthClientCredentials
from cognite.client.data_classes import Asset, AssetList, Event, EventList

# For CDF Functions, use print statements which are captured in function logs
# CDF Functions automatically capture stdout/stderr
def log_info(message: str):
    """Log info message for CDF Functions"""
    print(f"INFO: {message}")

def log_error(message: str):
    """Log error message for CDF Functions"""
    print(f"ERROR: {message}")

def log_warning(message: str):
    """Log warning message for CDF Functions"""
    print(f"WARNING: {message}")


class StandaloneJobsExtractor:
    """Standalone extractor for Plex jobs data to CDF events"""
    
    def __init__(self):
        """Initialize the standalone extractor with all necessary configurations"""
        # ============= HARDCODED CONFIGURATION =============
        
        # Plex API Configuration
        self.api_key = 'fGGv6CAq8l3a1jwJ9uA6ltkvzGSBlolT'
        self.customer_id = '340884'
        self.facility_name = 'RADEMO'
        
        # CDF Configuration
        cdf_host = 'https://westeurope-1.cognitedata.com'
        cdf_project = 'essc-sandbox-44'
        client_id = 'M9ydu9eWxSCbmWv3dJmSXnIe3cXIynF6'
        client_secret = 'SbT59mqDdPCnKuIrhfXlTfa1axx-kX9ED5iVkBhAvv84adEHqkkW5Y90Sf2Gdt7_'
        token_url = 'https://datamosaix-prod.us.auth0.com/oauth/token'
        
        # Dataset configuration
        self.dataset_id = 7195113081024241  # CDF_DATASET_PLEXSCHEDULING
        
        # Extraction configuration
        self.batch_size = 1000
        self.extraction_interval = 300  # 5 minutes
        self.extraction_days = 7  # How many days back to fetch
        
        # ============= END HARDCODED CONFIGURATION =============
        
        # Initialize CDF client
        credentials = OAuthClientCredentials(
            token_url=token_url,
            client_id=client_id,
            client_secret=client_secret,
            scopes=[f"{cdf_host}/.default"]
        )
        
        self.client = CogniteClient(
            ClientConfig(
                client_name="StandaloneJobsExtractor",
                base_url=cdf_host,
                project=cdf_project,
                credentials=credentials
            )
        )
        
        # PCN prefix for multi-tenancy
        self.pcn_prefix = f"PCN{self.customer_id}"
        
        log_info(f"Initialized Jobs Extractor for PCN {self.customer_id}")
    
    async def fetch_jobs(self, session: aiohttp.ClientSession) -> List[Dict]:
        """Fetch jobs from Plex API"""
        url = "https://connect.plex.com/scheduling/v1/jobs"
        headers = {
            'X-Plex-Connect-Api-Key': self.api_key,
            'X-Plex-Connect-Customer-Id': self.customer_id,
            'Content-Type': 'application/json'
        }
        
        # Calculate date range
        date_to = datetime.now(timezone.utc)
        date_from = date_to - timedelta(days=self.extraction_days)
        
        params = {
            'dateFrom': date_from.isoformat(),
            'dateTo': date_to.isoformat(),
            'limit': 1000
        }
        
        all_jobs = []
        offset = 0
        
        try:
            while True:
                params['offset'] = offset
                log_info(f"Fetching jobs with offset {offset}")
                
                async with session.get(url, headers=headers, params=params) as response:
                    if response.status != 200:
                        error_text = await response.text()
                        log_error(f"Failed to fetch jobs: {response.status} - {error_text}")
                        break
                    
                    data = await response.json()
                    
                    # Handle both list and dict responses
                    if isinstance(data, list):
                        jobs = data
                    elif isinstance(data, dict):
                        jobs = data.get('data', [])
                    else:
                        jobs = []
                    
                    if not jobs:
                        break
                    
                    all_jobs.extend(jobs)
                    log_info(f"Fetched {len(jobs)} jobs")
                    
                    # Check if more data available
                    if len(jobs) < 1000:
                        break
                    
                    offset += len(jobs)
                    
        except Exception as e:
            log_error(f"Error fetching jobs: {e}")
        
        return all_jobs
    
    def create_job_event(self, job: Dict) -> Optional[Event]:
        """Convert Plex job to CDF event"""
        try:
            # Get job ID for external ID (the unique identifier)
            job_id = job.get('id') or job.get('jobId')
            if not job_id:
                log_warning(f"Job missing ID: {job}")
                return None
            
            # Get human-readable job number for description
            job_number = job.get('jobNo') or job.get('jobNumber') or job.get('job_no') or str(job_id)[:10]
            
            # Get part information
            part_number = job.get('partNumber') or job.get('partNo') or ''
            part_name = job.get('partName') or job.get('part_name') or 'Unknown Part'
            
            # Get workcenter information
            workcenter = job.get('workcenterCode') or job.get('workcenterName') or job.get('workcenterId') or ''
            
            # Get quantity and make it readable
            quantity = job.get('quantity') or job.get('qty') or 0
            quantity_str = f"{quantity:,}" if quantity else "0"
            
            # Determine job status and subtype
            status = job.get('status', '').lower()
            if 'complete' in status or 'finish' in status:
                subtype = 'completed'
                status_display = 'Completed'
            elif 'progress' in status or 'active' in status or 'running' in status:
                subtype = 'in_progress'
                status_display = 'In Progress'
            else:
                subtype = 'scheduled'
                status_display = 'Scheduled'
            
            # Build external ID with PCN prefix (using the unique ID)
            external_id = f"{self.pcn_prefix}_JOB_{job_id}"
            
            # Build human-readable description
            description_parts = [f"Job #{job_number}"]
            
            if part_number and part_name:
                description_parts.append(f"{part_name} ({part_number})")
            elif part_name:
                description_parts.append(part_name)
            elif part_number:
                description_parts.append(f"Part {part_number}")
            
            if quantity:
                description_parts.append(f"Qty: {quantity_str}")
            
            if workcenter:
                description_parts.append(f"WC: {workcenter}")
            
            description_parts.append(f"[{status_display}]")
            
            description = " | ".join(description_parts)
            
            # Get timestamps
            start_time = None
            end_time = None
            
            # Try different date fields
            for start_field in ['startDate', 'scheduledStartDate', 'actualStartDate', 'start_date']:
                if start_field in job and job[start_field]:
                    try:
                        start_time = int(datetime.fromisoformat(
                            job[start_field].replace('Z', '+00:00')
                        ).timestamp() * 1000)
                        break
                    except:
                        pass
            
            for end_field in ['endDate', 'scheduledEndDate', 'actualEndDate', 'end_date', 'completedDate']:
                if end_field in job and job[end_field]:
                    try:
                        end_time = int(datetime.fromisoformat(
                            job[end_field].replace('Z', '+00:00')
                        ).timestamp() * 1000)
                        break
                    except:
                        pass
            
            # Default to now if no start time
            if not start_time:
                start_time = int(datetime.now(timezone.utc).timestamp() * 1000)
            
            # Build metadata with both IDs
            metadata = {
                'pcn': self.customer_id,
                'facility': self.facility_name,
                'source': 'plex_jobs',
                'job_id': str(job_id),  # Keep the unique ID in metadata
                'job_number': str(job_number),  # Add human-readable job number
                'status': status_display
            }
            
            # Add optional metadata fields
            if part_number:
                metadata['part_number'] = str(part_number)
            if part_name:
                metadata['part_name'] = str(part_name)
            if workcenter:
                metadata['workcenter'] = str(workcenter)
            if quantity:
                metadata['quantity'] = quantity_str
                metadata['quantity_raw'] = str(quantity)
            if job.get('priority'):
                metadata['priority'] = str(job.get('priority'))
            if job.get('customer'):
                metadata['customer'] = str(job.get('customer'))
            if job.get('orderNumber'):
                metadata['order_number'] = str(job.get('orderNumber'))
            
            # Create event
            event = Event(
                external_id=external_id,
                type='production_job',
                subtype=subtype,
                description=description,
                start_time=start_time,
                end_time=end_time,
                metadata=metadata,
                data_set_id=self.dataset_id if self.dataset_id > 0 else None
            )
            
            return event
            
        except Exception as e:
            log_error(f"Error creating event for job {job}: {e}")
            return None
    
    def create_events_batch(self, events: List[Event]) -> Dict[str, List]:
        """Create events in CDF with deduplication"""
        result = {
            'created': [],
            'duplicates': [],
            'failed': []
        }
        
        if not events:
            return result
        
        # Get existing events by external ID
        external_ids = [e.external_id for e in events]
        
        try:
            # Check for existing events
            existing = self.client.events.retrieve_multiple(
                external_ids=external_ids,
                ignore_unknown_ids=True
            )
            existing_ids = {e.external_id for e in existing} if existing else set()
            
            # Filter out duplicates
            new_events = [e for e in events if e.external_id not in existing_ids]
            duplicate_events = [e for e in events if e.external_id in existing_ids]
            
            result['duplicates'] = [e.external_id for e in duplicate_events]
            
            if new_events:
                # Create new events in batches
                for i in range(0, len(new_events), self.batch_size):
                    batch = new_events[i:i + self.batch_size]
                    try:
                        created = self.client.events.create(batch)
                        if isinstance(created, EventList):
                            result['created'].extend([e.external_id for e in created])
                        elif isinstance(created, Event):
                            result['created'].append(created.external_id)
                        log_info(f"Created {len(batch)} events")
                    except Exception as e:
                        log_error(f"Failed to create batch: {e}")
                        result['failed'].extend([e.external_id for e in batch])
            
            if result['duplicates']:
                log_info(f"Skipped {len(result['duplicates'])} duplicate events")
                
        except Exception as e:
            log_error(f"Error in create_events_batch: {e}")
            result['failed'] = [e.external_id for e in events]
        
        return result
    
    async def extract_once(self):
        """Run a single extraction cycle"""
        log_info("Starting jobs extraction cycle")
        
        async with aiohttp.ClientSession() as session:
            # Fetch jobs from Plex
            jobs = await self.fetch_jobs(session)
            log_info(f"Fetched {len(jobs)} jobs from Plex")
            
            if not jobs:
                log_info("No jobs to process")
                return
            
            # Convert to events
            events = []
            for job in jobs:
                event = self.create_job_event(job)
                if event:
                    events.append(event)
            
            log_info(f"Created {len(events)} events from jobs")
            
            # Create events in CDF
            if events:
                result = self.create_events_batch(events)
                log_info(f"Created: {len(result['created'])}, "
                          f"Duplicates: {len(result['duplicates'])}, "
                          f"Failed: {len(result['failed'])}")
    
    async def run_continuous(self):
        """Run continuous extraction with configured interval"""
        log_info(f"Starting continuous extraction with {self.extraction_interval}s interval")
        
        while True:
            try:
                await self.extract_once()
            except Exception as e:
                log_error(f"Error in extraction cycle: {e}")
            
            log_info(f"Waiting {self.extraction_interval} seconds until next extraction")
            await asyncio.sleep(self.extraction_interval)
    
    def run(self, continuous: bool = False):
        """Main entry point for the extractor"""
        if continuous:
            asyncio.run(self.run_continuous())
        else:
            asyncio.run(self.extract_once())


def handle(data: dict = None, client: CogniteClient = None) -> Dict[str, Any]:
    """
    CDF Function handler for scheduled execution
    
    Args:
        data: Optional configuration data passed to the function
        client: CogniteClient instance (provided by CDF Functions runtime)
    
    Returns:
        Dict with extraction results
    """
    log_info("CDF Function handler invoked")
    
    try:
        # Create and run extractor
        extractor = StandaloneJobsExtractor()
        
        # If client is provided by CDF runtime, use it instead
        if client:
            extractor.client = client
            log_info("Using CDF-provided client")
        
        # Run single extraction
        asyncio.run(extractor.extract_once())
        
        return {
            "status": "success",
            "message": "Jobs extraction completed",
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
    except Exception as e:
        log_error(f"Function execution failed: {e}")
        return {
            "status": "error",
            "error": str(e),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }


def main():
    """Main function for local/standalone execution"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Standalone Plex Jobs Extractor')
    parser.add_argument(
        '--continuous',
        action='store_true',
        help='Run continuously with configured interval'
    )
    parser.add_argument(
        '--interval',
        type=int,
        help='Override extraction interval in seconds (default: 300)'
    )
    args = parser.parse_args()
    
    try:
        extractor = StandaloneJobsExtractor()
        
        # Override interval if provided
        if args.interval:
            extractor.extraction_interval = args.interval
            log_info(f"Overriding interval to {args.interval} seconds")
        
        extractor.run(continuous=args.continuous)
    except KeyboardInterrupt:
        log_info("Extraction stopped by user")
    except Exception as e:
        log_error(f"Fatal error: {e}")
        raise


if __name__ == "__main__":
    main()