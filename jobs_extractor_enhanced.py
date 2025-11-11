#!/usr/bin/env python3
"""
Enhanced Jobs Extractor with All Improvements
- Full async/await implementation
- Type hints throughout
- Error handling with retry
- Asset ID resolution
- Structured logging
"""

from __future__ import annotations

import asyncio
import os
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any, Tuple, Final
from dataclasses import dataclass, field

import structlog
from cognite.client.data_classes import Event

from base_extractor_enhanced import (
    BaseExtractor, BaseExtractorConfig, ExtractionResult,
    DatasetType, with_retry
)
from error_handling import PlexAPIError

# Setup structured logging
logger = structlog.get_logger(__name__)


@dataclass
class JobData:
    """Structured job data"""
    id: str
    job_number: str
    status: str
    part_number: Optional[str] = None
    part_name: Optional[str] = None
    workcenter_id: Optional[str] = None
    workcenter_name: Optional[str] = None
    quantity: int = 0
    quantity_completed: int = 0
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    due_date: Optional[datetime] = None
    priority: int = 0
    customer_order: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


class JobsExtractorConfig(BaseExtractorConfig):
    """Configuration specific to jobs extractor"""
    lookback_days: int = 7
    include_completed: bool = True
    
    @classmethod
    def from_env(cls) -> JobsExtractorConfig:
        """Load configuration from environment"""
        base = BaseExtractorConfig.from_env('jobs')
        return cls(
            **base.dict(),
            lookback_days=int(os.getenv('JOBS_LOOKBACK_DAYS', '7')),
            include_completed=os.getenv('JOBS_INCLUDE_COMPLETED', 'true').lower() == 'true'
        )


class EnhancedJobsExtractor(BaseExtractor):
    """Enhanced jobs extractor with all improvements"""
    
    def __init__(self, config: Optional[JobsExtractorConfig] = None):
        """Initialize with enhanced configuration"""
        config = config or JobsExtractorConfig.from_env()
        super().__init__(config, 'jobs')
        
        self.config: Final[JobsExtractorConfig] = config
        self.processed_job_events: set[str] = set()
        
        self.logger.info(
            "jobs_extractor_initialized",
            lookback_days=config.lookback_days,
            include_completed=config.include_completed
        )
    
    def get_required_datasets(self) -> List[str]:
        """Jobs require scheduling and master datasets"""
        return ['scheduling', 'master']
    
    async def extract(self) -> ExtractionResult:
        """Main extraction with concurrent operations"""
        start_time = datetime.now(timezone.utc)
        result = ExtractionResult(
            success=True,
            items_processed=0,
            duration_ms=0
        )
        
        try:
            # Fetch jobs concurrently by status
            async with asyncio.TaskGroup() as tg:
                scheduled_task = tg.create_task(self._fetch_jobs_by_status('scheduled'))
                active_task = tg.create_task(self._fetch_jobs_by_status('active'))
                completed_task = tg.create_task(self._fetch_jobs_by_status('completed'))
            
            # Combine results (tasks are already awaited by TaskGroup)
            all_jobs: List[JobData] = []
            all_jobs.extend(scheduled_task.result())
            all_jobs.extend(active_task.result())
            
            if self.config.include_completed:
                all_jobs.extend(completed_task.result())
            
            self.logger.info(
                "jobs_fetched",
                total=len(all_jobs),
                scheduled=len(await scheduled_task),
                active=len(await active_task),
                completed=len(await completed_task) if self.config.include_completed else 0
            )
            
            # Convert to events
            events = await self._convert_jobs_to_events(all_jobs)
            
            # Create events in CDF with asset linking
            created, duplicates = await self.create_events_with_retry(
                events,
                link_assets=True
            )
            
            result.items_processed = len(created)
            result.metadata = {
                'created': len(created),
                'duplicates': len(duplicates),
                'total_jobs': len(all_jobs)
            }
            
            # Update processed set
            self.processed_job_events.update(created)
            
        except Exception as e:
            result.success = False
            result.errors.append(str(e))
            self.logger.error("jobs_extraction_failed", error=str(e), exc_info=True)
        
        result.duration_ms = (datetime.now(timezone.utc) - start_time).total_seconds() * 1000
        return result
    
    @with_retry(max_attempts=3)
    async def _fetch_jobs_by_status(self, status: str) -> List[JobData]:
        """Fetch jobs by status with retry"""
        endpoint = "/scheduling/v1/jobs"
        
        # Calculate date range
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=self.config.lookback_days)
        
        params = {
            'status': status,
            'dateFrom': start_date.isoformat(),
            'dateTo': end_date.isoformat(),
            'limit': 1000
        }
        
        all_jobs: List[JobData] = []
        offset = 0
        
        while True:
            params['offset'] = offset
            
            try:
                data = await self.fetch_plex_data(endpoint, params)
                
                if not data:
                    break
                
                # Parse jobs
                jobs = self._parse_jobs(data)
                all_jobs.extend(jobs)
                
                # Check for more data
                if len(jobs) < 1000:
                    break
                
                offset += len(jobs)
                
            except PlexAPIError as e:
                self.logger.error(
                    "fetch_jobs_error",
                    status=status,
                    offset=offset,
                    error=str(e)
                )
                raise
        
        self.logger.debug(
            "jobs_fetched_by_status",
            status=status,
            count=len(all_jobs)
        )
        
        return all_jobs
    
    def _parse_priority(self, priority_value: Any) -> int:
        """Parse priority value which might be string or int"""
        if priority_value is None:
            return 0
        
        # If already an int, return it
        if isinstance(priority_value, int):
            return priority_value
            
        # If string, try to parse or map
        if isinstance(priority_value, str):
            priority_value = priority_value.lower().strip()
            # Map common priority strings
            priority_map = {
                'low': 1,
                'medium': 2,
                'normal': 2,
                'high': 3,
                'critical': 4,
                'urgent': 4
            }
            if priority_value in priority_map:
                return priority_map[priority_value]
            
            # Try to parse as number
            try:
                return int(priority_value)
            except (ValueError, TypeError):
                return 0
        
        return 0
    
    def _parse_jobs(self, data: Union[List, Dict]) -> List[JobData]:
        """Parse job data from API response"""
        # Handle different response formats
        if isinstance(data, list):
            jobs_raw = data
        elif isinstance(data, dict) and 'data' in data:
            jobs_raw = data['data']
        else:
            jobs_raw = []
        
        jobs: List[JobData] = []
        
        for job_raw in jobs_raw:
            try:
                job = self._parse_single_job(job_raw)
                if job:
                    jobs.append(job)
            except Exception as e:
                # Try to get a meaningful identifier for logging
                job_identifier = (
                    job_raw.get('jobNo') or 
                    job_raw.get('jobNumber') or 
                    job_raw.get('job_no') or
                    job_raw.get('id', 'unknown')
                )
                self.logger.warning(
                    "job_parse_error",
                    job_id=job_identifier,
                    error=str(e)
                )
        
        return jobs
    
    def _parse_single_job(self, job_raw: Dict[str, Any]) -> Optional[JobData]:
        """Parse a single job with validation"""
        # Get job number FIRST (this is what we want as the ID)
        job_number = (
            job_raw.get('jobNo') or 
            job_raw.get('jobNumber') or 
            job_raw.get('job_no') or
            job_raw.get('job_number')
        )
        
        # If no job number, try to get from ID field
        if not job_number:
            job_id = (
                job_raw.get('id') or 
                job_raw.get('jobId') or 
                job_raw.get('job_id')
            )
            if not job_id:
                return None
            # Use the UUID as fallback
            job_number = str(job_id)[:10]
        
        # Use job number as the primary ID
        job_id = job_number
        
        # Parse dates
        def parse_date(date_str: Optional[str]) -> Optional[datetime]:
            if not date_str:
                return None
            try:
                return datetime.fromisoformat(date_str.replace('Z', '+00:00'))
            except:
                return None
        
        return JobData(
            id=str(job_id),
            job_number=str(job_number),
            status=job_raw.get('status', 'unknown').lower(),
            part_number=job_raw.get('partNumber') or job_raw.get('partNo'),
            part_name=job_raw.get('partName') or job_raw.get('part_name'),
            workcenter_id=job_raw.get('workcenterId') or job_raw.get('workcenterCode'),
            workcenter_name=job_raw.get('workcenterName'),
            quantity=int(job_raw.get('quantity')) if job_raw.get('quantity') is not None else 0,
            quantity_completed=int(job_raw.get('quantityCompleted')) if job_raw.get('quantityCompleted') is not None else 0,
            start_date=parse_date(
                job_raw.get('startDate') or 
                job_raw.get('scheduledStartDate') or
                job_raw.get('actualStartDate')
            ),
            end_date=parse_date(
                job_raw.get('endDate') or
                job_raw.get('scheduledEndDate') or
                job_raw.get('actualEndDate')
            ),
            due_date=parse_date(job_raw.get('dueDate')),
            # Priority might be a string like "Medium", "High", "Low"
            priority=self._parse_priority(job_raw.get('priority')),
            customer_order=job_raw.get('customerOrder'),
            metadata=job_raw  # Keep original for reference
        )
    
    async def _convert_jobs_to_events(self, jobs: List[JobData]) -> List[Event]:
        """Convert jobs to CDF events with proper linking"""
        events: List[Event] = []
        
        for job in jobs:
            try:
                event = await self._create_job_event(job)
                if event:
                    events.append(event)
            except Exception as e:
                self.logger.warning(
                    "event_creation_error",
                    job_id=job.id,
                    error=str(e)
                )
        
        return events
    
    async def _create_job_event(self, job: JobData) -> Optional[Event]:
        """Create a CDF event from job data"""
        # Create external ID
        external_id = self.create_event_external_id('job', job.id)
        
        # Skip if already processed
        if external_id in self.processed_job_events:
            return None
        
        # Determine subtype based on status
        subtype = self._determine_job_subtype(job.status)
        
        # Build human-readable description
        description = self._build_job_description(job)
        
        # Prepare timestamps
        start_time = int(job.start_date.timestamp() * 1000) if job.start_date else None
        end_time = int(job.end_date.timestamp() * 1000) if job.end_date else None
        
        if not start_time:
            start_time = int(datetime.now(timezone.utc).timestamp() * 1000)
        
        # Ensure end_time has a value for CDF Event
        if not end_time:
            # If no end time, set it to start_time + 1 hour or current time
            if start_time:
                end_time = start_time + (3600 * 1000)  # Add 1 hour in milliseconds
            else:
                end_time = int(datetime.now(timezone.utc).timestamp() * 1000)
        
        # Build metadata
        metadata = self._build_job_metadata(job)
        
        # Prepare asset links (will be resolved to numeric IDs)
        asset_external_ids: List[str] = []
        
        if job.workcenter_id:
            wc_external_id = self.create_asset_external_id('workcenter', job.workcenter_id)
            asset_external_ids.append(wc_external_id)
        
        if job.part_number:
            part_external_id = self.create_asset_external_id('part', job.part_number)
            asset_external_ids.append(part_external_id)
        
        # Create event
        event = Event(
            external_id=external_id,
            type='production_job',
            subtype=subtype,
            description=description,
            start_time=start_time,
            end_time=end_time,
            metadata=metadata,
            data_set_id=self.get_dataset_id('scheduling')
        )
        
        # Add asset external IDs for resolution
        if asset_external_ids:
            event.asset_external_ids = asset_external_ids  # Will be resolved in create_events_with_retry
        
        return event
    
    def _determine_job_subtype(self, status: str) -> str:
        """Determine event subtype from job status"""
        status_lower = status.lower()
        
        if 'complete' in status_lower or 'finish' in status_lower:
            return 'completed'
        elif 'progress' in status_lower or 'active' in status_lower or 'running' in status_lower:
            return 'in_progress'
        elif 'cancel' in status_lower:
            return 'cancelled'
        else:
            return 'scheduled'
    
    def _build_job_description(self, job: JobData) -> str:
        """Build human-readable job description"""
        parts = [f"Job #{job.job_number}"]
        
        if job.part_name and job.part_number:
            parts.append(f"{job.part_name} ({job.part_number})")
        elif job.part_name:
            parts.append(job.part_name)
        elif job.part_number:
            parts.append(f"Part {job.part_number}")
        
        if job.quantity:
            parts.append(f"Qty: {job.quantity:,}")
        
        if job.workcenter_name:
            parts.append(f"WC: {job.workcenter_name}")
        elif job.workcenter_id:
            parts.append(f"WC: {job.workcenter_id}")
        
        # Add status
        status_display = {
            'scheduled': '[Scheduled]',
            'in_progress': '[In Progress]',
            'completed': '[Completed]',
            'cancelled': '[Cancelled]'
        }.get(self._determine_job_subtype(job.status), f'[{job.status.title()}]')
        
        parts.append(status_display)
        
        return " | ".join(parts)
    
    def _build_job_metadata(self, job: JobData) -> Dict[str, str]:
        """Build job metadata"""
        metadata = {
            **self.naming.get_metadata_tags(),
            'job_id': job.id,
            'job_number': job.job_number,
            'status': job.status,
            'source': 'plex_jobs'
        }
        
        # Add optional fields
        if job.part_number:
            metadata['part_number'] = job.part_number
        if job.part_name:
            metadata['part_name'] = job.part_name
        if job.workcenter_id:
            metadata['workcenter_id'] = job.workcenter_id
        if job.workcenter_name:
            metadata['workcenter_name'] = job.workcenter_name
        if job.quantity:
            metadata['quantity'] = str(job.quantity)
            metadata['quantity_completed'] = str(job.quantity_completed)
        if job.priority:
            metadata['priority'] = str(job.priority)
        if job.customer_order:
            metadata['customer_order'] = job.customer_order
        if job.due_date:
            metadata['due_date'] = job.due_date.isoformat()
        
        return metadata


async def main():
    """Main entry point for standalone execution"""
    import os
    
    # Setup logging for console
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.dev.ConsoleRenderer()
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )
    
    try:
        extractor = EnhancedJobsExtractor()
        
        # Run once or continuously
        if os.getenv('RUN_CONTINUOUS', 'false').lower() == 'true':
            while True:
                await extractor.run_extraction_cycle()
                await asyncio.sleep(extractor.config.extraction_interval)
        else:
            await extractor.run_extraction_cycle()
            
    except KeyboardInterrupt:
        logger.info("Extraction stopped by user")
    except Exception as e:
        logger.error("Fatal error", error=str(e), exc_info=True)
        raise
    finally:
        await extractor.cleanup()


if __name__ == "__main__":
    import os
    asyncio.run(main())