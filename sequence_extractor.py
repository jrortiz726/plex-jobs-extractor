#!/usr/bin/env python3
"""
Sequence Extractor for Plex MES - Job Routing Tracking

This extractor creates Sequences to track job progress through operations:
- Job routing sequences showing planned operations
- Production log sequences tracking actual production events
- Quality inspection sequences for job quality data

NO RAW TABLES - Uses Sequences in PLEXSCHEDULING and PLEXPRODUCTION datasets.
"""

import os
import sys
import asyncio
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any

from dotenv import load_dotenv
from cognite.client.data_classes import Sequence, SequenceData, SequenceRow

from base_extractor import BaseExtractor, BaseExtractorConfig
from multi_facility_config import MultiTenantNamingConvention, FacilityConfig

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SequenceConfig(BaseExtractorConfig):
    """Configuration for Sequence Extractor"""
    
    @classmethod
    def from_env(cls) -> 'SequenceConfig':
        """Load configuration from environment variables"""
        base_config = BaseExtractorConfig.from_env('sequence')
        return cls(**base_config.__dict__)


class SequenceExtractor(BaseExtractor):
    """Extract job routing and production logs as Sequences"""
    
    def __init__(self, config: SequenceConfig):
        super().__init__(config, 'sequence')
        
        # Track processed sequences
        self.processed_sequences = set()
        
        logger.info(f"Sequence Extractor initialized for PCN {config.facility.pcn}")
    
    def get_required_datasets(self) -> List[str]:
        """Return required dataset types for sequences"""
        return ['scheduling', 'production']  # Needs both datasets
    
    async def fetch_part_operations(self, part_id: str) -> List[Dict]:
        """Fetch part operations (routing) from Plex"""
        endpoint = "/mdm/v1/part-operations"
        params = {'partId': part_id}
        
        try:
            data = await self.fetch_plex_data(endpoint, params)
            return data.get('data', [])
        except Exception as e:
            logger.error(f"Error fetching operations for part {part_id}: {e}")
            return []
    
    async def fetch_job_operations(self, job_id: str) -> List[Dict]:
        """Fetch job-specific operation status"""
        # This would typically come from a job operations endpoint
        # For now, we'll combine part operations with job production data
        endpoint = f"/production/v1/scheduling/jobs/{job_id}/operations"
        
        try:
            data = await self.fetch_plex_data(endpoint)
            return data.get('data', [])
        except:
            # If job-specific operations not available, fall back to part operations
            return []
    
    async def fetch_production_log(self, job_id: str) -> List[Dict]:
        """Fetch production log entries for a job"""
        endpoint = "/production/v1/production-history/production-entries"
        params = {
            'jobId': job_id,
            'limit': 1000,
            'sort': 'timestamp'
        }
        
        try:
            data = await self.fetch_plex_data(endpoint, params)
            return data.get('data', [])
        except Exception as e:
            logger.error(f"Error fetching production log for job {job_id}: {e}")
            return []
    
    def create_routing_sequence(self, job: Dict, operations: List[Dict]) -> Optional[Sequence]:
        """Create a sequence for job routing through operations"""
        job_id = job.get('jobId') or job.get('jobNumber') or job.get('id')
        if not job_id or not operations:
            return None
        
        external_id = self.create_sequence_external_id('routing', str(job_id))
        
        if external_id in self.processed_sequences:
            return None
        
        # Define columns for routing sequence
        columns = [
            {'externalId': 'operation_number', 'valueType': 'LONG'},
            {'externalId': 'operation_code', 'valueType': 'STRING'},
            {'externalId': 'operation_description', 'valueType': 'STRING'},
            {'externalId': 'workcenter_id', 'valueType': 'STRING'},
            {'externalId': 'setup_time_minutes', 'valueType': 'DOUBLE'},
            {'externalId': 'cycle_time_seconds', 'valueType': 'DOUBLE'},
            {'externalId': 'status', 'valueType': 'STRING'},
            {'externalId': 'quantity_complete', 'valueType': 'LONG'},
            {'externalId': 'quantity_remaining', 'valueType': 'LONG'},
            {'externalId': 'actual_start', 'valueType': 'LONG'},
            {'externalId': 'actual_end', 'valueType': 'LONG'}
        ]
        
        sequence = Sequence(
            external_id=external_id,
            name=f"Job {job_id} Routing",
            description=f"Routing sequence for job {job_id} - Part {job.get('partNumber', '')}",
            columns=columns,
            data_set_id=self.get_dataset_id('scheduling'),
            metadata={
                **self.naming.get_metadata_tags(),
                'job_id': str(job_id),
                'part_number': job.get('partNumber', ''),
                'sequence_type': 'job_routing',
                'last_updated': datetime.now(timezone.utc).isoformat()
            }
        )
        
        self.processed_sequences.add(external_id)
        return sequence
    
    def create_routing_rows(self, job: Dict, operations: List[Dict], 
                           job_operations: List[Dict] = None) -> List[SequenceRow]:
        """Create sequence rows for routing operations"""
        rows = []
        job_id = job.get('jobId') or job.get('jobNumber') or job.get('id')
        
        # Create a map of job-specific operation status if available
        job_op_status = {}
        if job_operations:
            for op in job_operations:
                op_num = op.get('operationNumber')
                if op_num:
                    job_op_status[op_num] = op
        
        for op in operations:
            op_num = op.get('operationNumber') or op.get('sequenceNumber')
            if not op_num:
                continue
            
            # Get job-specific status if available
            job_op = job_op_status.get(op_num, {})
            
            # Determine operation status
            status = 'pending'
            if job_op:
                if job_op.get('quantityComplete', 0) >= job_op.get('quantityRequired', 0):
                    status = 'complete'
                elif job_op.get('quantityComplete', 0) > 0:
                    status = 'in_progress'
            
            # Parse timestamps
            actual_start = None
            actual_end = None
            if job_op.get('actualStartDate'):
                try:
                    dt = datetime.fromisoformat(job_op['actualStartDate'].replace('Z', '+00:00'))
                    actual_start = int(dt.timestamp() * 1000)
                except:
                    pass
            
            if job_op.get('actualEndDate'):
                try:
                    dt = datetime.fromisoformat(job_op['actualEndDate'].replace('Z', '+00:00'))
                    actual_end = int(dt.timestamp() * 1000)
                except:
                    pass
            
            row = SequenceRow(
                rowNumber=op_num,
                values=[
                    op_num,  # operation_number
                    op.get('operationCode', ''),  # operation_code
                    op.get('description', ''),  # operation_description
                    str(op.get('workcenterId', '')),  # workcenter_id
                    float(op.get('setupTime', 0)),  # setup_time_minutes
                    float(op.get('cycleTime', 0)),  # cycle_time_seconds
                    status,  # status
                    job_op.get('quantityComplete', 0),  # quantity_complete
                    job_op.get('quantityRemaining', job.get('quantityOrdered', 0)),  # quantity_remaining
                    actual_start,  # actual_start
                    actual_end  # actual_end
                ]
            )
            rows.append(row)
        
        return rows
    
    def create_production_log_sequence(self, job_id: str, entries: List[Dict]) -> Optional[Sequence]:
        """Create a sequence for production log entries"""
        if not entries:
            return None
        
        external_id = self.create_sequence_external_id('prodlog', str(job_id))
        
        if external_id in self.processed_sequences:
            return None
        
        # Define columns for production log
        columns = [
            {'externalId': 'timestamp', 'valueType': 'LONG'},
            {'externalId': 'event_type', 'valueType': 'STRING'},
            {'externalId': 'quantity', 'valueType': 'LONG'},
            {'externalId': 'scrap_quantity', 'valueType': 'LONG'},
            {'externalId': 'operator', 'valueType': 'STRING'},
            {'externalId': 'workcenter_id', 'valueType': 'STRING'},
            {'externalId': 'operation_number', 'valueType': 'LONG'},
            {'externalId': 'reason_code', 'valueType': 'STRING'},
            {'externalId': 'notes', 'valueType': 'STRING'}
        ]
        
        sequence = Sequence(
            external_id=external_id,
            name=f"Job {job_id} Production Log",
            description=f"Production log entries for job {job_id}",
            columns=columns,
            data_set_id=self.get_dataset_id('production'),
            metadata={
                **self.naming.get_metadata_tags(),
                'job_id': str(job_id),
                'sequence_type': 'production_log',
                'last_updated': datetime.now(timezone.utc).isoformat()
            }
        )
        
        self.processed_sequences.add(external_id)
        return sequence
    
    def create_production_log_rows(self, entries: List[Dict]) -> List[SequenceRow]:
        """Create sequence rows for production log entries"""
        rows = []
        
        for i, entry in enumerate(entries):
            # Parse timestamp
            timestamp = None
            if entry.get('timestamp') or entry.get('createdAt'):
                try:
                    dt_str = entry.get('timestamp') or entry.get('createdAt')
                    dt = datetime.fromisoformat(dt_str.replace('Z', '+00:00'))
                    timestamp = int(dt.timestamp() * 1000)
                except:
                    pass
            
            # Determine event type
            event_type = 'produce'
            if entry.get('scrapQuantity', 0) > 0:
                event_type = 'scrap'
            elif entry.get('eventType'):
                event_type = entry['eventType'].lower()
            
            row = SequenceRow(
                rowNumber=i,
                values=[
                    timestamp,  # timestamp
                    event_type,  # event_type
                    entry.get('quantity', 0),  # quantity
                    entry.get('scrapQuantity', 0),  # scrap_quantity
                    entry.get('operator', ''),  # operator
                    str(entry.get('workcenterId', '')),  # workcenter_id
                    entry.get('operationNumber', 0),  # operation_number
                    entry.get('reasonCode', ''),  # reason_code
                    entry.get('notes', '')  # notes
                ]
            )
            rows.append(row)
        
        return rows
    
    async def extract(self):
        """Main extraction logic"""
        logger.info(f"Starting sequence extraction for PCN {self.config.facility.pcn}")
        
        # First fetch active jobs
        jobs_endpoint = "/production/v1/scheduling/jobs"
        jobs_params = {
            'status': 'IN_PROGRESS',  # Focus on active jobs for sequences
            'limit': 100
        }
        
        jobs_data = await self.fetch_plex_data(jobs_endpoint, jobs_params)
        jobs = jobs_data.get('data', [])
        
        if not jobs:
            logger.info("No active jobs found for sequence extraction")
            return
        
        logger.info(f"Processing sequences for {len(jobs)} active jobs")
        
        sequences_to_create = []
        sequence_data_to_insert = []
        
        for job in jobs:
            job_id = job.get('jobId') or job.get('jobNumber') or job.get('id')
            part_id = job.get('partId') or job.get('partNumber')
            
            if not job_id:
                continue
            
            # Fetch routing operations for the part
            if part_id:
                operations = await self.fetch_part_operations(part_id)
                if operations:
                    # Try to get job-specific operation status
                    job_operations = await self.fetch_job_operations(job_id)
                    
                    # Create routing sequence
                    sequence = self.create_routing_sequence(job, operations)
                    if sequence:
                        sequences_to_create.append(sequence)
                        
                        # Create rows for the sequence
                        rows = self.create_routing_rows(job, operations, job_operations)
                        if rows:
                            external_id = self.create_sequence_external_id('routing', str(job_id))
                            sequence_data_to_insert.append({
                                'external_id': external_id,
                                'rows': rows
                            })
            
            # Fetch and create production log sequence
            production_entries = await self.fetch_production_log(job_id)
            if production_entries:
                log_sequence = self.create_production_log_sequence(job_id, production_entries)
                if log_sequence:
                    sequences_to_create.append(log_sequence)
                    
                    # Create rows for the production log
                    log_rows = self.create_production_log_rows(production_entries)
                    if log_rows:
                        external_id = self.create_sequence_external_id('prodlog', str(job_id))
                        sequence_data_to_insert.append({
                            'external_id': external_id,
                            'rows': log_rows
                        })
        
        # Create sequences
        if sequences_to_create:
            try:
                created = self.cognite_client.sequences.create(sequences_to_create)
                logger.info(f"Created {len(created)} sequences")
            except Exception as e:
                if "already exists" in str(e).lower():
                    logger.info("Some sequences already exist, updating data only")
                else:
                    logger.error(f"Error creating sequences: {e}")
        
        # Insert sequence data
        for data in sequence_data_to_insert:
            try:
                self.cognite_client.sequences.data.insert(
                    external_id=data['external_id'],
                    rows=data['rows']
                )
                logger.debug(f"Inserted data for sequence {data['external_id']}")
            except Exception as e:
                logger.error(f"Error inserting data for sequence {data['external_id']}: {e}")
        
        logger.info(f"Sequence extraction completed for PCN {self.config.facility.pcn}")


async def main():
    """Main entry point"""
    config = SequenceConfig.from_env()
    
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
    
    logger.info(f"Starting Sequence Extractor for {config.facility.facility_name} (PCN: {config.facility.pcn})")
    
    extractor = SequenceExtractor(config)
    
    try:
        await extractor.run()
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())