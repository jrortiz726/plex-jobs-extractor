#!/usr/bin/env python3
"""
Quality Data Extractor for Plex MES using Data Source API

Extracts quality data including:
- Check sheets and specifications
- Non-conformance records (NCR)
- Problem control and forms
- Audits and test tracking
- Job and container quality data

ALL IDs include PCN prefix for multi-facility support.
Uses Plex Data Source API for accessing quality data sources.
"""

import os
import sys
import json
import base64
import aiohttp
import asyncio
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field
from enum import Enum

from dotenv import load_dotenv
from cognite.client import CogniteClient
from cognite.client.config import ClientConfig
from cognite.client.credentials import OAuthClientCredentials
from cognite.client.data_classes import Asset, Event, TimeSeries, Row

from multi_facility_config import MultiTenantNamingConvention, FacilityConfig

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class QualityDataSource(Enum):
    """Self-serviceable quality data sources that are granted to the user"""
    # Checksheets and Process Instructions
    CHECKSHEETS_GET = 4142  # 26 inputs, 26 outputs
    CHECKSHEET_MEASUREMENT_ADD = 16880  # 7 inputs, 3 outputs
    CHECKSHEET_ADD = 16879  # 12 inputs, 3 outputs
    CHECKSHEET_WITH_MEASUREMENTS_ADD = 21773  # 15 inputs, 7 outputs
    
    # Specifications
    SPECIFICATION_GET = 6429  # 2 inputs, 72 outputs - Available!
    SPECIFICATION_PICKER = 5112  # 6 inputs, 11 outputs - Available!
    SPECIFICATIONS_BY_PART = 230339  # 3 inputs, 4 outputs - Available!
    
    # Control Plans
    CONTROL_PLAN_LINES_EXPORT = 233636  # 10 inputs, 25 outputs
    CONTROL_PLAN_PICKER = 18531  # 4 inputs, 7 outputs
    SAMPLE_PLANS_GET = 2158  # 2 inputs, 13 outputs
    
    # SPC (Statistical Process Control)
    INSPECTION_MODES_GET = 4760  # 4 inputs, 46 outputs
    INSPECTION_MODES_PICKER = 4285  # 2 inputs, 2 outputs


@dataclass
class QualityConfig:
    """Configuration for Quality Extractor"""
    # Plex Data Source API credentials
    plex_username: str
    plex_password: str
    plex_customer_id: str
    plex_pcn_code: str  # PCN code for building the URL
    
    # CDF credentials
    cdf_host: str
    cdf_project: str
    cdf_client_id: str
    cdf_client_secret: str
    cdf_token_url: str
    
    # Facility information
    facility: FacilityConfig
    
    # Optional settings
    plex_base_url: str = "https://cloud.plex.com"  # Deprecated, using PCN code now
    use_test_env: bool = False
    extraction_interval: int = 300  # 5 minutes
    batch_size: int = 1000
    max_retries: int = 3
    retry_delay: int = 5
    
    # Dataset ID
    dataset_quality_id: Optional[int] = None
    
    # Date range for extraction
    extraction_start_date: Optional[datetime] = None
    extraction_days_back: int = 30
    
    @classmethod
    def from_env(cls) -> 'QualityConfig':
        """Load configuration from environment variables"""
        pcn = os.getenv('PLEX_CUSTOMER_ID')
        
        facility = FacilityConfig(
            pcn=pcn,
            facility_name=os.getenv('FACILITY_NAME', f'Facility {pcn}'),
            facility_code=os.getenv('FACILITY_CODE', f'F{pcn[:3]}'),
            timezone=os.getenv('FACILITY_TIMEZONE', 'UTC'),
            country=os.getenv('FACILITY_COUNTRY', 'US')
        )
        
        def get_int_env(key: str, default: int = None) -> Optional[int]:
            value = os.getenv(key)
            if value:
                try:
                    return int(value)
                except ValueError:
                    logger.warning(f"Invalid integer value for {key}: {value}")
            return default
        
        # Get extraction start date
        start_date = None
        start_date_str = os.getenv('QUALITY_EXTRACTION_START_DATE') or os.getenv('EXTRACTION_START_DATE')
        if start_date_str:
            try:
                start_date = datetime.fromisoformat(start_date_str.replace('Z', '+00:00'))
            except:
                logger.warning(f"Invalid start date format: {start_date_str}")
        
        quality_id = get_int_env(f'CDF_DATASET_{pcn}_QUALITY') or \
                    get_int_env('CDF_DATASET_PLEXQUALITY')
        
        return cls(
            plex_username=os.getenv('PLEX_DS_USERNAME', os.getenv('PLEX_USERNAME')),
            plex_password=os.getenv('PLEX_DS_PASSWORD', os.getenv('PLEX_PASSWORD')),
            plex_customer_id=pcn,
            plex_pcn_code=os.getenv('PLEX_PCN_CODE', 'ra-process'),
            facility=facility,
            cdf_host=os.getenv('CDF_HOST'),
            cdf_project=os.getenv('CDF_PROJECT'),
            cdf_client_id=os.getenv('CDF_CLIENT_ID'),
            cdf_client_secret=os.getenv('CDF_CLIENT_SECRET'),
            cdf_token_url=os.getenv('CDF_TOKEN_URL'),
            use_test_env=os.getenv('PLEX_USE_TEST', 'false').lower() == 'true',
            extraction_interval=get_int_env('QUALITY_EXTRACTION_INTERVAL', 300),
            batch_size=get_int_env('QUALITY_BATCH_SIZE', 1000),
            dataset_quality_id=quality_id,
            extraction_start_date=start_date,
            extraction_days_back=get_int_env('QUALITY_DAYS_BACK', 30)
        )


class PlexDataSourceClient:
    """Client for Plex Data Source API"""
    
    def __init__(self, config: QualityConfig):
        self.config = config
        self.session: Optional[aiohttp.ClientSession] = None
        
        # Build base URL using PCN code
        pcn_code = config.plex_pcn_code or os.getenv('PLEX_PCN_CODE', 'ra-process')
        
        if config.use_test_env:
            self.base_url = f"https://{pcn_code}.test.on.plex.com"
        else:
            self.base_url = f"https://{pcn_code}.on.plex.com"
        
        # Create authorization header
        credentials = f"{config.plex_username}:{config.plex_password}"
        encoded = base64.b64encode(credentials.encode('utf-8')).decode('ascii')
        self.auth_header = f"Basic {encoded}"
    
    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
    
    async def execute_data_source(
        self,
        data_source_id: int,
        inputs: Dict[str, Any],
        format_type: int = 2,
        pretty: bool = False
    ) -> Dict[str, Any]:
        """Execute a Plex data source"""
        url = f"{self.base_url}/api/datasources/{data_source_id}/execute"
        
        # Add query parameters
        params = {'format': str(format_type)}
        if pretty:
            params['pretty'] = 'true'
        
        headers = {
            'Authorization': self.auth_header,
            'Content-Type': 'application/json; charset=utf-8',
            'Accept': 'application/json',
            'Accept-Encoding': 'gzip, deflate'
        }
        
        # Format request body based on format type
        # format=2 does NOT use 'inputs' wrapper
        if format_type == 2:
            body = inputs
        else:
            body = {'inputs': inputs}
        
        for attempt in range(self.config.max_retries):
            try:
                async with self.session.post(
                    url,
                    headers=headers,
                    json=body,
                    params=params
                ) as response:
                    if response.status == 200:
                        return await response.json()
                    elif response.status in [401, 403]:
                        error_data = await response.json()
                        logger.error(f"Authentication error: {error_data}")
                        raise Exception(f"Auth failed: {error_data}")
                    else:
                        error_text = await response.text()
                        logger.warning(f"Data source {data_source_id} returned {response.status}: {error_text}")
                        if attempt < self.config.max_retries - 1:
                            await asyncio.sleep(self.config.retry_delay * (attempt + 1))
                        else:
                            raise Exception(f"Data source execution failed: {error_text}")
            
            except aiohttp.ClientError as e:
                logger.error(f"Network error calling data source {data_source_id}: {e}")
                if attempt < self.config.max_retries - 1:
                    await asyncio.sleep(self.config.retry_delay * (attempt + 1))
                else:
                    raise
    
    async def get_data_source_metadata(self, data_source_id: int) -> Dict[str, Any]:
        """Get metadata for a data source"""
        url = f"{self.base_url}/api/datasources/{data_source_id}"
        
        headers = {
            'Authorization': self.auth_header,
            'Accept': 'application/json'
        }
        
        async with self.session.get(url, headers=headers) as response:
            if response.status == 200:
                return await response.json()
            else:
                error_text = await response.text()
                raise Exception(f"Failed to get metadata: {error_text}")


class QualityExtractor:
    """Extracts quality data from Plex using Data Source API"""
    
    def __init__(self, config: QualityConfig):
        self.config = config
        self.naming = MultiTenantNamingConvention(config.facility)
        self.cognite_client = self._init_cognite_client()
        self.processed_records = set()
        
        # Calculate extraction date range
        if config.extraction_start_date:
            self.start_date = config.extraction_start_date
        else:
            self.start_date = datetime.now(timezone.utc) - timedelta(days=config.extraction_days_back)
        
        self.end_date = datetime.now(timezone.utc)
    
    def _init_cognite_client(self) -> CogniteClient:
        """Initialize Cognite client"""
        creds = OAuthClientCredentials(
            token_url=self.config.cdf_token_url,
            client_id=self.config.cdf_client_id,
            client_secret=self.config.cdf_client_secret,
            scopes=["user_impersonation"]
        )
        
        config = ClientConfig(
            client_name="quality-extractor",
            base_url=self.config.cdf_host,
            project=self.config.cdf_project,
            credentials=creds
        )
        
        return CogniteClient(config)
    
    async def extract_specifications(self, ds_client: PlexDataSourceClient) -> List[Event]:
        """Extract part specifications and check sheets"""
        logger.info("Extracting specifications and check sheets...")
        events = []
        
        try:
            # Get specifications using picker
            response = await ds_client.execute_data_source(
                QualityDataSource.SPECIFICATION_PICKER.value,
                {
                    'Active_Flag': True,
                    'Part_Type': '',  # All part types
                    'Part_Status': 'Production'
                }
            )
            
            if 'rows' in response:
                for row in response['rows']:
                    spec_key = row.get('Specification_Key')
                    
                    # Skip if already processed
                    record_id = f"spec_{spec_key}"
                    if record_id in self.processed_records:
                        continue
                    
                    # Get detailed specification data
                    detail_response = await ds_client.execute_data_source(
                        QualityDataSource.SPECIFICATION_GET.value,
                        {'Specification_Key': spec_key}
                    )
                    
                    if 'outputs' in detail_response:
                        spec_data = detail_response['outputs']
                        
                        # Create event for specification
                        event = Event(
                            external_id=self.naming.event_id(
                                'specification',
                                str(spec_key),
                                int(datetime.now(timezone.utc).timestamp())
                            ),
                            type='quality_specification',
                            subtype='checksheet',
                            start_time=int(datetime.now(timezone.utc).timestamp() * 1000),
                            metadata={
                                'pcn': self.config.facility.pcn,
                                'facility_name': self.config.facility.facility_name,
                                'specification_no': spec_data.get('Specification_No', ''),
                                'specification_name': spec_data.get('Name', ''),
                                'part_no': spec_data.get('Part_No', ''),
                                'part_revision': spec_data.get('Revision', ''),
                                'specification_type': spec_data.get('Specification_Type', ''),
                                'dimension_type': spec_data.get('Dimension_Type', ''),
                                'nominal': str(spec_data.get('Nominal', '')),
                                'upper_limit': str(spec_data.get('Upper_Limit', '')),
                                'lower_limit': str(spec_data.get('Lower_Limit', ''))
                            },
                            data_set_id=self.config.dataset_quality_id
                        )
                        events.append(event)
                        self.processed_records.add(record_id)
                        
        except Exception as e:
            logger.error(f"Error extracting specifications: {e}")
        
        logger.info(f"Extracted {len(events)} specification events")
        return events
    
    async def extract_inspection_modes(self, ds_client: PlexDataSourceClient) -> List[Event]:
        """Extract inspection modes and SPC data"""
        logger.info("Extracting inspection modes and SPC data...")
        events = []
        
        try:
            # Get inspection modes - using the self-serviceable data source
            response = await ds_client.execute_data_source(
                QualityDataSource.INSPECTION_MODES_GET.value,
                {}  # Start with empty inputs, may need adjustments
            )
            
            if 'rows' in response:
                for row in response['rows']:
                    mode_key = row.get('Inspection_Mode_Key') or row.get('id')
                    
                    # Skip if already processed
                    record_id = f"inspection_{mode_key}"
                    if record_id in self.processed_records:
                        continue
                    
                    # Create event for inspection mode
                    event = Event(
                        external_id=self.naming.event_id(
                            'inspection_mode',
                            str(mode_key),
                            int(datetime.now(timezone.utc).timestamp())
                        ),
                        type='quality_inspection',
                        subtype='spc_mode',
                        start_time=int(datetime.now(timezone.utc).timestamp() * 1000),
                        metadata={
                            'pcn': self.config.facility.pcn,
                            'facility_name': self.config.facility.facility_name,
                            'inspection_mode': row.get('Inspection_Mode', ''),
                            'mode_description': row.get('Description', ''),
                            'mode_type': row.get('Mode_Type', ''),
                            'frequency': row.get('Frequency', ''),
                            'status': row.get('Status', 'Active')
                        },
                        data_set_id=self.config.dataset_quality_id
                    )
                    events.append(event)
                    self.processed_records.add(record_id)
                    
        except Exception as e:
            logger.error(f"Error extracting inspection modes: {e}")
        
        logger.info(f"Extracted {len(events)} inspection mode events")
        return events
    
    async def extract_sample_plans(self, ds_client: PlexDataSourceClient) -> List[Event]:
        """Extract sample plans data"""
        logger.info("Extracting sample plans...")
        events = []
        
        try:
            # Get sample plans using SAMPLE_PLANS_GET
            response = await ds_client.execute_data_source(
                QualityDataSource.SAMPLE_PLANS_GET.value,
                {}  # Start with empty inputs
            )
            
            if 'rows' in response:
                for row in response['rows']:
                    plan_key = row.get('Sample_Plan_Key') or row.get('id')
                    
                    # Skip if already processed
                    record_id = f"sample_plan_{plan_key}"
                    if record_id in self.processed_records:
                        continue
                    
                    # Create event for sample plan
                    event = Event(
                        external_id=self.naming.event_id(
                            'sample_plan',
                            str(plan_key),
                            int(datetime.now(timezone.utc).timestamp())
                        ),
                        type='quality_sample_plan',
                        subtype='inspection_plan',
                        start_time=int(datetime.now(timezone.utc).timestamp() * 1000),
                        metadata={
                            'pcn': self.config.facility.pcn,
                            'facility_name': self.config.facility.facility_name,
                            'plan_name': row.get('Plan_Name', ''),
                            'plan_type': row.get('Plan_Type', ''),
                            'sample_size': str(row.get('Sample_Size', '')),
                            'frequency': row.get('Frequency', ''),
                            'acceptance_criteria': row.get('Acceptance_Criteria', ''),
                            'status': row.get('Status', 'Active')
                        },
                        data_set_id=self.config.dataset_quality_id
                    )
                    events.append(event)
                    self.processed_records.add(record_id)
                    
        except Exception as e:
            logger.error(f"Error extracting sample plans: {e}")
        
        logger.info(f"Extracted {len(events)} sample plan events")
        return events
    
    async def extract_checksheets(self, ds_client: PlexDataSourceClient) -> List[Event]:
        """Extract checksheets data"""
        logger.info("Extracting checksheets...")
        events = []
        
        try:
            # Get checksheets using CHECKSHEETS_GET
            response = await ds_client.execute_data_source(
                QualityDataSource.CHECKSHEETS_GET.value,
                {}  # Empty inputs - will be wrapped by execute_data_source
            )
            
            if response and 'outputs' in response:
                # Create event for checksheet data
                outputs = response['outputs']
                event = Event(
                    external_id=self.naming.event_id(
                        'checksheet',
                        'batch',
                        int(datetime.now(timezone.utc).timestamp())
                    ),
                    type='quality_checksheet',
                    subtype='inspection',
                    start_time=int(datetime.now(timezone.utc).timestamp() * 1000),
                    metadata={
                        'pcn': self.config.facility.pcn,
                        'facility_name': self.config.facility.facility_name,
                        'data_source': 'checksheets_get',
                        'record_count': str(len(response.get('tables', [])))
                    },
                    data_set_id=self.config.dataset_quality_id
                )
                events.append(event)
                
        except Exception as e:
            logger.error(f"Error extracting checksheets: {e}")
        
        logger.info(f"Extracted {len(events)} checksheet events")
        return events
    
    async def extract_control_plans(self, ds_client: PlexDataSourceClient) -> List[Event]:
        """Extract control plans data"""
        logger.info("Extracting control plans...")
        events = []
        
        try:
            # Get control plans using CONTROL_PLAN_PICKER
            response = await ds_client.execute_data_source(
                QualityDataSource.CONTROL_PLAN_PICKER.value,
                {}  # Empty inputs - will be wrapped by execute_data_source
            )
            
            if response and 'rows' in response:
                for row in response['rows']:
                    plan_id = row.get('Control_Plan_Key') or row.get('id')
                    if not plan_id:
                        continue
                    
                    event = Event(
                        external_id=self.naming.event_id(
                            'control_plan',
                            str(plan_id),
                            int(datetime.now(timezone.utc).timestamp())
                        ),
                        type='quality_control_plan',
                        subtype='plan',
                        start_time=int(datetime.now(timezone.utc).timestamp() * 1000),
                        metadata={
                            'pcn': self.config.facility.pcn,
                            'facility_name': self.config.facility.facility_name,
                            'control_plan_id': str(plan_id),
                            'plan_name': row.get('Name', ''),
                            'status': row.get('Status', 'Active')
                        },
                        data_set_id=self.config.dataset_quality_id
                    )
                    events.append(event)
                    
        except Exception as e:
            logger.error(f"Error extracting control plans: {e}")
        
        logger.info(f"Extracted {len(events)} control plan events")
        return events
    
    async def extract_control_plan_lines(self, ds_client: PlexDataSourceClient) -> List[Event]:
        """Extract control plan lines export data"""
        logger.info("Extracting control plan lines...")
        events = []
        
        try:
            # Get control plan lines using CONTROL_PLAN_LINES_EXPORT
            response = await ds_client.execute_data_source(
                QualityDataSource.CONTROL_PLAN_LINES_EXPORT.value,
                {}  # Empty inputs - will be wrapped by execute_data_source
            )
            
            if response and 'rows' in response:
                for row in response['rows']:
                    line_key = row.get('Control_Plan_Line_Key') or row.get('id')
                    
                    # Skip if already processed
                    record_id = f"control_plan_line_{line_key}"
                    if record_id in self.processed_records:
                        continue
                    
                    # Create event for control plan line
                    event = Event(
                        external_id=self.naming.event_id(
                            'control_plan_line',
                            str(line_key),
                            int(datetime.now(timezone.utc).timestamp())
                        ),
                        type='quality_control_plan',
                        subtype='plan_line',
                        start_time=int(datetime.now(timezone.utc).timestamp() * 1000),
                        metadata={
                            'pcn': self.config.facility.pcn,
                            'facility_name': self.config.facility.facility_name,
                            'line_number': str(row.get('Line_Number', '')),
                            'characteristic': row.get('Characteristic', ''),
                            'specification': row.get('Specification', ''),
                            'tolerance': row.get('Tolerance', ''),
                            'measurement_method': row.get('Measurement_Method', ''),
                            'frequency': row.get('Frequency', ''),
                            'sample_size': str(row.get('Sample_Size', '')),
                            'control_method': row.get('Control_Method', ''),
                            'reaction_plan': row.get('Reaction_Plan', '')
                        },
                        data_set_id=self.config.dataset_quality_id
                    )
                    events.append(event)
                    self.processed_records.add(record_id)
                    
        except Exception as e:
            logger.error(f"Error extracting control plan lines: {e}")
        
        logger.info(f"Extracted {len(events)} control plan line events")
        return events
    
    async def create_quality_metrics(self) -> List[TimeSeries]:
        """Create time series for quality metrics"""
        logger.info("Creating quality metrics time series...")
        time_series = []
        
        metrics = [
            ('defect_rate', 'Defect rate percentage'),
            ('first_pass_yield', 'First pass yield percentage'),
            ('scrap_cost', 'Scrap cost in dollars'),
            ('ncr_count', 'Non-conformance record count'),
            ('audit_score', 'Quality audit score'),
            ('test_pass_rate', 'Test pass rate percentage')
        ]
        
        for metric_name, description in metrics:
            ts = TimeSeries(
                external_id=self.naming.timeseries_id('QUALITY', 'FACILITY', metric_name),
                name=f"Quality {metric_name.replace('_', ' ').title()}",
                description=description,
                metadata={
                    'pcn': self.config.facility.pcn,
                    'facility_name': self.config.facility.facility_name,
                    'metric_type': 'quality',
                    'metric_name': metric_name
                },
                data_set_id=self.config.dataset_quality_id
            )
            time_series.append(ts)
        
        # Create time series in CDF
        try:
            created = self.cognite_client.time_series.create(time_series)
            logger.info(f"Created {len(created)} quality metric time series")
        except Exception as e:
            logger.warning(f"Some time series may already exist: {e}")
        
        return time_series
    
    async def run(self):
        """Main extraction process"""
        logger.info(f"Starting quality extraction for PCN {self.config.facility.pcn}")
        logger.info(f"Date range: {self.start_date} to {self.end_date}")
        logger.info("Using only self-serviceable quality data sources")
        
        async with PlexDataSourceClient(self.config) as ds_client:
            all_events = []
            
            # Extract different types of quality data using only available data sources
            
            # 1. Specifications (SPECIFICATION_GET, SPECIFICATION_PICKER, SPECIFICATIONS_BY_PART)
            spec_events = await self.extract_specifications(ds_client)
            all_events.extend(spec_events)
            
            # 2. Checksheets (CHECKSHEETS_GET)
            checksheet_events = await self.extract_checksheets(ds_client)
            all_events.extend(checksheet_events)
            
            # 3. Control Plans (CONTROL_PLAN_PICKER)
            control_plan_events = await self.extract_control_plans(ds_client)
            all_events.extend(control_plan_events)
            
            # 4. Control Plan Lines (CONTROL_PLAN_LINES_EXPORT)
            control_line_events = await self.extract_control_plan_lines(ds_client)
            all_events.extend(control_line_events)
            
            # 5. Sample Plans (SAMPLE_PLANS_GET)
            sample_plan_events = await self.extract_sample_plans(ds_client)
            all_events.extend(sample_plan_events)
            
            # 6. Inspection Modes / SPC (INSPECTION_MODES_GET)
            inspection_events = await self.extract_inspection_modes(ds_client)
            all_events.extend(inspection_events)
            
            # Upload events to CDF
            if all_events:
                logger.info(f"Uploading {len(all_events)} quality events to CDF...")
                try:
                    for i in range(0, len(all_events), self.config.batch_size):
                        batch = all_events[i:i + self.config.batch_size]
                        self.cognite_client.events.create(batch)
                        logger.info(f"Uploaded batch {i//self.config.batch_size + 1}")
                except Exception as e:
                    logger.error(f"Error uploading events: {e}")
            
            # Create quality metrics time series
            await self.create_quality_metrics()
        
        logger.info("Quality extraction completed")


async def main():
    """Main entry point"""
    config = QualityConfig.from_env()
    extractor = QualityExtractor(config)
    await extractor.run()


if __name__ == '__main__':
    asyncio.run(main())