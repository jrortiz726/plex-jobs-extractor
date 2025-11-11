#!/usr/bin/env python3
"""
Enhanced Quality Extractor with All Improvements
- Full async/await implementation
- Type hints throughout
- Error handling with retry
- Asset ID resolution
- Structured logging
- Data Source API integration
"""

from __future__ import annotations

import asyncio
import base64
import json
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any, Tuple, Final, TypeAlias
from dataclasses import dataclass, field
from enum import StrEnum, auto

import structlog
import aiohttp
from cognite.client.data_classes import Asset, Event, TimeSeries, Datapoints

from base_extractor_enhanced import (
    BaseExtractor, BaseExtractorConfig, ExtractionResult,
    DatasetType, with_retry
)
from error_handling import PlexAPIError, handle_api_response

# Setup structured logging
logger = structlog.get_logger(__name__)

# Type aliases
NCRId: TypeAlias = str
ChecksheetId: TypeAlias = str
SpecificationId: TypeAlias = str
InspectionId: TypeAlias = str
PartId: TypeAlias = str


class QualityEventType(StrEnum):
    """Quality event types"""
    NCR = auto()  # Non-conformance report
    INSPECTION = auto()
    AUDIT = auto()
    CHECKSHEET = auto()
    PROBLEM_REPORT = auto()
    CORRECTIVE_ACTION = auto()


class NCRStatus(StrEnum):
    """NCR status enumeration"""
    OPEN = auto()
    IN_REVIEW = auto()
    APPROVED = auto()
    REJECTED = auto()
    CLOSED = auto()


@dataclass
class NCReport:
    """Non-conformance report data"""
    id: NCRId
    number: str
    status: NCRStatus
    part_id: Optional[PartId] = None
    part_name: Optional[str] = None
    quantity_affected: int = 0
    defect_description: Optional[str] = None
    root_cause: Optional[str] = None
    corrective_action: Optional[str] = None
    created_date: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    closed_date: Optional[datetime] = None
    operator: Optional[str] = None
    workcenter_id: Optional[str] = None
    severity: Optional[str] = None
    cost_impact: Optional[float] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Specification:
    """Quality specification data"""
    id: SpecificationId
    name: str
    specification_type: str  # dimensional, material, performance, etc.
    part_id: Optional[PartId] = None
    nominal_value: Optional[float] = None
    upper_limit: Optional[float] = None
    lower_limit: Optional[float] = None
    unit_of_measure: Optional[str] = None
    active: bool = True
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Checksheet:
    """Quality checksheet data"""
    id: ChecksheetId
    name: str
    part_id: Optional[PartId] = None
    operation_id: Optional[str] = None
    frequency: Optional[str] = None  # per piece, hourly, daily, etc.
    check_items: List[Dict[str, Any]] = field(default_factory=list)
    active: bool = True
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class InspectionResult:
    """Inspection result data"""
    id: InspectionId
    result: str  # pass, fail, conditional - moved before optional fields
    checksheet_id: Optional[ChecksheetId] = None
    part_id: Optional[PartId] = None
    serial_number: Optional[str] = None
    measurements: List[Dict[str, Any]] = field(default_factory=list)
    inspector: Optional[str] = None
    inspection_date: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    defects_found: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ProblemReport:
    """Problem report data"""
    id: str
    title: str
    description: str
    status: str
    priority: str
    reported_date: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    reporter: Optional[str] = None
    assigned_to: Optional[str] = None
    resolution: Optional[str] = None
    resolved_date: Optional[datetime] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


class QualityExtractorConfig(BaseExtractorConfig):
    """Configuration specific to quality extractor"""
    extract_ncrs: bool = True
    extract_specifications: bool = True
    extract_checksheets: bool = True
    extract_inspections: bool = True
    extract_problem_reports: bool = True
    use_datasource_api: bool = True
    datasource_username: Optional[str] = None
    datasource_password: Optional[str] = None
    lookback_days: int = 30
    
    @classmethod
    def from_env(cls) -> QualityExtractorConfig:
        """Load configuration from environment"""
        import os
        base = BaseExtractorConfig.from_env('quality')
        
        return cls(
            **base.dict(),
            extract_ncrs=os.getenv('EXTRACT_NCRS', 'true').lower() == 'true',
            extract_specifications=os.getenv('EXTRACT_SPECIFICATIONS', 'true').lower() == 'true',
            extract_checksheets=os.getenv('EXTRACT_CHECKSHEETS', 'true').lower() == 'true',
            extract_inspections=os.getenv('EXTRACT_INSPECTIONS', 'true').lower() == 'true',
            extract_problem_reports=os.getenv('EXTRACT_PROBLEM_REPORTS', 'true').lower() == 'true',
            use_datasource_api=os.getenv('USE_DATASOURCE_API', 'true').lower() == 'true',
            datasource_username=os.getenv('PLEX_DS_USERNAME'),
            datasource_password=os.getenv('PLEX_DS_PASSWORD'),
            lookback_days=int(os.getenv('QUALITY_LOOKBACK_DAYS', '30'))
        )


class DataSourceAPIClient:
    """Client for Plex Data Source API"""
    
    def __init__(self, username: str, password: str, pcn_code: str, use_test: bool = False):
        self.username = username
        self.password = password
        
        # Build base URL
        if use_test:
            self.base_url = f"https://{pcn_code}.test.on.plex.com"
        else:
            self.base_url = f"https://{pcn_code}.on.plex.com"
        
        # Create authorization header
        credentials = f"{username}:{password}"
        encoded = base64.b64encode(credentials.encode('utf-8')).decode('ascii')
        self.auth_header = f"Basic {encoded}"
        
        # HTTP client
        self.session: Optional[aiohttp.ClientSession] = None
        
        logger.info(
            "datasource_api_initialized",
            base_url=self.base_url,
            username=username
        )
    
    async def __aenter__(self):
        """Async context manager entry"""
        self.session = aiohttp.ClientSession()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        if self.session:
            await self.session.close()
    
    @with_retry(max_attempts=3)
    async def execute_datasource(
        self,
        datasource_id: int,
        inputs: Optional[Dict[str, Any]] = None,
        format_type: int = 2
    ) -> Dict[str, Any]:
        """Execute a data source and return results"""
        if not self.session:
            self.session = aiohttp.ClientSession()
        
        url = f"{self.base_url}/api/datasources/{datasource_id}/execute?format={format_type}"
        
        headers = {
            'Authorization': self.auth_header,
            'Content-Type': 'application/json; charset=utf-8',
            'Accept': 'application/json'
        }
        
        body = inputs or {}
        
        logger.debug(
            "datasource_api_request",
            datasource_id=datasource_id,
            inputs=inputs
        )
        
        try:
            async with self.session.post(url, headers=headers, json=body) as response:
                handle_api_response(response, "DataSource API")
                
                if response.status == 200:
                    data = await response.json()
                    return data
                else:
                    error = await response.text()
                    logger.error(
                        "datasource_api_error",
                        datasource_id=datasource_id,
                        status=response.status,
                        error=error[:200]
                    )
                    raise PlexAPIError(f"DataSource {datasource_id} failed: {error[:200]}")
                    
        except Exception as e:
            logger.error("datasource_execution_error", error=str(e))
            raise


class EnhancedQualityExtractor(BaseExtractor):
    """Enhanced quality extractor with all improvements"""
    
    def __init__(self, config: Optional[QualityExtractorConfig] = None):
        """Initialize with enhanced configuration"""
        config = config or QualityExtractorConfig.from_env()
        super().__init__(config, 'quality')
        
        self.config: Final[QualityExtractorConfig] = config
        self.processed_ncrs: set[NCRId] = set()
        self.processed_inspections: set[InspectionId] = set()
        
        # Initialize DataSource API client if configured
        self.ds_client: Optional[DataSourceAPIClient] = None
        if config.use_datasource_api and config.datasource_username and config.datasource_password:
            import os
            pcn_code = os.getenv('PLEX_PCN_CODE', 'ra-process')
            use_test = os.getenv('PLEX_USE_TEST', 'false').lower() == 'true'
            
            self.ds_client = DataSourceAPIClient(
                username=config.datasource_username,
                password=config.datasource_password,
                pcn_code=pcn_code,
                use_test=use_test
            )
        
        self.logger.info(
            "quality_extractor_initialized",
            extract_ncrs=config.extract_ncrs,
            extract_specifications=config.extract_specifications,
            extract_checksheets=config.extract_checksheets,
            extract_inspections=config.extract_inspections,
            use_datasource_api=config.use_datasource_api and self.ds_client is not None,
            lookback_days=config.lookback_days
        )
    
    def get_required_datasets(self) -> List[str]:
        """Quality requires quality and master datasets"""
        return ['quality', 'master']
    
    async def extract(self) -> ExtractionResult:
        """Main extraction with concurrent operations"""
        start_time = datetime.now(timezone.utc)
        result = ExtractionResult(
            success=True,
            items_processed=0,
            duration_ms=0
        )
        
        try:
            # Ensure quality hierarchy exists
            await self._ensure_quality_hierarchy()
            
            tasks = []
            
            # Use DataSource API context if available
            if self.ds_client:
                async with self.ds_client:
                    async with asyncio.TaskGroup() as tg:
                        if self.config.extract_specifications:
                            tasks.append(tg.create_task(self._extract_specifications()))
                        
                        if self.config.extract_checksheets:
                            tasks.append(tg.create_task(self._extract_checksheets()))
                        
                        if self.config.extract_ncrs:
                            tasks.append(tg.create_task(self._extract_ncrs()))
                        
                        if self.config.extract_inspections:
                            tasks.append(tg.create_task(self._extract_inspections()))
                        
                        if self.config.extract_problem_reports:
                            tasks.append(tg.create_task(self._extract_problem_reports()))
            else:
                # Use regular API without DataSource
                async with asyncio.TaskGroup() as tg:
                    if self.config.extract_ncrs:
                        tasks.append(tg.create_task(self._extract_ncrs_api()))
            
            # Aggregate results
            for task in tasks:
                task_result = await task
                result.items_processed += task_result.items_processed
                if not task_result.success:
                    result.success = False
                    result.errors.extend(task_result.errors)
            
            self.logger.info(
                "quality_extraction_completed",
                items_processed=result.items_processed,
                success=result.success
            )
            
        except Exception as e:
            result.success = False
            result.errors.append(str(e))
            self.logger.error("quality_extraction_failed", error=str(e), exc_info=True)
        
        result.duration_ms = (datetime.now(timezone.utc) - start_time).total_seconds() * 1000
        return result
    
    async def _ensure_quality_hierarchy(self) -> None:
        """Ensure quality asset hierarchy exists"""
        try:
            root_assets = [
                Asset(
                    external_id=self.create_asset_external_id('quality_root', self.config.facility.pcn),
                    name=f"{self.config.facility.facility_name} - Quality",
                    parent_external_id=self.create_asset_external_id('facility', self.config.facility.pcn),
                    description="Root asset for quality data",
                    metadata={
                        **self.naming.get_metadata_tags(),
                        'asset_type': 'quality_root'
                    },
                    data_set_id=self.get_dataset_id('quality')
                ),
                Asset(
                    external_id=self.create_asset_external_id('specifications_root', self.config.facility.pcn),
                    name=f"{self.config.facility.facility_name} - Specifications",
                    parent_external_id=self.create_asset_external_id('quality_root', self.config.facility.pcn),
                    description="Quality specifications library",
                    metadata={
                        **self.naming.get_metadata_tags(),
                        'asset_type': 'specifications_root'
                    },
                    data_set_id=self.get_dataset_id('quality')
                ),
                Asset(
                    external_id=self.create_asset_external_id('checksheets_root', self.config.facility.pcn),
                    name=f"{self.config.facility.facility_name} - Checksheets",
                    parent_external_id=self.create_asset_external_id('quality_root', self.config.facility.pcn),
                    description="Quality checksheets library",
                    metadata={
                        **self.naming.get_metadata_tags(),
                        'asset_type': 'checksheets_root'
                    },
                    data_set_id=self.get_dataset_id('quality')
                )
            ]
            
            await self.create_assets_with_retry(root_assets, resolve_parents=True)
            
        except Exception as e:
            self.logger.error("quality_hierarchy_creation_error", error=str(e))
    
    @with_retry(max_attempts=3)
    async def _extract_specifications(self) -> ExtractionResult:
        """Extract quality specifications using DataSource API"""
        result = ExtractionResult(success=True, items_processed=0, duration_ms=0)
        
        if not self.ds_client:
            self.logger.warning("datasource_api_not_configured")
            return result
        
        try:
            # DataSource ID for specifications
            SPEC_DATASOURCE_ID = 6429  # Specification_Get
            
            # Specification_Get only needs minimal inputs
            # According to docs it has 2 inputs
            inputs = {
                'Part_No': '',  # Empty string to get all parts
                'Active': 1     # 1 for active specs
            }
            
            # Execute datasource
            data = await self.ds_client.execute_datasource(SPEC_DATASOURCE_ID, inputs)
            
            specifications = []
            if 'rows' in data:
                for row in data['rows']:
                    spec = self._parse_specification(row)
                    if spec:
                        specifications.append(spec)
            
            # Create specification assets
            assets = []
            for spec in specifications:
                asset = self._create_specification_asset(spec)
                if asset:
                    assets.append(asset)
            
            # Create in CDF
            if assets:
                created, failed = await self.create_assets_with_retry(
                    assets,
                    resolve_parents=True
                )
                result.items_processed = len(created)
            
            self.logger.info(
                "specifications_extracted",
                specifications_found=len(specifications),
                assets_created=result.items_processed
            )
            
        except Exception as e:
            result.success = False
            result.errors.append(f"Specifications extraction failed: {e}")
            self.logger.error("specifications_extraction_error", error=str(e))
        
        return result
    
    @with_retry(max_attempts=3)
    async def _extract_checksheets(self) -> ExtractionResult:
        """Extract quality checksheets using DataSource API"""
        result = ExtractionResult(success=True, items_processed=0, duration_ms=0)
        
        if not self.ds_client:
            return result
        
        try:
            # DataSource ID for checksheets
            CHECKSHEET_DATASOURCE_ID = 4142  # Checksheets_Get
            
            # Most parameters are optional, use minimal set
            end_date = datetime.now(timezone.utc)
            start_date = end_date - timedelta(days=7)  # Last 7 days
            
            inputs = {
                'Date_Begin': start_date.strftime('%Y-%m-%d'),
                'Date_End': end_date.strftime('%Y-%m-%d'),
                'Max_Records': 100  # Limit results
            }
            
            # Execute datasource
            data = await self.ds_client.execute_datasource(CHECKSHEET_DATASOURCE_ID, inputs)
            
            checksheets = []
            if 'rows' in data:
                for row in data['rows']:
                    checksheet = self._parse_checksheet(row)
                    if checksheet:
                        checksheets.append(checksheet)
            
            # Create checksheet assets
            assets = []
            for checksheet in checksheets:
                asset = self._create_checksheet_asset(checksheet)
                if asset:
                    assets.append(asset)
            
            # Create in CDF
            if assets:
                created, failed = await self.create_assets_with_retry(
                    assets,
                    resolve_parents=True
                )
                result.items_processed = len(created)
            
            self.logger.info(
                "checksheets_extracted",
                checksheets_found=len(checksheets),
                assets_created=result.items_processed
            )
            
        except Exception as e:
            result.success = False
            result.errors.append(f"Checksheets extraction failed: {e}")
            self.logger.error("checksheets_extraction_error", error=str(e))
        
        return result
    
    @with_retry(max_attempts=3)
    async def _extract_ncrs(self) -> ExtractionResult:
        """Extract NCRs using DataSource API or regular API"""
        result = ExtractionResult(success=True, items_processed=0, duration_ms=0)
        
        try:
            ncrs = []
            
            # NCR data source not available in the quality data sources
            # Skip NCR extraction via Data Source API
            self.logger.info("ncr_extraction_skipped", reason="No NCR data source available")
            
            if False:  # Disabled - no NCR data source available
                # Use DataSource API
                NCR_DATASOURCE_ID = 12345  # No actual NCR datasource available
                
                end_date = datetime.now(timezone.utc)
                start_date = end_date - timedelta(days=self.config.lookback_days)
                
                inputs = {
                    'From_Date': start_date.isoformat(),
                    'To_Date': end_date.isoformat()
                }
                
                data = await self.ds_client.execute_datasource(NCR_DATASOURCE_ID, inputs)
                
                if 'rows' in data:
                    for row in data['rows']:
                        ncr = self._parse_ncr(row)
                        if ncr:
                            ncrs.append(ncr)
            else:
                # Fallback to regular API
                ncrs = await self._fetch_ncrs_api()
            
            # Create NCR events
            events = []
            for ncr in ncrs:
                event = self._create_ncr_event(ncr)
                if event:
                    events.append(event)
            
            # Create in CDF
            if events:
                created, duplicates = await self.create_events_with_retry(
                    events,
                    link_assets=True
                )
                result.items_processed = len(created)
                
                # Update processed set
                self.processed_ncrs.update([e.external_id for e in events if e])
            
            # Create NCR metrics time series
            await self._create_ncr_metrics(ncrs)
            
            self.logger.info(
                "ncrs_extracted",
                ncrs_found=len(ncrs),
                events_created=result.items_processed
            )
            
        except Exception as e:
            result.success = False
            result.errors.append(f"NCR extraction failed: {e}")
            self.logger.error("ncr_extraction_error", error=str(e))
        
        return result
    
    @with_retry(max_attempts=3)
    async def _extract_inspections(self) -> ExtractionResult:
        """Extract inspection results"""
        result = ExtractionResult(success=True, items_processed=0, duration_ms=0)
        
        try:
            inspections = []
            
            if self.ds_client:
                # Use DataSource API for inspection modes
                INSPECTION_DATASOURCE_ID = 4760  # Inspection_Modes_Get
                
                data = await self.ds_client.execute_datasource(INSPECTION_DATASOURCE_ID)
                
                if 'rows' in data:
                    for row in data['rows']:
                        inspection = self._parse_inspection(row)
                        if inspection:
                            inspections.append(inspection)
            
            # Create inspection events
            events = []
            for inspection in inspections:
                event = self._create_inspection_event(inspection)
                if event:
                    events.append(event)
            
            # Create in CDF
            if events:
                created, duplicates = await self.create_events_with_retry(
                    events,
                    link_assets=True
                )
                result.items_processed = len(created)
                
                # Update processed set
                self.processed_inspections.update([e.external_id for e in events if e])
            
            self.logger.info(
                "inspections_extracted",
                inspections_found=len(inspections),
                events_created=result.items_processed
            )
            
        except Exception as e:
            result.success = False
            result.errors.append(f"Inspections extraction failed: {e}")
            self.logger.error("inspections_extraction_error", error=str(e))
        
        return result
    
    @with_retry(max_attempts=3)
    async def _extract_problem_reports(self) -> ExtractionResult:
        """Extract problem reports"""
        result = ExtractionResult(success=True, items_processed=0, duration_ms=0)
        
        # Problem reports must be fetched via DataSource API
        # There is no regular REST API endpoint for quality data
        self.logger.info("problem_reports_extraction_skipped", 
                        reason="Must use DataSource API - no REST endpoint exists")
        return result
        
        # Original attempt to use non-existent endpoint commented out:
        # try:
        #     endpoint = "/quality/v1/problem-reports"  # This endpoint doesn't exist
        #     
        #     end_date = datetime.now(timezone.utc)
        #     start_date = end_date - timedelta(days=self.config.lookback_days)
        #     
        #     params = {
        #         'dateFrom': start_date.isoformat(),
        #         'dateTo': end_date.isoformat(),
        #         'limit': 1000
        #     }
        #     
        #     data = await self.fetch_plex_data(endpoint, params)
        #     
        #     problem_reports = []
        #     for pr_data in data if isinstance(data, list) else data.get('data', []):
        #         pr = self._parse_problem_report(pr_data)
        #         if pr:
        #             problem_reports.append(pr)
        #     
        #     # Create problem report events
        #     events = []
        #     for pr in problem_reports:
        #         event = self._create_problem_report_event(pr)
        #         if event:
        #             events.append(event)
        #     
        #     # Create in CDF
        #     if events:
        #         created, duplicates = await self.create_events_with_retry(
        #             events,
        #             link_assets=False
        #         )
        #         result.items_processed = len(created)
        #     
        #     self.logger.info(
        #         "problem_reports_extracted",
        #         reports_found=len(problem_reports),
        #         events_created=result.items_processed
        #     )
        #     
        # except Exception as e:
        #     result.success = False
        #     result.errors.append(f"Problem reports extraction failed: {e}")
        #     self.logger.error("problem_reports_extraction_error", error=str(e))
    
    async def _extract_ncrs_api(self) -> ExtractionResult:
        """Fallback NCR extraction using regular API"""
        result = ExtractionResult(success=True, items_processed=0, duration_ms=0)
        
        try:
            ncrs = await self._fetch_ncrs_api()
            
            # Create NCR events
            events = []
            for ncr in ncrs:
                event = self._create_ncr_event(ncr)
                if event:
                    events.append(event)
            
            # Create in CDF
            if events:
                created, duplicates = await self.create_events_with_retry(
                    events,
                    link_assets=True
                )
                result.items_processed = len(created)
            
        except Exception as e:
            result.success = False
            result.errors.append(f"NCR API extraction failed: {e}")
        
        return result
    
    async def _fetch_ncrs_api(self) -> List[NCReport]:
        """Fetch NCRs via regular API"""
        # No REST API endpoint for NCRs exists
        # Must use Data Source API which is handled in extract_ncrs
        self.logger.info("ncrs_api_skipped", reason="No REST API endpoint for NCRs")
        return []
    
    def _parse_specification(self, data: Dict[str, Any]) -> Optional[Specification]:
        """Parse specification from API response"""
        spec_id = data.get('id') or data.get('specificationId')
        if not spec_id:
            return None
        
        return Specification(
            id=str(spec_id),
            name=data.get('name', ''),
            part_id=data.get('partId'),
            specification_type=data.get('type', 'dimensional'),
            nominal_value=data.get('nominalValue'),
            upper_limit=data.get('upperLimit'),
            lower_limit=data.get('lowerLimit'),
            unit_of_measure=data.get('unitOfMeasure'),
            active=data.get('active', True),
            metadata=data
        )
    
    def _parse_checksheet(self, data: Dict[str, Any]) -> Optional[Checksheet]:
        """Parse checksheet from API response"""
        cs_id = data.get('id') or data.get('checksheetId')
        if not cs_id:
            return None
        
        return Checksheet(
            id=str(cs_id),
            name=data.get('name', ''),
            part_id=data.get('partId'),
            operation_id=data.get('operationId'),
            frequency=data.get('frequency'),
            check_items=data.get('checkItems', []),
            active=data.get('active', True),
            metadata=data
        )
    
    def _parse_ncr(self, data: Dict[str, Any]) -> Optional[NCReport]:
        """Parse NCR from API response"""
        ncr_id = data.get('id') or data.get('ncrId')
        if not ncr_id:
            return None
        
        # Parse status
        status_str = data.get('status', 'open').lower()
        status = NCRStatus.OPEN
        if 'closed' in status_str:
            status = NCRStatus.CLOSED
        elif 'approved' in status_str:
            status = NCRStatus.APPROVED
        elif 'rejected' in status_str:
            status = NCRStatus.REJECTED
        elif 'review' in status_str:
            status = NCRStatus.IN_REVIEW
        
        # Parse dates
        created_date = datetime.now(timezone.utc)
        if data.get('createdDate'):
            try:
                created_date = datetime.fromisoformat(data['createdDate'].replace('Z', '+00:00'))
            except:
                pass
        
        closed_date = None
        if data.get('closedDate'):
            try:
                closed_date = datetime.fromisoformat(data['closedDate'].replace('Z', '+00:00'))
            except:
                pass
        
        return NCReport(
            id=str(ncr_id),
            number=data.get('ncrNumber', str(ncr_id)),
            status=status,
            part_id=data.get('partId'),
            part_name=data.get('partName'),
            quantity_affected=int(data.get('quantityAffected', 0)),
            defect_description=data.get('defectDescription'),
            root_cause=data.get('rootCause'),
            corrective_action=data.get('correctiveAction'),
            created_date=created_date,
            closed_date=closed_date,
            operator=data.get('operator'),
            workcenter_id=data.get('workcenterId'),
            severity=data.get('severity'),
            cost_impact=data.get('costImpact'),
            metadata=data
        )
    
    def _parse_inspection(self, data: Dict[str, Any]) -> Optional[InspectionResult]:
        """Parse inspection from API response"""
        insp_id = data.get('id') or data.get('inspectionId')
        if not insp_id:
            return None
        
        # Parse date
        inspection_date = datetime.now(timezone.utc)
        if data.get('inspectionDate'):
            try:
                inspection_date = datetime.fromisoformat(data['inspectionDate'].replace('Z', '+00:00'))
            except:
                pass
        
        return InspectionResult(
            id=str(insp_id),
            checksheet_id=data.get('checksheetId'),
            part_id=data.get('partId'),
            serial_number=data.get('serialNumber'),
            result=data.get('result', 'unknown'),
            measurements=data.get('measurements', []),
            inspector=data.get('inspector'),
            inspection_date=inspection_date,
            defects_found=data.get('defectsFound', []),
            metadata=data
        )
    
    def _parse_problem_report(self, data: Dict[str, Any]) -> Optional[ProblemReport]:
        """Parse problem report from API response"""
        pr_id = data.get('id') or data.get('problemReportId')
        if not pr_id:
            return None
        
        # Parse dates
        reported_date = datetime.now(timezone.utc)
        if data.get('reportedDate'):
            try:
                reported_date = datetime.fromisoformat(data['reportedDate'].replace('Z', '+00:00'))
            except:
                pass
        
        resolved_date = None
        if data.get('resolvedDate'):
            try:
                resolved_date = datetime.fromisoformat(data['resolvedDate'].replace('Z', '+00:00'))
            except:
                pass
        
        return ProblemReport(
            id=str(pr_id),
            title=data.get('title', ''),
            description=data.get('description', ''),
            status=data.get('status', 'open'),
            priority=data.get('priority', 'medium'),
            reported_date=reported_date,
            reporter=data.get('reporter'),
            assigned_to=data.get('assignedTo'),
            resolution=data.get('resolution'),
            resolved_date=resolved_date,
            metadata=data
        )
    
    def _create_specification_asset(self, spec: Specification) -> Asset:
        """Create specification asset"""
        external_id = self.create_asset_external_id('specification', spec.id)
        
        metadata = {
            **self.naming.get_metadata_tags(),
            'specification_id': spec.id,
            'specification_type': spec.specification_type,
            'active': str(spec.active)
        }
        
        # Add limits
        if spec.nominal_value is not None:
            metadata['nominal_value'] = str(spec.nominal_value)
        if spec.upper_limit is not None:
            metadata['upper_limit'] = str(spec.upper_limit)
        if spec.lower_limit is not None:
            metadata['lower_limit'] = str(spec.lower_limit)
        if spec.unit_of_measure:
            metadata['unit_of_measure'] = spec.unit_of_measure
        
        return Asset(
            external_id=external_id,
            name=spec.name,
            parent_external_id=self.create_asset_external_id('specifications_root', self.config.facility.pcn),
            description=f"Quality specification: {spec.name}",
            metadata=metadata,
            data_set_id=self.get_dataset_id('quality')
        )
    
    def _create_checksheet_asset(self, checksheet: Checksheet) -> Asset:
        """Create checksheet asset"""
        external_id = self.create_asset_external_id('checksheet', checksheet.id)
        
        metadata = {
            **self.naming.get_metadata_tags(),
            'checksheet_id': checksheet.id,
            'frequency': checksheet.frequency or '',
            'check_items_count': str(len(checksheet.check_items)),
            'active': str(checksheet.active)
        }
        
        if checksheet.part_id:
            metadata['part_id'] = checksheet.part_id
        if checksheet.operation_id:
            metadata['operation_id'] = checksheet.operation_id
        
        return Asset(
            external_id=external_id,
            name=checksheet.name,
            parent_external_id=self.create_asset_external_id('checksheets_root', self.config.facility.pcn),
            description=f"Quality checksheet: {checksheet.name}",
            metadata=metadata,
            data_set_id=self.get_dataset_id('quality')
        )
    
    def _create_ncr_event(self, ncr: NCReport) -> Optional[Event]:
        """Create NCR event"""
        external_id = self.create_event_external_id('ncr', ncr.id)
        
        # Skip if already processed
        if external_id in self.processed_ncrs:
            return None
        
        metadata = {
            **self.naming.get_metadata_tags(),
            'ncr_id': ncr.id,
            'ncr_number': ncr.number,
            'status': ncr.status.value,
            'quantity_affected': str(ncr.quantity_affected),
            'source': 'plex_quality'
        }
        
        # Add optional metadata
        if ncr.part_id:
            metadata['part_id'] = ncr.part_id
        if ncr.part_name:
            metadata['part_name'] = ncr.part_name
        if ncr.severity:
            metadata['severity'] = ncr.severity
        if ncr.cost_impact is not None:
            metadata['cost_impact'] = str(ncr.cost_impact)
        if ncr.operator:
            metadata['operator'] = ncr.operator
        if ncr.workcenter_id:
            metadata['workcenter_id'] = ncr.workcenter_id
        
        # Build description
        desc_parts = [f"NCR #{ncr.number}"]
        if ncr.part_name:
            desc_parts.append(ncr.part_name)
        desc_parts.append(f"Qty: {ncr.quantity_affected}")
        desc_parts.append(f"[{ncr.status.value}]")
        
        # Prepare asset links
        asset_external_ids = []
        if ncr.part_id:
            asset_external_ids.append(self.create_asset_external_id('part', ncr.part_id))
        if ncr.workcenter_id:
            asset_external_ids.append(self.create_asset_external_id('workcenter', ncr.workcenter_id))
        
        event = Event(
            external_id=external_id,
            type='quality_ncr',
            subtype=ncr.status.value,
            description=" | ".join(desc_parts),
            start_time=int(ncr.created_date.timestamp() * 1000),
            end_time=int(ncr.closed_date.timestamp() * 1000) if ncr.closed_date else None,
            metadata=metadata,
            data_set_id=self.get_dataset_id('quality')
        )
        
        if asset_external_ids:
            event.asset_external_ids = asset_external_ids
        
        return event
    
    def _create_inspection_event(self, inspection: InspectionResult) -> Optional[Event]:
        """Create inspection event"""
        external_id = self.create_event_external_id('inspection', inspection.id)
        
        # Skip if already processed
        if external_id in self.processed_inspections:
            return None
        
        metadata = {
            **self.naming.get_metadata_tags(),
            'inspection_id': inspection.id,
            'result': inspection.result,
            'measurements_count': str(len(inspection.measurements)),
            'defects_count': str(len(inspection.defects_found)),
            'source': 'plex_quality'
        }
        
        # Add optional metadata
        if inspection.checksheet_id:
            metadata['checksheet_id'] = inspection.checksheet_id
        if inspection.part_id:
            metadata['part_id'] = inspection.part_id
        if inspection.serial_number:
            metadata['serial_number'] = inspection.serial_number
        if inspection.inspector:
            metadata['inspector'] = inspection.inspector
        
        # Build description
        desc_parts = [f"Inspection: {inspection.result.upper()}"]
        if inspection.serial_number:
            desc_parts.append(f"S/N: {inspection.serial_number}")
        if inspection.defects_found:
            desc_parts.append(f"Defects: {len(inspection.defects_found)}")
        
        # Prepare asset links
        asset_external_ids = []
        if inspection.part_id:
            asset_external_ids.append(self.create_asset_external_id('part', inspection.part_id))
        if inspection.checksheet_id:
            asset_external_ids.append(self.create_asset_external_id('checksheet', inspection.checksheet_id))
        
        event = Event(
            external_id=external_id,
            type='quality_inspection',
            subtype=inspection.result,
            description=" | ".join(desc_parts),
            start_time=int(inspection.inspection_date.timestamp() * 1000),
            metadata=metadata,
            data_set_id=self.get_dataset_id('quality')
        )
        
        if asset_external_ids:
            event.asset_external_ids = asset_external_ids
        
        return event
    
    def _create_problem_report_event(self, pr: ProblemReport) -> Event:
        """Create problem report event"""
        external_id = self.create_event_external_id('problem_report', pr.id)
        
        metadata = {
            **self.naming.get_metadata_tags(),
            'problem_report_id': pr.id,
            'status': pr.status,
            'priority': pr.priority,
            'source': 'plex_quality'
        }
        
        # Add optional metadata
        if pr.reporter:
            metadata['reporter'] = pr.reporter
        if pr.assigned_to:
            metadata['assigned_to'] = pr.assigned_to
        if pr.resolution:
            metadata['resolution'] = pr.resolution
        
        # Build description
        desc_parts = [f"Problem: {pr.title}"]
        desc_parts.append(f"Priority: {pr.priority.upper()}")
        desc_parts.append(f"[{pr.status.upper()}]")
        
        event = Event(
            external_id=external_id,
            type='quality_problem',
            subtype=pr.priority,
            description=" | ".join(desc_parts),
            start_time=int(pr.reported_date.timestamp() * 1000),
            end_time=int(pr.resolved_date.timestamp() * 1000) if pr.resolved_date else None,
            metadata=metadata,
            data_set_id=self.get_dataset_id('quality')
        )
        
        return event
    
    async def _create_ncr_metrics(self, ncrs: List[NCReport]) -> None:
        """Create NCR metrics time series"""
        try:
            # Create time series for NCR metrics
            ts_external_id = self.create_asset_external_id('ncr_count', self.config.facility.pcn)
            
            ts = TimeSeries(
                external_id=ts_external_id,
                name=f"NCR Count - {self.config.facility.facility_name}",
                unit='count',
                description="Daily count of NCRs",
                metadata={
                    **self.naming.get_metadata_tags(),
                    'metric_type': 'ncr_count',
                    'source': 'plex_quality'
                },
                data_set_id=self.get_dataset_id('quality')
            )
            
            # Create time series
            await self.async_cdf.create_time_series([ts])
            
            # Calculate daily counts
            today_count = sum(
                1 for ncr in ncrs
                if ncr.created_date.date() == datetime.now(timezone.utc).date()
            )
            
            # Insert datapoint
            if today_count > 0:
                timestamp = int(datetime.now(timezone.utc).timestamp() * 1000)
                dp = Datapoints(
                    external_id=ts_external_id,
                    datapoints=[(timestamp, today_count)]
                )
                
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(
                    None,
                    self.client.time_series.data.insert,
                    dp
                )
                
                self.logger.info("ncr_metrics_created", count=today_count)
                
        except Exception as e:
            self.logger.error("ncr_metrics_error", error=str(e))


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
        extractor = EnhancedQualityExtractor()
        
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