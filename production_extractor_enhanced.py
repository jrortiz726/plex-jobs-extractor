#!/usr/bin/env python3
"""
Enhanced Production Extractor with All Improvements
- Full async/await implementation
- Type hints throughout
- Error handling with retry
- Asset ID resolution
- Structured logging
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any, Tuple, Final, TypeAlias
from dataclasses import dataclass, field

import structlog
from cognite.client.data_classes import Asset, Event, TimeSeries, Datapoints

from base_extractor_enhanced import (
    BaseExtractor, BaseExtractorConfig, ExtractionResult,
    DatasetType, with_retry
)
from error_handling import PlexAPIError

# Setup structured logging
logger = structlog.get_logger(__name__)

# Type aliases
WorkcenterId: TypeAlias = str
PartId: TypeAlias = str
Timestamp: TypeAlias = int


@dataclass
class ProductionEntry:
    """Structured production data"""
    id: str
    workcenter_id: WorkcenterId
    workcenter_name: str
    part_id: Optional[PartId] = None
    part_name: Optional[str] = None
    quantity_produced: int = 0
    quantity_scrapped: int = 0
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    operator_id: Optional[str] = None
    shift: Optional[str] = None
    job_id: Optional[str] = None
    cycle_time: Optional[float] = None
    oee: Optional[float] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass 
class WorkcenterStatus:
    """Workcenter status data"""
    workcenter_id: WorkcenterId
    workcenter_code: str  # Human-readable code like "NFF-01"
    workcenter_name: str  # Human-readable name like "Liquid Nitrogen Flash Freezer"
    status: str  # running, idle, down, maintenance
    status_reason: Optional[str] = None
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    current_job: Optional[str] = None
    operator: Optional[str] = None
    production_rate: Optional[float] = None
    efficiency: Optional[float] = None


class ProductionExtractorConfig(BaseExtractorConfig):
    """Configuration specific to production extractor"""
    extract_workcenter_status: bool = True
    extract_production_entries: bool = True
    extract_oee_metrics: bool = True
    lookback_hours: int = 24
    
    @classmethod
    def from_env(cls) -> ProductionExtractorConfig:
        """Load configuration from environment"""
        import os
        base = BaseExtractorConfig.from_env('production')
        return cls(
            **base.dict(),
            extract_workcenter_status=os.getenv('EXTRACT_WORKCENTER_STATUS', 'true').lower() == 'true',
            extract_production_entries=os.getenv('EXTRACT_PRODUCTION_ENTRIES', 'true').lower() == 'true',
            extract_oee_metrics=os.getenv('EXTRACT_OEE_METRICS', 'true').lower() == 'true',
            lookback_hours=int(os.getenv('PRODUCTION_LOOKBACK_HOURS', '24'))
        )


class EnhancedProductionExtractor(BaseExtractor):
    """Enhanced production extractor with all improvements"""
    
    def __init__(self, config: Optional[ProductionExtractorConfig] = None):
        """Initialize with enhanced configuration"""
        config = config or ProductionExtractorConfig.from_env()
        super().__init__(config, 'production')
        
        self.config: Final[ProductionExtractorConfig] = config
        self.processed_entries: set[str] = set()
        self.workcenter_assets: Dict[WorkcenterId, str] = {}  # Cache workcenter assets
        
        self.logger.info(
            "production_extractor_initialized",
            extract_status=config.extract_workcenter_status,
            extract_entries=config.extract_production_entries,
            extract_oee=config.extract_oee_metrics,
            lookback_hours=config.lookback_hours
        )
    
    def get_required_datasets(self) -> List[str]:
        """Production requires production and master datasets"""
        return ['production', 'master']
    
    async def extract(self) -> ExtractionResult:
        """Main extraction with concurrent operations"""
        start_time = datetime.now(timezone.utc)
        result = ExtractionResult(
            success=True,
            items_processed=0,
            duration_ms=0
        )
        
        try:
            tasks = []
            
            # Create extraction tasks based on configuration
            async with asyncio.TaskGroup() as tg:
                if self.config.extract_workcenter_status:
                    tasks.append(tg.create_task(self._extract_workcenter_status()))
                
                if self.config.extract_production_entries:
                    tasks.append(tg.create_task(self._extract_production_entries()))
                
                if self.config.extract_oee_metrics:
                    tasks.append(tg.create_task(self._extract_oee_metrics()))
            
            # Aggregate results
            for task in tasks:
                task_result = await task
                result.items_processed += task_result.items_processed
                if not task_result.success:
                    result.success = False
                    result.errors.extend(task_result.errors)
            
            self.logger.info(
                "production_extraction_completed",
                items_processed=result.items_processed,
                success=result.success
            )
            
        except Exception as e:
            result.success = False
            result.errors.append(str(e))
            self.logger.error("production_extraction_failed", error=str(e), exc_info=True)
        
        result.duration_ms = (datetime.now(timezone.utc) - start_time).total_seconds() * 1000
        return result
    
    @with_retry(max_attempts=3)
    async def _extract_workcenter_status(self) -> ExtractionResult:
        """Extract workcenter status data"""
        result = ExtractionResult(success=True, items_processed=0, duration_ms=0)
        
        try:
            # Fetch all workcenters first
            workcenters = await self._fetch_workcenters()
            
            if not workcenters:
                self.logger.warning("no_workcenters_found")
                return result
            
            # Fetch status for each workcenter concurrently
            status_tasks = []
            for wc_id in workcenters:
                status_tasks.append(self._fetch_workcenter_status(wc_id))
            
            statuses = await asyncio.gather(*status_tasks, return_exceptions=True)
            
            # Process status data
            assets_to_update = []
            events_to_create = []
            
            for status in statuses:
                if isinstance(status, Exception):
                    self.logger.warning("workcenter_status_fetch_error", error=str(status))
                    continue
                
                if status:
                    # Update workcenter asset
                    asset = await self._create_workcenter_asset(status)
                    if asset:
                        assets_to_update.append(asset)
                    
                    # Create status event
                    event = self._create_status_event(status)
                    if event:
                        events_to_create.append(event)
            
            # Create/update in CDF
            if assets_to_update:
                created_assets, failed_assets = await self.create_assets_with_retry(
                    assets_to_update,
                    resolve_parents=True
                )
                result.items_processed += len(created_assets)
            
            if events_to_create:
                created_events, duplicate_events = await self.create_events_with_retry(
                    events_to_create,
                    link_assets=True
                )
                result.items_processed += len(created_events)
            
            self.logger.info(
                "workcenter_status_extracted",
                workcenters=len(workcenters),
                assets_updated=len(assets_to_update),
                events_created=len(events_to_create)
            )
            
        except Exception as e:
            result.success = False
            result.errors.append(f"Workcenter status extraction failed: {e}")
            self.logger.error("workcenter_status_extraction_error", error=str(e))
        
        return result
    
    @with_retry(max_attempts=3)
    async def _extract_production_entries(self) -> ExtractionResult:
        """Extract production entry data"""
        result = ExtractionResult(success=True, items_processed=0, duration_ms=0)
        
        try:
            # Calculate time range
            end_time = datetime.now(timezone.utc)
            start_time = end_time - timedelta(hours=self.config.lookback_hours)
            
            # Fetch production entries
            entries = await self._fetch_production_entries(start_time, end_time)
            
            if not entries:
                self.logger.info("no_production_entries_found")
                return result
            
            # Convert to events
            events = []
            for entry in entries:
                event = self._create_production_event(entry)
                if event:
                    events.append(event)
            
            # Create events in CDF
            if events:
                created, duplicates = await self.create_events_with_retry(
                    events,
                    link_assets=True
                )
                result.items_processed = len(created)
                
                # Update processed set
                self.processed_entries.update(created)
            
            self.logger.info(
                "production_entries_extracted",
                entries_found=len(entries),
                events_created=result.items_processed
            )
            
        except Exception as e:
            result.success = False
            result.errors.append(f"Production entries extraction failed: {e}")
            self.logger.error("production_entries_extraction_error", error=str(e))
        
        return result
    
    @with_retry(max_attempts=3)
    async def _extract_oee_metrics(self) -> ExtractionResult:
        """Extract OEE metrics as time series"""
        result = ExtractionResult(success=True, items_processed=0, duration_ms=0)
        
        try:
            # Fetch OEE data for all workcenters
            workcenters = await self._fetch_workcenters()
            
            # Fetch OEE metrics concurrently
            oee_tasks = []
            for wc_id in workcenters:
                oee_tasks.append(self._fetch_oee_metrics(wc_id))
            
            oee_data = await asyncio.gather(*oee_tasks, return_exceptions=True)
            
            # Create time series and datapoints
            timeseries_to_create = []
            datapoints_to_insert = {}
            
            for wc_id, metrics in zip(workcenters, oee_data):
                if isinstance(metrics, Exception):
                    continue
                
                if metrics:
                    # Create time series for OEE components
                    ts_availability = self._create_oee_timeseries(wc_id, 'availability')
                    ts_performance = self._create_oee_timeseries(wc_id, 'performance')
                    ts_quality = self._create_oee_timeseries(wc_id, 'quality')
                    ts_overall = self._create_oee_timeseries(wc_id, 'overall')
                    
                    timeseries_to_create.extend([ts_availability, ts_performance, ts_quality, ts_overall])
                    
                    # Prepare datapoints
                    if 'availability' in metrics:
                        datapoints_to_insert[ts_availability.external_id] = metrics['availability']
                    if 'performance' in metrics:
                        datapoints_to_insert[ts_performance.external_id] = metrics['performance']
                    if 'quality' in metrics:
                        datapoints_to_insert[ts_quality.external_id] = metrics['quality']
                    if 'overall' in metrics:
                        datapoints_to_insert[ts_overall.external_id] = metrics['overall']
            
            # Create time series in CDF
            if timeseries_to_create:
                await self._create_timeseries_batch(timeseries_to_create)
                result.items_processed += len(timeseries_to_create)
            
            # Insert datapoints
            if datapoints_to_insert:
                await self._insert_datapoints_batch(datapoints_to_insert)
            
            self.logger.info(
                "oee_metrics_extracted",
                workcenters=len(workcenters),
                timeseries_created=len(timeseries_to_create),
                datapoints_inserted=len(datapoints_to_insert)
            )
            
        except Exception as e:
            result.success = False
            result.errors.append(f"OEE metrics extraction failed: {e}")
            self.logger.error("oee_metrics_extraction_error", error=str(e))
        
        return result
    
    async def _fetch_workcenters(self) -> List[WorkcenterId]:
        """Fetch all workcenters for the facility"""
        endpoint = "/production/v1/production-definitions/workcenters"
        
        try:
            data = await self.fetch_plex_data(endpoint)
            
            workcenters = []
            for wc in data if isinstance(data, list) else data.get('data', []):
                wc_id = wc.get('id') or wc.get('workcenterId')
                if wc_id:
                    workcenters.append(str(wc_id))
                    # Cache workcenter info
                    self.workcenter_assets[str(wc_id)] = self.create_asset_external_id('workcenter', str(wc_id))
            
            return workcenters
            
        except Exception as e:
            self.logger.error("fetch_workcenters_error", error=str(e))
            return []
    
    async def _fetch_workcenter_status(self, workcenter_id: WorkcenterId) -> Optional[WorkcenterStatus]:
        """Fetch current status for a specific workcenter"""
        endpoint = f"/production/v1/control/workcenters/{workcenter_id}"
        
        try:
            data = await self.fetch_plex_data(endpoint)
            
            return WorkcenterStatus(
                workcenter_id=workcenter_id,
                workcenter_code=data.get('code', ''),
                workcenter_name=data.get('name', ''),
                status=data.get('statusDescription', 'unknown'),
                status_reason=data.get('statusId'),
                timestamp=datetime.now(timezone.utc),
                current_job=data.get('jobNumber'),
                operator=','.join(data.get('operators', [])) if data.get('operators') else None,
                production_rate=data.get('productionRate'),
                efficiency=data.get('efficiency')
            )
            
        except Exception as e:
            self.logger.warning(
                "fetch_workcenter_status_error",
                workcenter_id=workcenter_id,
                error=str(e)
            )
            return None
    
    async def _fetch_production_entries(
        self,
        start_time: datetime,
        end_time: datetime
    ) -> List[ProductionEntry]:
        """Fetch production entries in time range"""
        endpoint = "/production/v1/production-history/production-entries"
        
        params = {
            'beginDate': start_time.isoformat(),
            'endDate': end_time.isoformat(),
            'limit': 1000
        }
        
        all_entries = []
        offset = 0
        
        while True:
            params['offset'] = offset
            
            try:
                data = await self.fetch_plex_data(endpoint, params)
                
                if not data:
                    break
                
                entries_raw = data if isinstance(data, list) else data.get('data', [])
                
                for entry_raw in entries_raw:
                    entry = self._parse_production_entry(entry_raw)
                    if entry:
                        all_entries.append(entry)
                
                if len(entries_raw) < 1000:
                    break
                
                offset += len(entries_raw)
                
            except Exception as e:
                self.logger.error("fetch_production_entries_error", error=str(e))
                break
        
        return all_entries
    
    def _parse_production_entry(self, data: Dict[str, Any]) -> Optional[ProductionEntry]:
        """Parse production entry from API response"""
        entry_id = data.get('id') or data.get('entryId')
        if not entry_id:
            return None
        
        # Parse timestamp
        timestamp_str = data.get('timestamp') or data.get('createdAt')
        timestamp = datetime.now(timezone.utc)
        if timestamp_str:
            try:
                timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            except:
                pass
        
        return ProductionEntry(
            id=str(entry_id),
            workcenter_id=str(data.get('workcenterId', '')),
            workcenter_name=data.get('workcenterName', ''),
            part_id=data.get('partId'),
            part_name=data.get('partName'),
            quantity_produced=int(data.get('quantityProduced', 0)),
            quantity_scrapped=int(data.get('quantityScrapped', 0)),
            timestamp=timestamp,
            operator_id=data.get('operatorId'),
            shift=data.get('shift'),
            job_id=data.get('jobId'),
            cycle_time=data.get('cycleTime'),
            oee=data.get('oee'),
            metadata=data
        )
    
    async def _fetch_oee_metrics(self, workcenter_id: WorkcenterId) -> Optional[Dict[str, float]]:
        """Fetch OEE metrics for a workcenter"""
        # OEE metrics are now handled by the separate performance_extractor_enhanced.py
        # which uses the Plex Data Source API (IDs 18765 and 22870)
        # This keeps production extractor focused on REST API only
        return None
    
    async def _create_workcenter_asset(self, status: WorkcenterStatus) -> Optional[Asset]:
        """Create or update workcenter asset"""
        external_id = self.create_asset_external_id('workcenter', status.workcenter_id)
        
        metadata = {
            **self.naming.get_metadata_tags(),
            'workcenter_id': status.workcenter_id,
            'workcenter_code': status.workcenter_code,
            'workcenter_name': status.workcenter_name,
            'current_status': status.status,
            'last_updated': datetime.now(timezone.utc).isoformat()
        }
        
        if status.current_job:
            metadata['current_job'] = status.current_job
        if status.operator:
            metadata['current_operator'] = status.operator
        if status.efficiency is not None:
            metadata['efficiency'] = str(status.efficiency)
        
        # Use human-readable name and code
        asset_name = f"{status.workcenter_code} - {status.workcenter_name}" if status.workcenter_code and status.workcenter_name else f"Workcenter {status.workcenter_id}"
        
        return Asset(
            external_id=external_id,
            name=asset_name,
            parent_external_id=self.create_asset_external_id('facility', self.config.facility.pcn),
            metadata=metadata,
            description=f"{status.workcenter_name} (Code: {status.workcenter_code})" if status.workcenter_name else f"Production workcenter {status.workcenter_id}",
            data_set_id=self.get_dataset_id('production')
        )
    
    def _create_status_event(self, status: WorkcenterStatus) -> Optional[Event]:
        """Create workcenter status event"""
        external_id = self.create_event_external_id(
            'wc_status',
            f"{status.workcenter_id}_{int(status.timestamp.timestamp())}"
        )
        
        metadata = {
            **self.naming.get_metadata_tags(),
            'workcenter_id': status.workcenter_id,
            'status': status.status,
            'source': 'plex_production'
        }
        
        if status.status_reason:
            metadata['status_reason'] = status.status_reason
        if status.production_rate is not None:
            metadata['production_rate'] = str(status.production_rate)
        
        # Prepare asset link
        wc_asset_external_id = self.create_asset_external_id('workcenter', status.workcenter_id)
        
        event = Event(
            external_id=external_id,
            type='workcenter_status',
            subtype=status.status,
            description=f"Workcenter {status.workcenter_id} - {status.status}",
            start_time=int(status.timestamp.timestamp() * 1000),
            metadata=metadata,
            data_set_id=self.get_dataset_id('production')
        )
        
        # Add asset link for resolution
        event.asset_external_ids = [wc_asset_external_id]
        
        return event
    
    def _create_production_event(self, entry: ProductionEntry) -> Optional[Event]:
        """Create production entry event"""
        external_id = self.create_event_external_id('production', entry.id)
        
        # Skip if already processed
        if external_id in self.processed_entries:
            return None
        
        metadata = {
            **self.naming.get_metadata_tags(),
            'entry_id': entry.id,
            'workcenter_id': entry.workcenter_id,
            'quantity_produced': str(entry.quantity_produced),
            'quantity_scrapped': str(entry.quantity_scrapped),
            'source': 'plex_production'
        }
        
        # Add optional metadata
        if entry.part_id:
            metadata['part_id'] = entry.part_id
        if entry.part_name:
            metadata['part_name'] = entry.part_name
        if entry.operator_id:
            metadata['operator_id'] = entry.operator_id
        if entry.shift:
            metadata['shift'] = entry.shift
        if entry.job_id:
            metadata['job_id'] = entry.job_id
        if entry.cycle_time is not None:
            metadata['cycle_time'] = str(entry.cycle_time)
        if entry.oee is not None:
            metadata['oee'] = str(entry.oee)
        
        # Build description
        desc_parts = [f"Production: {entry.quantity_produced} units"]
        if entry.part_name:
            desc_parts.append(f"Part: {entry.part_name}")
        if entry.workcenter_name:
            desc_parts.append(f"WC: {entry.workcenter_name}")
        
        # Prepare asset links
        asset_external_ids = []
        asset_external_ids.append(self.create_asset_external_id('workcenter', entry.workcenter_id))
        if entry.part_id:
            asset_external_ids.append(self.create_asset_external_id('part', entry.part_id))
        
        event = Event(
            external_id=external_id,
            type='production_entry',
            subtype='production',
            description=" | ".join(desc_parts),
            start_time=int(entry.timestamp.timestamp() * 1000),
            metadata=metadata,
            data_set_id=self.get_dataset_id('production')
        )
        
        # Add asset links for resolution
        event.asset_external_ids = asset_external_ids
        
        return event
    
    def _create_oee_timeseries(self, workcenter_id: WorkcenterId, metric: str) -> TimeSeries:
        """Create OEE time series"""
        external_id = self.create_asset_external_id(f'oee_{metric}', workcenter_id)
        
        # Get workcenter asset for linking
        wc_asset_external_id = self.create_asset_external_id('workcenter', workcenter_id)
        wc_asset_id = self.id_resolver.resolve_single(wc_asset_external_id)
        
        return TimeSeries(
            external_id=external_id,
            name=f"OEE {metric.title()} - WC {workcenter_id}",
            unit='%' if metric != 'overall' else 'OEE',
            asset_id=wc_asset_id,
            description=f"OEE {metric} metric for workcenter {workcenter_id}",
            metadata={
                **self.naming.get_metadata_tags(),
                'workcenter_id': workcenter_id,
                'metric_type': metric,
                'source': 'plex_production'
            },
            data_set_id=self.get_dataset_id('production')
        )
    
    async def _create_timeseries_batch(self, timeseries: List[TimeSeries]) -> None:
        """Create time series in CDF"""
        try:
            # Check existing
            external_ids = [ts.external_id for ts in timeseries]
            existing = await self.dedup_helper.get_existing_timeseries(external_ids)
            
            new_ts = [ts for ts in timeseries if ts.external_id not in existing]
            
            if new_ts:
                await self.async_cdf.create_time_series(new_ts)
                self.logger.info(
                    "timeseries_created",
                    count=len(new_ts)
                )
            
        except Exception as e:
            self.logger.error("timeseries_creation_error", error=str(e))
    
    async def _insert_datapoints_batch(self, datapoints: Dict[str, float]) -> None:
        """Insert datapoints to time series"""
        try:
            # Prepare datapoints list
            dp_list = []
            timestamp = int(datetime.now(timezone.utc).timestamp() * 1000)
            
            for external_id, value in datapoints.items():
                dp = Datapoints(
                    external_id=external_id,
                    datapoints=[(timestamp, value)]
                )
                dp_list.append(dp)
            
            # Insert via CDF client
            if dp_list:
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(
                    None,
                    self.client.time_series.data.insert_multiple,
                    dp_list
                )
                
                self.logger.info(
                    "datapoints_inserted",
                    count=len(dp_list)
                )
                
        except Exception as e:
            self.logger.error("datapoints_insertion_error", error=str(e))


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
        extractor = EnhancedProductionExtractor()
        
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