#!/usr/bin/env python3
"""
Enhanced Performance/OEE Extractor for Plex to CDF

This extractor handles all performance metrics using the Plex Data Source API,
including OEE (Overall Equipment Effectiveness) calculations.
"""

import os
import asyncio
import base64
import aiohttp
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass, field

from cognite.client.data_classes import Asset, Event, TimeSeries, TimeSeriesWrite
from cognite.client.data_classes.data_modeling import EdgeApply

from base_extractor_enhanced import EnhancedBaseExtractor, ExtractorConfig, ExtractionResult
from error_handling import with_retry, PlexAPIError
import structlog

logger = structlog.get_logger(__name__)


# Type aliases
WorkcenterId = str
PartId = str


@dataclass
class PerformanceConfig(ExtractorConfig):
    """Configuration for Performance/OEE extractor"""
    datasource_username: Optional[str] = None
    datasource_password: Optional[str] = None
    pcn_code: str = 'ra-process'
    use_test: bool = False
    extract_daily_performance: bool = True
    extract_realtime_performance: bool = True
    performance_lookback_days: int = 7
    
    @classmethod
    def from_env(cls) -> 'PerformanceConfig':
        """Load configuration from environment"""
        base = ExtractorConfig.from_env('performance')
        
        return cls(
            **base.dict(),
            datasource_username=os.getenv('PLEX_DS_USERNAME'),
            datasource_password=os.getenv('PLEX_DS_PASSWORD'),
            pcn_code=os.getenv('PLEX_PCN_CODE', 'ra-process'),
            use_test=os.getenv('PLEX_USE_TEST', 'false').lower() == 'true',
            extract_daily_performance=os.getenv('EXTRACT_DAILY_PERFORMANCE', 'true').lower() == 'true',
            extract_realtime_performance=os.getenv('EXTRACT_REALTIME_PERFORMANCE', 'true').lower() == 'true',
            performance_lookback_days=int(os.getenv('PERFORMANCE_LOOKBACK_DAYS', '7'))
        )


@dataclass
class WorkcenterPerformance:
    """Workcenter performance metrics"""
    workcenter_id: WorkcenterId
    workcenter_code: str
    timestamp: datetime
    availability: float
    performance: float
    quality: float
    oee: float
    production_qty: Optional[int] = None
    reject_qty: Optional[int] = None
    scrap_qty: Optional[int] = None
    downtime_minutes: Optional[float] = None
    planned_production_time: Optional[float] = None
    part_no: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


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
    
    async def execute_datasource(
        self,
        datasource_id: int,
        inputs: Optional[Dict[str, Any]] = None,
        format_type: int = 2
    ) -> List[Dict[str, Any]]:
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
                if response.status == 200:
                    data = await response.json()
                    return data if isinstance(data, list) else []
                elif response.status in [401, 403]:
                    raise PlexAPIError(f"DataSource API authentication failed: {response.status}")
                else:
                    text = await response.text()
                    raise PlexAPIError(f"DataSource API error {response.status}: {text[:200]}")
                    
        except aiohttp.ClientError as e:
            raise PlexAPIError(f"DataSource API connection error: {e}")


class EnhancedPerformanceExtractor(EnhancedBaseExtractor):
    """Enhanced extractor for performance and OEE metrics"""
    
    def __init__(self, config: PerformanceConfig):
        super().__init__(config)
        self.config = config
        
        # Initialize Data Source API client
        if config.datasource_username and config.datasource_password:
            self.ds_client = DataSourceAPIClient(
                username=config.datasource_username,
                password=config.datasource_password,
                pcn_code=config.pcn_code,
                use_test=config.use_test
            )
        else:
            self.ds_client = None
            logger.warning("performance_extractor_no_credentials",
                         message="Data Source API credentials not configured")
        
        self.logger.info(
            "performance_extractor_initialized",
            extract_daily=config.extract_daily_performance,
            extract_realtime=config.extract_realtime_performance,
            lookback_days=config.performance_lookback_days
        )
    
    async def extract(self) -> ExtractionResult:
        """Main extraction method"""
        if not self.ds_client:
            return ExtractionResult(
                success=False,
                items_processed=0,
                duration_ms=0,
                errors=["Data Source API credentials not configured"]
            )
        
        results = []
        
        async with self.ds_client:
            # Extract daily performance
            if self.config.extract_daily_performance:
                daily_result = await self._extract_daily_performance()
                results.append(daily_result)
            
            # Extract real-time performance
            if self.config.extract_realtime_performance:
                realtime_result = await self._extract_realtime_performance()
                results.append(realtime_result)
        
        # Aggregate results
        total_result = ExtractionResult(
            success=all(r.success for r in results),
            items_processed=sum(r.items_processed for r in results),
            duration_ms=sum(r.duration_ms for r in results),
            errors=[e for r in results for e in r.errors]
        )
        
        self.logger.info(
            "performance_extraction_completed",
            items_processed=total_result.items_processed,
            success=total_result.success
        )
        
        return total_result
    
    @with_retry(max_attempts=3)
    async def _extract_daily_performance(self) -> ExtractionResult:
        """Extract daily performance using Data Source ID 18765"""
        result = ExtractionResult(success=True, items_processed=0, duration_ms=0)
        start_time = datetime.now(timezone.utc)
        
        try:
            # Prepare date range
            end_date = datetime.now(timezone.utc)
            start_date = end_date - timedelta(days=self.config.performance_lookback_days)
            
            # Data Source ID 18765: Daily_Performance_Report_Get
            datasource_id = 18765
            inputs = {
                "Start_Date": start_date.strftime("%Y-%m-%d"),
                "End_Date": end_date.strftime("%Y-%m-%d")
                # Can add more filters like Workcenter_Code if needed
            }
            
            # Execute data source
            data = await self.ds_client.execute_datasource(datasource_id, inputs)
            
            if not data:
                self.logger.info("no_daily_performance_data")
                return result
            
            # Process performance data
            performance_metrics = []
            for row in data:
                metric = self._parse_daily_performance(row)
                if metric:
                    performance_metrics.append(metric)
            
            # Create time series and events
            await self._create_performance_timeseries(performance_metrics)
            
            result.items_processed = len(performance_metrics)
            result.duration_ms = int((datetime.now(timezone.utc) - start_time).total_seconds() * 1000)
            
            self.logger.info(
                "daily_performance_extracted",
                count=len(performance_metrics)
            )
            
        except Exception as e:
            result.success = False
            result.errors.append(f"Daily performance extraction failed: {e}")
            self.logger.error("daily_performance_error", error=str(e))
        
        return result
    
    @with_retry(max_attempts=3)
    async def _extract_realtime_performance(self) -> ExtractionResult:
        """Extract real-time performance using Data Source ID 22870"""
        result = ExtractionResult(success=True, items_processed=0, duration_ms=0)
        start_time = datetime.now(timezone.utc)
        
        try:
            # First get list of workcenters
            workcenters = await self._get_workcenter_list()
            
            if not workcenters:
                self.logger.info("no_workcenters_found")
                return result
            
            # Data Source ID 22870: Workcenter_Performance_Simple_Get
            datasource_id = 22870
            performance_metrics = []
            
            for wc in workcenters:
                try:
                    inputs = {
                        "Workcenter_Key": wc.get('key') or wc.get('id')
                    }
                    
                    data = await self.ds_client.execute_datasource(datasource_id, inputs)
                    
                    if data and len(data) > 0:
                        metric = self._parse_realtime_performance(data[0], wc)
                        if metric:
                            performance_metrics.append(metric)
                            
                except Exception as e:
                    self.logger.warning(
                        "workcenter_performance_error",
                        workcenter=wc.get('code'),
                        error=str(e)
                    )
            
            # Create time series and events
            await self._create_performance_timeseries(performance_metrics)
            
            result.items_processed = len(performance_metrics)
            result.duration_ms = int((datetime.now(timezone.utc) - start_time).total_seconds() * 1000)
            
            self.logger.info(
                "realtime_performance_extracted",
                count=len(performance_metrics)
            )
            
        except Exception as e:
            result.success = False
            result.errors.append(f"Real-time performance extraction failed: {e}")
            self.logger.error("realtime_performance_error", error=str(e))
        
        return result
    
    async def _get_workcenter_list(self) -> List[Dict[str, Any]]:
        """Get list of workcenters from production definitions"""
        try:
            endpoint = "/production/v1/production-definitions/workcenters"
            data = await self.fetch_plex_data(endpoint)
            
            workcenters = []
            for wc_data in data if isinstance(data, list) else data.get('data', []):
                workcenters.append(wc_data)
            
            return workcenters
            
        except Exception as e:
            self.logger.error("fetch_workcenters_error", error=str(e))
            return []
    
    def _parse_daily_performance(self, data: Dict[str, Any]) -> Optional[WorkcenterPerformance]:
        """Parse daily performance data"""
        try:
            # Calculate OEE components
            production_qty = int(data.get('Production_Qty', 0))
            reject_qty = int(data.get('Reject_Qty', 0))
            scrap_qty = int(data.get('Scrap_Qty', 0))
            
            # Quality = (Good Parts / Total Parts) * 100
            total_parts = production_qty
            good_parts = production_qty - reject_qty - scrap_qty
            quality = (good_parts / total_parts * 100) if total_parts > 0 else 0
            
            # Performance from data or calculate
            performance = float(data.get('Performance', 100))
            
            # Availability (simplified - would need more data for accurate calc)
            availability = 100.0  # Default
            
            # OEE = Availability * Performance * Quality / 10000
            oee = (availability * performance * quality) / 10000
            
            return WorkcenterPerformance(
                workcenter_id=str(data.get('Workcenter_Key')),
                workcenter_code=data.get('Workcenter_Code', ''),
                timestamp=datetime.now(timezone.utc),
                availability=availability,
                performance=performance,
                quality=quality,
                oee=oee,
                production_qty=production_qty,
                reject_qty=reject_qty,
                scrap_qty=scrap_qty,
                downtime_minutes=float(data.get('Downtime_DB', 0)),
                planned_production_time=float(data.get('Planned_Production_Time', 0)),
                part_no=data.get('Part_No'),
                metadata=data
            )
            
        except Exception as e:
            logger.warning("parse_daily_performance_error", error=str(e))
            return None
    
    def _parse_realtime_performance(
        self,
        data: Dict[str, Any],
        workcenter: Dict[str, Any]
    ) -> Optional[WorkcenterPerformance]:
        """Parse real-time performance data"""
        try:
            performance = float(data.get('Performance', 0))
            
            return WorkcenterPerformance(
                workcenter_id=str(workcenter.get('id')),
                workcenter_code=data.get('Workcenter_Code', workcenter.get('code', '')),
                timestamp=datetime.now(timezone.utc),
                availability=100.0,  # Default
                performance=performance,
                quality=100.0,  # Default
                oee=performance,  # Simplified
                part_no=data.get('Part_No_Revision'),
                metadata=data
            )
            
        except Exception as e:
            logger.warning("parse_realtime_performance_error", error=str(e))
            return None
    
    async def _create_performance_timeseries(
        self,
        metrics: List[WorkcenterPerformance]
    ) -> None:
        """Create time series for performance metrics"""
        if not metrics:
            return
        
        # Group by workcenter
        by_workcenter = {}
        for metric in metrics:
            wc_id = metric.workcenter_id
            if wc_id not in by_workcenter:
                by_workcenter[wc_id] = []
            by_workcenter[wc_id].append(metric)
        
        # Create time series and datapoints
        timeseries_to_create = []
        datapoints_to_insert = {}
        
        for wc_id, wc_metrics in by_workcenter.items():
            # Time series external IDs
            oee_ts_id = self.create_asset_external_id('ts_oee', wc_id)
            availability_ts_id = self.create_asset_external_id('ts_availability', wc_id)
            performance_ts_id = self.create_asset_external_id('ts_performance', wc_id)
            quality_ts_id = self.create_asset_external_id('ts_quality', wc_id)
            
            # Create time series if needed
            for ts_id, name, unit in [
                (oee_ts_id, f"OEE - {wc_metrics[0].workcenter_code}", "%"),
                (availability_ts_id, f"Availability - {wc_metrics[0].workcenter_code}", "%"),
                (performance_ts_id, f"Performance - {wc_metrics[0].workcenter_code}", "%"),
                (quality_ts_id, f"Quality - {wc_metrics[0].workcenter_code}", "%")
            ]:
                ts = TimeSeries(
                    external_id=ts_id,
                    name=name,
                    unit=unit,
                    data_set_id=self.get_dataset_id('production'),
                    metadata={
                        'workcenter_id': wc_id,
                        'workcenter_code': wc_metrics[0].workcenter_code,
                        'type': 'performance_metric'
                    }
                )
                timeseries_to_create.append(ts)
            
            # Prepare datapoints
            timestamps = []
            oee_values = []
            availability_values = []
            performance_values = []
            quality_values = []
            
            for metric in wc_metrics:
                timestamp = int(metric.timestamp.timestamp() * 1000)
                timestamps.append(timestamp)
                oee_values.append(metric.oee)
                availability_values.append(metric.availability)
                performance_values.append(metric.performance)
                quality_values.append(metric.quality)
            
            datapoints_to_insert[oee_ts_id] = (timestamps, oee_values)
            datapoints_to_insert[availability_ts_id] = (timestamps, availability_values)
            datapoints_to_insert[performance_ts_id] = (timestamps, performance_values)
            datapoints_to_insert[quality_ts_id] = (timestamps, quality_values)
        
        # Create time series in CDF
        if timeseries_to_create:
            try:
                await self.async_cdf.upsert_time_series(timeseries_to_create)
                self.logger.info(
                    "performance_timeseries_created",
                    count=len(timeseries_to_create)
                )
            except Exception as e:
                self.logger.error("timeseries_creation_error", error=str(e))
        
        # Insert datapoints
        if datapoints_to_insert:
            try:
                datapoints_list = []
                for ts_id, (timestamps, values) in datapoints_to_insert.items():
                    if timestamps and values:
                        dp = TimeSeriesWrite(
                            external_id=ts_id,
                            datapoints=list(zip(timestamps, values))
                        )
                        datapoints_list.append(dp)
                
                if datapoints_list:
                    await self.async_cdf.insert_datapoints(datapoints_list)
                    self.logger.info(
                        "performance_datapoints_inserted",
                        timeseries_count=len(datapoints_list)
                    )
                    
            except Exception as e:
                self.logger.error("datapoints_insertion_error", error=str(e))


async def main():
    """Main entry point for standalone execution"""
    config = PerformanceConfig.from_env()
    extractor = EnhancedPerformanceExtractor(config)
    
    if config.run_once:
        await extractor.run_extraction_cycle()
    else:
        await extractor.run_continuous()


if __name__ == "__main__":
    asyncio.run(main())