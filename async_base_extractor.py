#!/usr/bin/env python3
"""
Enhanced Async Base Extractor with Proper Async/Await Patterns
Implements concurrent operations, async context managers, and efficient resource management
"""

import asyncio
import aiohttp
import logging
from typing import Dict, List, Optional, Any, TypeVar, Generic, AsyncIterator, Callable, Awaitable
from datetime import datetime, timezone, timedelta
from contextlib import asynccontextmanager
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from enum import StrEnum

from cognite.client import CogniteClient
from cognite.client.data_classes import Asset, AssetList, Event, EventList, TimeSeries, TimeSeriesList
from cognite.client.data_classes.data_modeling import Space, ViewList, NodeList
import httpx

from error_handling import RetryHandler, RetryConfig, with_retry, handle_api_response, PlexAPIError

logger = logging.getLogger(__name__)

T = TypeVar('T')


class DatasetType(StrEnum):
    """Type-safe dataset enumeration"""
    MASTER = "master"
    PRODUCTION = "production"
    SCHEDULING = "scheduling"
    QUALITY = "quality"
    INVENTORY = "inventory"
    MAINTENANCE = "maintenance"


@dataclass
class ExtractionResult:
    """Result of an extraction operation"""
    success: bool
    items_processed: int
    duration_ms: float
    errors: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


class AsyncCDFClient:
    """Async wrapper for CogniteClient operations using thread pool"""
    
    def __init__(self, sync_client: CogniteClient, max_workers: int = 10):
        self._client = sync_client
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
    
    async def create_assets(self, assets: List[Asset]) -> AssetList:
        """Async asset creation"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self._executor,
            self._client.assets.create,
            assets
        )
    
    async def upsert_assets(self, assets: List[Asset]) -> AssetList:
        """Async asset upsert"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self._executor,
            self._client.assets.upsert,
            assets
        )
    
    async def create_events(self, events: List[Event]) -> EventList:
        """Async event creation"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self._executor,
            self._client.events.create,
            events
        )
    
    async def create_time_series(self, time_series: List[TimeSeries]) -> TimeSeriesList:
        """Async time series creation"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self._executor,
            self._client.time_series.create,
            time_series
        )
    
    async def retrieve_assets(self, external_ids: List[str]) -> AssetList:
        """Async asset retrieval"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self._executor,
            self._client.assets.retrieve_multiple,
            external_ids,
            True  # ignore_unknown_ids
        )
    
    def cleanup(self):
        """Cleanup thread pool"""
        self._executor.shutdown(wait=True)


class AsyncBatchProcessor:
    """Optimized async batch processor with backpressure handling"""
    
    def __init__(self, batch_size: int = 1000, max_concurrent: int = 5):
        self.batch_size = batch_size
        self.max_concurrent = max_concurrent
        self.semaphore = asyncio.Semaphore(max_concurrent)
    
    async def process_in_batches(
        self,
        items: AsyncIterator[T],
        processor: Callable[[List[T]], Awaitable[None]]
    ) -> ExtractionResult:
        """Process items in optimized batches with concurrency control"""
        start_time = datetime.now(timezone.utc)
        total_processed = 0
        errors = []
        tasks = []
        
        async for batch in self._batch_iterator(items, self.batch_size):
            # Create task with semaphore control
            task = asyncio.create_task(
                self._process_batch_with_limit(batch, processor)
            )
            tasks.append(task)
            total_processed += len(batch)
            
            # Backpressure: wait if too many pending tasks
            if len(tasks) >= self.max_concurrent * 2:
                done, tasks = await asyncio.wait(
                    tasks,
                    return_when=asyncio.FIRST_COMPLETED
                )
                
                # Check for errors in completed tasks
                for task in done:
                    if task.exception():
                        errors.append(str(task.exception()))
                        logger.error(f"Batch processing failed: {task.exception()}")
        
        # Wait for all remaining tasks
        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for result in results:
                if isinstance(result, Exception):
                    errors.append(str(result))
        
        duration = (datetime.now(timezone.utc) - start_time).total_seconds() * 1000
        
        return ExtractionResult(
            success=len(errors) == 0,
            items_processed=total_processed,
            duration_ms=duration,
            errors=errors
        )
    
    async def _batch_iterator(
        self,
        items: AsyncIterator[T],
        size: int
    ) -> AsyncIterator[List[T]]:
        """Create batches from async iterator"""
        batch = []
        async for item in items:
            batch.append(item)
            if len(batch) >= size:
                yield batch
                batch = []
        if batch:
            yield batch
    
    async def _process_batch_with_limit(
        self,
        batch: List[T],
        processor: Callable[[List[T]], Awaitable[None]]
    ) -> None:
        """Process a batch with concurrency limit"""
        async with self.semaphore:
            await processor(batch)


class AsyncPlexClient:
    """Enhanced async Plex API client with proper connection pooling"""
    
    def __init__(
        self,
        api_key: str,
        customer_id: str,
        max_connections: int = 20,
        timeout: int = 30
    ):
        self.api_key = api_key
        self.customer_id = customer_id
        
        # HTTP/2 client with connection pooling
        self.client = httpx.AsyncClient(
            http2=True,
            limits=httpx.Limits(
                max_keepalive_connections=max_connections,
                max_connections=max_connections * 2,
                keepalive_expiry=30
            ),
            timeout=httpx.Timeout(timeout, pool=5.0),
            headers={
                'X-Plex-Connect-Api-Key': api_key,
                'X-Plex-Connect-Customer-Id': customer_id,
                'Content-Type': 'application/json'
            }
        )
        
        # Rate limiting
        self.rate_limiter = asyncio.Semaphore(10)  # Max 10 concurrent requests
        self.retry_handler = RetryHandler()
    
    @asynccontextmanager
    async def session(self):
        """Async context manager for client session"""
        try:
            yield self
        finally:
            await self.close()
    
    async def close(self):
        """Close the HTTP client"""
        await self.client.aclose()
    
    @with_retry(max_attempts=3, initial_delay=1.0)
    async def fetch(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        method: str = "GET"
    ) -> Dict[str, Any]:
        """Fetch data from Plex API with retry and rate limiting"""
        async with self.rate_limiter:
            url = f"https://connect.plex.com{endpoint}"
            
            try:
                if method == "GET":
                    response = await self.client.get(url, params=params)
                elif method == "POST":
                    response = await self.client.post(url, json=params)
                else:
                    raise ValueError(f"Unsupported method: {method}")
                
                handle_api_response(response, "Plex API")
                response.raise_for_status()
                return response.json()
                
            except httpx.TimeoutException as e:
                logger.error(f"Timeout fetching {endpoint}: {e}")
                raise PlexAPIError(f"Request timeout: {endpoint}")
            except httpx.HTTPError as e:
                logger.error(f"HTTP error fetching {endpoint}: {e}")
                raise PlexAPIError(f"HTTP error: {e}")
    
    async def fetch_paginated(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        page_size: int = 1000
    ) -> AsyncIterator[Dict[str, Any]]:
        """Fetch paginated data from Plex API"""
        params = params or {}
        params['limit'] = page_size
        offset = 0
        
        while True:
            params['offset'] = offset
            
            try:
                data = await self.fetch(endpoint, params)
                
                # Handle different response formats
                if isinstance(data, list):
                    items = data
                elif isinstance(data, dict) and 'data' in data:
                    items = data['data']
                else:
                    items = [data] if data else []
                
                if not items:
                    break
                
                for item in items:
                    yield item
                
                # Check if we got less than page_size (last page)
                if len(items) < page_size:
                    break
                
                offset += len(items)
                
            except Exception as e:
                logger.error(f"Error in paginated fetch: {e}")
                raise


class AsyncBaseExtractor(Generic[T]):
    """Enhanced base extractor with proper async patterns"""
    
    def __init__(
        self,
        cdf_client: CogniteClient,
        plex_api_key: str,
        plex_customer_id: str,
        extractor_name: str,
        dataset_mapping: Dict[DatasetType, int]
    ):
        self.cdf_client = cdf_client
        self.async_cdf = AsyncCDFClient(cdf_client)
        self.plex_client = AsyncPlexClient(plex_api_key, plex_customer_id)
        self.extractor_name = extractor_name
        self.dataset_mapping = dataset_mapping
        
        # Batch processor for efficient processing
        self.batch_processor = AsyncBatchProcessor()
        
        # Metrics
        self.metrics = {
            'total_extractions': 0,
            'successful_extractions': 0,
            'failed_extractions': 0,
            'total_items_processed': 0
        }
    
    async def extract(self) -> ExtractionResult:
        """Main extraction method to be implemented by subclasses"""
        raise NotImplementedError("Subclasses must implement extract()")
    
    async def run_extraction_cycle(self) -> None:
        """Run a complete extraction cycle with proper async handling"""
        logger.info(f"Starting extraction cycle for {self.extractor_name}")
        
        try:
            # Run multiple extraction tasks concurrently
            async with asyncio.TaskGroup() as tg:
                asset_task = tg.create_task(self._extract_assets())
                event_task = tg.create_task(self._extract_events())
                ts_task = tg.create_task(self._extract_timeseries())
            
            # All tasks completed successfully
            self.metrics['successful_extractions'] += 1
            logger.info(f"Extraction cycle completed for {self.extractor_name}")
            
        except* Exception as eg:
            # Handle exceptions from task group
            for error in eg.exceptions:
                logger.error(f"Extraction error in {self.extractor_name}: {error}")
            self.metrics['failed_extractions'] += 1
            raise
        finally:
            self.metrics['total_extractions'] += 1
    
    async def _extract_assets(self) -> ExtractionResult:
        """Extract and create/update assets"""
        logger.info(f"Extracting assets for {self.extractor_name}")
        
        # Fetch data from Plex
        assets_to_create = []
        async for item in self._fetch_asset_data():
            asset = self._transform_to_asset(item)
            if asset:
                assets_to_create.append(asset)
        
        # Process in batches
        if assets_to_create:
            result = await self.batch_processor.process_in_batches(
                self._async_iter(assets_to_create),
                self._create_assets_batch
            )
            self.metrics['total_items_processed'] += result.items_processed
            return result
        
        return ExtractionResult(success=True, items_processed=0, duration_ms=0)
    
    async def _extract_events(self) -> ExtractionResult:
        """Extract and create events"""
        logger.info(f"Extracting events for {self.extractor_name}")
        
        # Fetch data from Plex
        events_to_create = []
        async for item in self._fetch_event_data():
            event = self._transform_to_event(item)
            if event:
                events_to_create.append(event)
        
        # Process in batches
        if events_to_create:
            result = await self.batch_processor.process_in_batches(
                self._async_iter(events_to_create),
                self._create_events_batch
            )
            self.metrics['total_items_processed'] += result.items_processed
            return result
        
        return ExtractionResult(success=True, items_processed=0, duration_ms=0)
    
    async def _extract_timeseries(self) -> ExtractionResult:
        """Extract and create timeseries"""
        logger.info(f"Extracting timeseries for {self.extractor_name}")
        
        # Fetch data from Plex
        ts_to_create = []
        async for item in self._fetch_timeseries_data():
            ts = self._transform_to_timeseries(item)
            if ts:
                ts_to_create.append(ts)
        
        # Process in batches
        if ts_to_create:
            result = await self.batch_processor.process_in_batches(
                self._async_iter(ts_to_create),
                self._create_timeseries_batch
            )
            self.metrics['total_items_processed'] += result.items_processed
            return result
        
        return ExtractionResult(success=True, items_processed=0, duration_ms=0)
    
    async def _async_iter(self, items: List[T]) -> AsyncIterator[T]:
        """Convert list to async iterator"""
        for item in items:
            yield item
    
    async def _create_assets_batch(self, assets: List[Asset]) -> None:
        """Create assets in CDF"""
        await self.async_cdf.upsert_assets(assets)
    
    async def _create_events_batch(self, events: List[Event]) -> None:
        """Create events in CDF"""
        await self.async_cdf.create_events(events)
    
    async def _create_timeseries_batch(self, timeseries: List[TimeSeries]) -> None:
        """Create timeseries in CDF"""
        await self.async_cdf.create_time_series(timeseries)
    
    # These methods should be implemented by subclasses
    async def _fetch_asset_data(self) -> AsyncIterator[Dict[str, Any]]:
        """Fetch asset data from Plex - implement in subclass"""
        return
        yield  # Make it a generator
    
    async def _fetch_event_data(self) -> AsyncIterator[Dict[str, Any]]:
        """Fetch event data from Plex - implement in subclass"""
        return
        yield  # Make it a generator
    
    async def _fetch_timeseries_data(self) -> AsyncIterator[Dict[str, Any]]:
        """Fetch timeseries data from Plex - implement in subclass"""
        return
        yield  # Make it a generator
    
    def _transform_to_asset(self, data: Dict[str, Any]) -> Optional[Asset]:
        """Transform Plex data to CDF Asset - implement in subclass"""
        return None
    
    def _transform_to_event(self, data: Dict[str, Any]) -> Optional[Event]:
        """Transform Plex data to CDF Event - implement in subclass"""
        return None
    
    def _transform_to_timeseries(self, data: Dict[str, Any]) -> Optional[TimeSeries]:
        """Transform Plex data to CDF TimeSeries - implement in subclass"""
        return None
    
    async def cleanup(self):
        """Cleanup resources"""
        await self.plex_client.close()
        self.async_cdf.cleanup()
    
    async def get_metrics(self) -> Dict[str, Any]:
        """Get extraction metrics"""
        return self.metrics.copy()