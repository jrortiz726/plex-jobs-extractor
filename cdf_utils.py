#!/usr/bin/env python3
"""
CDF Utilities for Deduplication and Upsert Operations

Provides helper functions to:
- Check if assets/events/timeseries already exist
- Update only when data has changed
- Prevent duplicate data creation
- Track state for incremental updates
"""

import os
import logging
from typing import List, Dict, Any, Optional, Set
from datetime import datetime, timezone
import hashlib
import json

from cognite.client import CogniteClient
from cognite.client.data_classes import Asset, Event, TimeSeries, AssetUpdate
from cognite.client.exceptions import CogniteAPIError

logger = logging.getLogger(__name__)


class CDFDeduplicationHelper:
    """Helper class for preventing duplicate data in CDF"""
    
    def __init__(self, cognite_client: CogniteClient):
        self.client = cognite_client
        self._existing_assets_cache: Dict[str, Asset] = {}
        self._existing_events_cache: Set[str] = set()
        self._existing_timeseries_cache: Dict[str, TimeSeries] = {}
        self._last_cache_refresh = datetime.now(timezone.utc)
        self.cache_ttl_seconds = 300  # 5 minutes
        
    def _should_refresh_cache(self) -> bool:
        """Check if cache should be refreshed"""
        elapsed = (datetime.now(timezone.utc) - self._last_cache_refresh).total_seconds()
        return elapsed > self.cache_ttl_seconds
    
    def refresh_cache(self, external_id_prefix: str = None):
        """Refresh the cache of existing CDF resources"""
        logger.info("Refreshing CDF resource cache...")
        
        try:
            # Refresh assets cache
            if external_id_prefix:
                assets = self.client.assets.list(
                    external_id_prefix=external_id_prefix,
                    limit=-1
                )
            else:
                assets = self.client.assets.list(limit=1000)
            
            self._existing_assets_cache = {
                asset.external_id: asset for asset in assets
            }
            logger.info(f"Cached {len(self._existing_assets_cache)} existing assets")
            
            # Refresh timeseries cache
            if external_id_prefix:
                timeseries = self.client.time_series.list(
                    external_id_prefix=external_id_prefix,
                    limit=-1
                )
            else:
                timeseries = self.client.time_series.list(limit=1000)
            
            self._existing_timeseries_cache = {
                ts.external_id: ts for ts in timeseries
            }
            logger.info(f"Cached {len(self._existing_timeseries_cache)} existing time series")
            
            self._last_cache_refresh = datetime.now(timezone.utc)
            
        except Exception as e:
            logger.error(f"Error refreshing cache: {e}")
    
    def asset_exists(self, external_id: str) -> bool:
        """Check if an asset already exists"""
        if self._should_refresh_cache():
            self.refresh_cache()
        
        # Check cache first
        if external_id in self._existing_assets_cache:
            return True
        
        # Double-check with API in case cache is stale
        try:
            asset = self.client.assets.retrieve(external_id=external_id)
            if asset:
                self._existing_assets_cache[external_id] = asset
                return True
        except:
            pass
        
        return False
    
    def timeseries_exists(self, external_id: str) -> bool:
        """Check if a time series already exists"""
        if self._should_refresh_cache():
            self.refresh_cache()
        
        # Check cache first
        if external_id in self._existing_timeseries_cache:
            return True
        
        # Double-check with API
        try:
            ts = self.client.time_series.retrieve(external_id=external_id)
            if ts:
                self._existing_timeseries_cache[external_id] = ts
                return True
        except:
            pass
        
        return False
    
    def event_exists(self, external_id: str) -> bool:
        """Check if an event already exists"""
        # Events are harder to cache efficiently, so we check directly
        try:
            events = self.client.events.list(
                external_id=external_id,
                limit=1
            )
            return len(events) > 0
        except:
            return False
    
    def compute_metadata_hash(self, metadata: Dict[str, Any]) -> str:
        """Compute a hash of metadata for change detection"""
        # Sort keys for consistent hashing
        sorted_metadata = json.dumps(metadata, sort_keys=True)
        return hashlib.md5(sorted_metadata.encode()).hexdigest()
    
    def asset_needs_update(self, external_id: str, new_metadata: Dict[str, Any]) -> bool:
        """Check if an asset needs updating based on metadata changes"""
        if external_id not in self._existing_assets_cache:
            return False  # Asset doesn't exist, should create not update
        
        existing_asset = self._existing_assets_cache[external_id]
        existing_hash = self.compute_metadata_hash(existing_asset.metadata or {})
        new_hash = self.compute_metadata_hash(new_metadata)
        
        return existing_hash != new_hash
    
    def upsert_assets(self, assets: List[Asset]) -> Dict[str, List[Asset]]:
        """
        Upsert assets - create new ones and update existing ones only if changed
        
        Returns:
            Dict with 'created' and 'updated' lists of assets
        """
        if self._should_refresh_cache():
            self.refresh_cache()
        
        to_create = []
        to_update = []
        skipped = []
        
        for asset in assets:
            if not self.asset_exists(asset.external_id):
                to_create.append(asset)
                logger.debug(f"Asset {asset.external_id} will be created")
            elif self.asset_needs_update(asset.external_id, asset.metadata or {}):
                # Create an update object
                update = AssetUpdate(external_id=asset.external_id)
                update.metadata.set(asset.metadata)
                if asset.name:
                    update.name.set(asset.name)
                if asset.description:
                    update.description.set(asset.description)
                to_update.append(update)
                logger.debug(f"Asset {asset.external_id} will be updated")
            else:
                skipped.append(asset)
                logger.debug(f"Asset {asset.external_id} unchanged, skipping")
        
        result = {
            'created': [],
            'updated': [],
            'skipped': skipped
        }
        
        # Create new assets
        if to_create:
            try:
                created = self.client.assets.create(to_create)
                # Handle Cognite SDK response (AssetList or Asset)
                if hasattr(created, '__iter__') and not isinstance(created, str):
                    # It's iterable (AssetList)
                    result['created'] = list(created)
                else:
                    # Single Asset
                    result['created'] = [created]
                logger.info(f"Created {len(result['created'])} new assets")
                # Update cache
                for asset in result['created']:
                    self._existing_assets_cache[asset.external_id] = asset
            except CogniteAPIError as e:
                logger.error(f"Error creating assets: {e}")
                # Try creating one by one to identify problematic assets
                for asset in to_create:
                    try:
                        created_single = self.client.assets.create(asset)
                        result['created'].append(created_single)
                        self._existing_assets_cache[created_single.external_id] = created_single
                    except Exception as e2:
                        logger.error(f"Failed to create asset {asset.external_id}: {e2}")
        
        # Update existing assets
        if to_update:
            try:
                updated = self.client.assets.update(to_update)
                # Handle AssetList or list of assets
                if hasattr(updated, '__iter__'):
                    result['updated'] = list(updated)
                else:
                    result['updated'] = [updated]
                logger.info(f"Updated {len(result['updated'])} existing assets")
                # Update cache
                for asset in result['updated']:
                    self._existing_assets_cache[asset.external_id] = asset
            except CogniteAPIError as e:
                logger.error(f"Error updating assets: {e}")
        
        logger.info(f"Asset upsert complete: {len(result['created'])} created, "
                   f"{len(result['updated'])} updated, {len(skipped)} skipped")
        
        return result
    
    def upsert_timeseries(self, timeseries_list: List[TimeSeries]) -> Dict[str, List[TimeSeries]]:
        """
        Upsert time series - create only if they don't exist
        
        Returns:
            Dict with 'created' and 'skipped' lists
        """
        if self._should_refresh_cache():
            self.refresh_cache()
        
        to_create = []
        skipped = []
        
        for ts in timeseries_list:
            if not self.timeseries_exists(ts.external_id):
                to_create.append(ts)
                logger.debug(f"TimeSeries {ts.external_id} will be created")
            else:
                skipped.append(ts)
                logger.debug(f"TimeSeries {ts.external_id} already exists, skipping")
        
        result = {
            'created': [],
            'skipped': skipped
        }
        
        if to_create:
            try:
                created = self.client.time_series.create(to_create)
                # Handle Cognite SDK response (TimeSeriesList or TimeSeries)
                if hasattr(created, '__iter__') and not isinstance(created, str):
                    # It's iterable (TimeSeriesList)
                    result['created'] = list(created)
                else:
                    # Single TimeSeries
                    result['created'] = [created]
                logger.info(f"Created {len(result['created'])} new time series")
                # Update cache
                for ts in result['created']:
                    self._existing_timeseries_cache[ts.external_id] = ts
            except CogniteAPIError as e:
                logger.error(f"Error creating time series: {e}")
                # Try one by one
                for ts in to_create:
                    try:
                        created = self.client.time_series.create(ts)
                        result['created'].append(created)
                        self._existing_timeseries_cache[created.external_id] = created
                    except Exception as e2:
                        logger.error(f"Failed to create timeseries {ts.external_id}: {e2}")
        
        logger.info(f"TimeSeries upsert complete: {len(result['created'])} created, "
                   f"{len(skipped)} skipped")
        
        return result
    
    def filter_duplicate_events(self, events: List[Event]) -> List[Event]:
        """
        Filter out duplicate events based on external_id
        
        Returns:
            List of events that don't already exist
        """
        unique_events = []
        
        for event in events:
            if not self.event_exists(event.external_id):
                unique_events.append(event)
                logger.debug(f"Event {event.external_id} is unique")
            else:
                logger.debug(f"Event {event.external_id} already exists, filtering out")
        
        logger.info(f"Filtered events: {len(unique_events)} unique out of {len(events)} total")
        return unique_events
    
    def create_events_batch(self, events: List[Event], batch_size: int = 1000) -> Dict[str, List]:
        """
        Create events in batches, filtering duplicates
        
        Returns:
            Dict with 'created' and 'duplicates' lists
        """
        result = {
            'created': [],
            'duplicates': []
        }
        
        if not events:
            return result
        
        # Filter duplicates
        unique_events = []
        for event in events:
            if not self.event_exists(event.external_id):
                unique_events.append(event)
            else:
                result['duplicates'].append(event.external_id)
                logger.debug(f"Event {event.external_id} already exists, filtering out")
        
        logger.info(f"Filtered events: {len(unique_events)} unique out of {len(events)} total")
        
        if not unique_events:
            logger.info("No new events to create")
            return result
        
        # Create in batches
        for i in range(0, len(unique_events), batch_size):
            batch = unique_events[i:i + batch_size]
            try:
                created = self.client.events.create(batch)
                if hasattr(created, '__iter__'):
                    for event in created:
                        result['created'].append(event.external_id)
                else:
                    result['created'].append(created.external_id)
                logger.info(f"Created batch of {len(batch)} events")
            except CogniteAPIError as e:
                logger.error(f"Error creating event batch: {e}")
                # Try one by one for this batch
                for event in batch:
                    try:
                        created = self.client.events.create(event)
                        result['created'].append(created.external_id)
                    except Exception as e2:
                        logger.error(f"Failed to create event {event.external_id}: {e2}")
        
        logger.info(f"Created {len(result['created'])} new events")
        return result


class StateTracker:
    """Track extraction state to enable incremental updates"""
    
    def __init__(self, state_file: str = "extraction_state.json"):
        self.state_file = state_file
        self.state = self._load_state()
    
    def _load_state(self) -> Dict[str, Any]:
        """Load state from file"""
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"Error loading state file: {e}")
        return {}
    
    def save_state(self):
        """Save state to file"""
        try:
            with open(self.state_file, 'w') as f:
                json.dump(self.state, f, indent=2, default=str)
            logger.debug(f"State saved to {self.state_file}")
        except Exception as e:
            logger.error(f"Error saving state: {e}")
    
    def get_last_extraction_time(self, extractor_name: str) -> Optional[datetime]:
        """Get the last extraction time for a specific extractor"""
        if extractor_name in self.state:
            timestamp = self.state[extractor_name].get('last_extraction_time')
            if timestamp:
                return datetime.fromisoformat(timestamp)
        return None
    
    def set_last_extraction_time(self, extractor_name: str, timestamp: datetime):
        """Set the last extraction time for a specific extractor"""
        if extractor_name not in self.state:
            self.state[extractor_name] = {}
        self.state[extractor_name]['last_extraction_time'] = timestamp.isoformat()
        self.save_state()
    
    def get_last_processed_id(self, extractor_name: str, resource_type: str) -> Optional[str]:
        """Get the last processed ID for incremental extraction"""
        if extractor_name in self.state:
            return self.state[extractor_name].get(f'last_{resource_type}_id')
        return None
    
    def set_last_processed_id(self, extractor_name: str, resource_type: str, last_id: str):
        """Set the last processed ID for incremental extraction"""
        if extractor_name not in self.state:
            self.state[extractor_name] = {}
        self.state[extractor_name][f'last_{resource_type}_id'] = last_id
        self.save_state()
    
    def get_processed_ids(self, extractor_name: str, resource_type: str) -> Set[str]:
        """Get set of already processed IDs"""
        if extractor_name in self.state:
            ids = self.state[extractor_name].get(f'processed_{resource_type}_ids', [])
            return set(ids)
        return set()
    
    def add_processed_id(self, extractor_name: str, resource_type: str, id_value: str):
        """Add an ID to the processed set"""
        if extractor_name not in self.state:
            self.state[extractor_name] = {}
        
        key = f'processed_{resource_type}_ids'
        if key not in self.state[extractor_name]:
            self.state[extractor_name][key] = []
        
        if id_value not in self.state[extractor_name][key]:
            self.state[extractor_name][key].append(id_value)
            # Keep only last 10000 IDs to prevent unbounded growth
            if len(self.state[extractor_name][key]) > 10000:
                self.state[extractor_name][key] = self.state[extractor_name][key][-10000:]
            self.save_state()