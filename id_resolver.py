#!/usr/bin/env python3
"""
Asset ID Resolver - Converts external IDs to numeric IDs for CDF Events
Solves the critical issue of CDF events requiring numeric asset IDs
"""

from typing import Dict, List, Optional, Set, Tuple
from cognite.client import CogniteClient
from cognite.client.data_classes import Asset, AssetList
import asyncio
from functools import lru_cache
import logging

logger = logging.getLogger(__name__)


class AssetIDResolver:
    """Resolves external asset IDs to numeric IDs with caching"""
    
    def __init__(self, client: CogniteClient, cache_size: int = 10000):
        """
        Initialize the resolver with a CDF client
        
        Args:
            client: CogniteClient instance
            cache_size: Maximum number of cached ID mappings
        """
        self.client = client
        self._cache: Dict[str, int] = {}
        self._reverse_cache: Dict[int, str] = {}
        self._not_found: Set[str] = set()
        self.cache_size = cache_size
    
    def clear_cache(self) -> None:
        """Clear all cached ID mappings"""
        self._cache.clear()
        self._reverse_cache.clear()
        self._not_found.clear()
    
    def resolve_single(self, external_id: str) -> Optional[int]:
        """
        Resolve a single external ID to numeric ID
        
        Args:
            external_id: Asset external ID
            
        Returns:
            Numeric asset ID or None if not found
        """
        # Check cache first
        if external_id in self._cache:
            return self._cache[external_id]
        
        # Check if previously not found
        if external_id in self._not_found:
            return None
        
        try:
            # Fetch from CDF
            asset = self.client.assets.retrieve(external_id=external_id)
            if asset:
                # Cache the result
                self._add_to_cache(external_id, asset.id)
                return asset.id
            else:
                self._not_found.add(external_id)
                return None
        except Exception as e:
            logger.error(f"Error resolving asset ID for {external_id}: {e}")
            return None
    
    def resolve_batch(self, external_ids: List[str]) -> Dict[str, Optional[int]]:
        """
        Resolve multiple external IDs to numeric IDs in batch
        
        Args:
            external_ids: List of asset external IDs
            
        Returns:
            Dictionary mapping external IDs to numeric IDs (or None)
        """
        result = {}
        uncached_ids = []
        
        # Check cache first
        for ext_id in external_ids:
            if ext_id in self._cache:
                result[ext_id] = self._cache[ext_id]
            elif ext_id in self._not_found:
                result[ext_id] = None
            else:
                uncached_ids.append(ext_id)
        
        # Fetch uncached IDs from CDF
        if uncached_ids:
            try:
                assets = self.client.assets.retrieve_multiple(
                    external_ids=uncached_ids,
                    ignore_unknown_ids=True
                )
                
                if isinstance(assets, AssetList):
                    # Process found assets
                    found_ids = set()
                    for asset in assets:
                        self._add_to_cache(asset.external_id, asset.id)
                        result[asset.external_id] = asset.id
                        found_ids.add(asset.external_id)
                    
                    # Mark not found IDs
                    for ext_id in uncached_ids:
                        if ext_id not in found_ids:
                            self._not_found.add(ext_id)
                            result[ext_id] = None
                            
            except Exception as e:
                logger.error(f"Error resolving batch of asset IDs: {e}")
                # Mark all as not found
                for ext_id in uncached_ids:
                    result[ext_id] = None
        
        return result
    
    def get_or_create_asset(
        self,
        external_id: str,
        name: str,
        parent_external_id: Optional[str] = None,
        metadata: Optional[Dict[str, str]] = None,
        dataset_id: Optional[int] = None
    ) -> Optional[int]:
        """
        Get numeric ID for asset, creating it if it doesn't exist
        
        Args:
            external_id: Asset external ID
            name: Asset name
            parent_external_id: Parent asset external ID
            metadata: Asset metadata
            dataset_id: Dataset ID for the asset
            
        Returns:
            Numeric asset ID or None if creation failed
        """
        # Try to resolve existing
        asset_id = self.resolve_single(external_id)
        if asset_id:
            return asset_id
        
        # Create new asset
        try:
            # Resolve parent ID if provided
            parent_id = None
            if parent_external_id:
                parent_id = self.resolve_single(parent_external_id)
                if not parent_id:
                    logger.warning(f"Parent asset {parent_external_id} not found")
            
            # Create asset
            asset = Asset(
                external_id=external_id,
                name=name,
                parent_id=parent_id,
                metadata=metadata or {},
                data_set_id=dataset_id
            )
            
            created = self.client.assets.create(asset)
            if created:
                self._add_to_cache(external_id, created.id)
                return created.id
                
        except Exception as e:
            logger.error(f"Error creating asset {external_id}: {e}")
        
        return None
    
    def resolve_hierarchy(
        self,
        assets_with_parents: List[Tuple[str, Optional[str]]]
    ) -> Dict[str, Optional[int]]:
        """
        Resolve a hierarchy of assets with parent relationships
        
        Args:
            assets_with_parents: List of (external_id, parent_external_id) tuples
            
        Returns:
            Dictionary mapping external IDs to numeric IDs
        """
        # Build dependency graph
        children: Dict[str, List[str]] = {}
        roots = []
        
        for ext_id, parent_ext_id in assets_with_parents:
            if parent_ext_id:
                if parent_ext_id not in children:
                    children[parent_ext_id] = []
                children[parent_ext_id].append(ext_id)
            else:
                roots.append(ext_id)
        
        # Resolve in hierarchical order
        result = {}
        queue = roots.copy()
        
        while queue:
            batch = queue[:100]  # Process in batches
            queue = queue[100:]
            
            # Resolve current batch
            batch_result = self.resolve_batch(batch)
            result.update(batch_result)
            
            # Add children to queue
            for ext_id in batch:
                if ext_id in children:
                    queue.extend(children[ext_id])
        
        return result
    
    def _add_to_cache(self, external_id: str, numeric_id: int) -> None:
        """Add ID mapping to cache with size limit"""
        # Remove from not found set if present
        self._not_found.discard(external_id)
        
        # Check cache size limit
        if len(self._cache) >= self.cache_size:
            # Remove oldest entry (simple FIFO for now)
            if self._cache:
                old_ext_id = next(iter(self._cache))
                old_num_id = self._cache.pop(old_ext_id)
                self._reverse_cache.pop(old_num_id, None)
        
        # Add to cache
        self._cache[external_id] = numeric_id
        self._reverse_cache[numeric_id] = external_id
    
    def get_external_id(self, numeric_id: int) -> Optional[str]:
        """
        Reverse lookup: get external ID from numeric ID
        
        Args:
            numeric_id: Numeric asset ID
            
        Returns:
            External ID or None if not cached
        """
        return self._reverse_cache.get(numeric_id)


class EventAssetLinker:
    """Helper to link events to assets using numeric IDs"""
    
    def __init__(self, resolver: AssetIDResolver):
        """
        Initialize with an ID resolver
        
        Args:
            resolver: AssetIDResolver instance
        """
        self.resolver = resolver
    
    def prepare_event_asset_ids(
        self,
        asset_external_ids: List[str]
    ) -> List[int]:
        """
        Convert external asset IDs to numeric IDs for event creation
        
        Args:
            asset_external_ids: List of asset external IDs
            
        Returns:
            List of numeric asset IDs (excluding None values)
        """
        if not asset_external_ids:
            return []
        
        # Resolve all IDs
        id_map = self.resolver.resolve_batch(asset_external_ids)
        
        # Filter out None values and return numeric IDs
        numeric_ids = []
        for ext_id in asset_external_ids:
            num_id = id_map.get(ext_id)
            if num_id is not None:
                numeric_ids.append(num_id)
            else:
                logger.warning(f"Asset {ext_id} not found, skipping link")
        
        return numeric_ids
    
    def link_events_to_assets(
        self,
        events: List[Dict],
        asset_field: str = 'asset_external_ids'
    ) -> List[Dict]:
        """
        Process a list of event dictionaries to convert asset external IDs to numeric
        
        Args:
            events: List of event dictionaries
            asset_field: Field name containing asset external IDs
            
        Returns:
            Modified events with numeric asset IDs
        """
        processed_events = []
        
        for event in events:
            event_copy = event.copy()
            
            # Get asset external IDs
            asset_ext_ids = event_copy.pop(asset_field, [])
            
            if asset_ext_ids:
                # Convert to numeric IDs
                numeric_ids = self.prepare_event_asset_ids(asset_ext_ids)
                if numeric_ids:
                    event_copy['asset_ids'] = numeric_ids
            
            processed_events.append(event_copy)
        
        return processed_events


# Singleton instance for global use
_resolver_instance: Optional[AssetIDResolver] = None

def get_resolver(client: CogniteClient) -> AssetIDResolver:
    """Get or create the global resolver instance"""
    global _resolver_instance
    if _resolver_instance is None:
        _resolver_instance = AssetIDResolver(client)
    return _resolver_instance