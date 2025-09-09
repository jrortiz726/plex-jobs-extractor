#!/usr/bin/env python3
"""
Multi-Facility Configuration Strategy for Plex MES Integration

This module demonstrates how to handle multiple Plex facilities (PCNs) 
in the naming conventions and dataset organization.
"""

import os
from dataclasses import dataclass
from typing import Optional, Dict, Any
from enum import Enum


class NamingStrategy(Enum):
    """Different strategies for handling multiple facilities"""
    SEPARATE_DATASETS = "separate_datasets"  # Each PCN gets its own datasets
    SHARED_DATASETS = "shared_datasets"      # All PCNs share datasets with prefixed IDs
    HYBRID = "hybrid"                        # Master data shared, operational data separated


@dataclass
class FacilityConfig:
    """Configuration for a specific Plex facility"""
    pcn: str                    # Plex Customer Number (e.g., "340884")
    facility_name: str           # Human-readable name (e.g., "Detroit Plant")
    facility_code: str           # Short code (e.g., "DET")
    timezone: str               # Facility timezone
    country: str                # Country code
    
    def get_prefix(self) -> str:
        """Get the prefix for all IDs from this facility"""
        return f"PCN{self.pcn}"
    
    def get_dataset_suffix(self) -> str:
        """Get suffix for dataset names"""
        # Could use facility_code for readability: "plex_det_production"
        # Or PCN for uniqueness: "plex_340884_production"
        return self.pcn


class MultiTenantNamingConvention:
    """Handles naming conventions for multi-facility Plex integration"""
    
    def __init__(self, facility: FacilityConfig, strategy: NamingStrategy = NamingStrategy.SEPARATE_DATASETS):
        self.facility = facility
        self.strategy = strategy
        self.pcn_prefix = f"PCN{facility.pcn}"
    
    # Asset External IDs
    def asset_id(self, asset_type: str, identifier: str) -> str:
        """Generate asset external ID with PCN prefix"""
        # Examples: PCN340884_JOB_12345, PCN340884_WC_MACHINE_001
        return f"{self.pcn_prefix}_{asset_type}_{identifier}"
    
    def root_asset_id(self, asset_type: str) -> str:
        """Generate root asset ID for facility hierarchy"""
        # Examples: PCN340884_PLANT_ROOT, PCN340884_PRODUCTION_ROOT
        return f"{self.pcn_prefix}_{asset_type}_ROOT"
    
    # Event External IDs
    def event_id(self, event_type: str, entity: str, timestamp: float) -> str:
        """Generate event external ID with PCN prefix"""
        # Example: PCN340884_EVT_JOB_START_12345_1234567890
        return f"{self.pcn_prefix}_EVT_{event_type}_{entity}_{int(timestamp)}"
    
    # Time Series External IDs
    def timeseries_id(self, entity_type: str, entity_id: str, metric: str) -> str:
        """Generate time series external ID with PCN prefix"""
        # Example: PCN340884_TS_WC_MACHINE001_OEE
        return f"{self.pcn_prefix}_TS_{entity_type}_{entity_id}_{metric}"
    
    # Dataset Names
    def dataset_name(self, domain: str) -> str:
        """Generate dataset name based on strategy"""
        if self.strategy == NamingStrategy.SEPARATE_DATASETS:
            # Example: plex_340884_production
            return f"plex_{self.facility.pcn}_{domain}"
        elif self.strategy == NamingStrategy.SHARED_DATASETS:
            # Example: plex_production (shared across all PCNs)
            return f"plex_{domain}"
        else:  # HYBRID
            # Master data shared, operational separated
            if domain in ['master', 'reference']:
                return f"plex_{domain}"
            else:
                return f"plex_{self.facility.pcn}_{domain}"
    
    def get_metadata_tags(self) -> Dict[str, str]:
        """Get standard metadata tags for this facility"""
        return {
            "pcn": self.facility.pcn,
            "facility_name": self.facility.facility_name,
            "facility_code": self.facility.facility_code,
            "country": self.facility.country,
            "timezone": self.facility.timezone,
            "source": "PlexMES"
        }


class MultiTenantExtractorConfig:
    """Extended configuration for multi-facility support"""
    
    @classmethod
    def from_env(cls) -> Dict[str, Any]:
        """Load configuration with PCN awareness"""
        pcn = os.getenv('PLEX_CUSTOMER_ID', '340884')
        
        # Map PCNs to facility information (could be loaded from config file)
        FACILITY_REGISTRY = {
            "340884": FacilityConfig(
                pcn="340884",
                facility_name="Main Manufacturing Plant",
                facility_code="MAIN",
                timezone="America/Detroit",
                country="US"
            ),
            "123456": FacilityConfig(
                pcn="123456",
                facility_name="European Plant",
                facility_code="EU01",
                timezone="Europe/Berlin",
                country="DE"
            )
            # Add more facilities as needed
        }
        
        # Get facility config or create default
        if pcn in FACILITY_REGISTRY:
            facility = FACILITY_REGISTRY[pcn]
        else:
            # Unknown PCN - create generic config
            facility = FacilityConfig(
                pcn=pcn,
                facility_name=f"Facility {pcn}",
                facility_code=f"F{pcn[:3]}",
                timezone="UTC",
                country="XX"
            )
        
        # Determine naming strategy from environment
        strategy_name = os.getenv('NAMING_STRATEGY', 'SEPARATE_DATASETS')
        strategy = NamingStrategy[strategy_name]
        
        naming = MultiTenantNamingConvention(facility, strategy)
        
        return {
            'facility': facility,
            'naming': naming,
            'pcn': pcn,
            'strategy': strategy
        }


# Example usage in extractors
def example_usage():
    """Show how to use the multi-tenant naming in extractors"""
    
    # Load configuration
    config = MultiTenantExtractorConfig.from_env()
    naming = config['naming']
    facility = config['facility']
    
    # Create assets with PCN-aware IDs
    job_id = "12345"
    asset_external_id = naming.asset_id("JOB", job_id)
    print(f"Asset ID: {asset_external_id}")  # PCN340884_JOB_12345
    
    # Create root hierarchy
    root_id = naming.root_asset_id("PRODUCTION")
    print(f"Root Asset: {root_id}")  # PCN340884_PRODUCTION_ROOT
    
    # Create events
    event_id = naming.event_id("JOB_START", job_id, 1234567890.123)
    print(f"Event ID: {event_id}")  # PCN340884_EVT_JOB_START_12345_1234567890
    
    # Create time series
    ts_id = naming.timeseries_id("WC", "MACHINE001", "OEE")
    print(f"TimeSeries ID: {ts_id}")  # PCN340884_TS_WC_MACHINE001_OEE
    
    # Get dataset names
    prod_dataset = naming.dataset_name("production")
    print(f"Production Dataset: {prod_dataset}")  # plex_340884_production
    
    # Get metadata tags for all objects
    metadata = naming.get_metadata_tags()
    print(f"Metadata: {metadata}")


# Configuration for dataset creation
def get_dataset_config_for_facility(facility: FacilityConfig, strategy: NamingStrategy) -> list:
    """Generate dataset configuration for a specific facility"""
    
    naming = MultiTenantNamingConvention(facility, strategy)
    
    datasets = []
    domains = ['production', 'scheduling', 'quality', 'inventory', 'maintenance', 'master']
    
    for domain in domains:
        dataset_name = naming.dataset_name(domain)
        
        # Skip if using shared datasets and not the primary facility
        if strategy == NamingStrategy.SHARED_DATASETS and facility.pcn != "340884":
            continue
            
        datasets.append({
            "external_id": dataset_name,
            "name": f"Plex {facility.facility_name} - {domain.title()}",
            "description": f"{domain.title()} data from Plex MES for {facility.facility_name} (PCN: {facility.pcn})",
            "metadata": {
                **naming.get_metadata_tags(),
                "domain": domain,
                "naming_strategy": strategy.value
            }
        })
    
    return datasets


if __name__ == "__main__":
    import json
    
    print("Multi-Facility Naming Convention Examples")
    print("=" * 60)
    
    # Test with different facilities
    facilities = [
        FacilityConfig("340884", "Detroit Plant", "DET", "America/Detroit", "US"),
        FacilityConfig("123456", "Berlin Plant", "BER", "Europe/Berlin", "DE")
    ]
    
    for facility in facilities:
        print(f"\nFacility: {facility.facility_name} (PCN: {facility.pcn})")
        print("-" * 40)
        
        naming = MultiTenantNamingConvention(facility, NamingStrategy.SEPARATE_DATASETS)
        
        # Show example IDs
        print(f"Job Asset:     {naming.asset_id('JOB', '12345')}")
        print(f"Event:         {naming.event_id('START', 'JOB_12345', 1234567890)}")
        print(f"Time Series:   {naming.timeseries_id('WC', 'MILL01', 'OEE')}")
        print(f"Dataset:       {naming.dataset_name('production')}")
        print(f"Root Asset:    {naming.root_asset_id('PLANT')}")
        
        # Show metadata
        print(f"Metadata:      {json.dumps(naming.get_metadata_tags(), indent=2)}")