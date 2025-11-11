#!/usr/bin/env python3
"""
Inspect CDF datasets to see what data exists from previous runs
"""

import os
from dotenv import load_dotenv
from cognite.client import CogniteClient
from cognite.client.config import ClientConfig
from cognite.client.credentials import OAuthClientCredentials
from datetime import datetime, timezone
from collections import defaultdict

# Load environment variables
load_dotenv()

def init_cdf_client():
    """Initialize CDF client"""
    creds = OAuthClientCredentials(
        token_url=os.getenv("CDF_TOKEN_URL"),
        client_id=os.getenv("CDF_CLIENT_ID"),
        client_secret=os.getenv("CDF_CLIENT_SECRET"),
        scopes=["user_impersonation"]
    )
    
    return CogniteClient(
        ClientConfig(
            client_name="inspect-script",
            base_url=os.getenv("CDF_HOST"),
            project=os.getenv("CDF_PROJECT"),
            credentials=creds
        )
    )

def get_dataset_ids():
    """Get all Plex dataset IDs from environment"""
    datasets = {
        "Scheduling": os.getenv("CDF_DATASET_PLEXSCHEDULING"),
        "Production": os.getenv("CDF_DATASET_PLEXPRODUCTION"),
        "Inventory": os.getenv("CDF_DATASET_PLEXINVENTORY"),
        "Master": os.getenv("CDF_DATASET_PLEXMASTER"),
        "Quality": os.getenv("CDF_DATASET_PLEXQUALITY"),
        "Maintenance": os.getenv("CDF_DATASET_PLEXMAINTENANCE"),
    }
    
    # Filter out None values and convert to int
    return {name: int(id) for name, id in datasets.items() if id}

def inspect_assets(client, dataset_id, dataset_name):
    """Inspect assets in a dataset"""
    print(f"\n  Assets in {dataset_name}:")
    
    try:
        # Get sample of assets
        assets = client.assets.list(
            data_set_ids=[dataset_id],
            limit=1000
        )
        
        if not assets:
            print(f"    No assets found")
            return
        
        # Count by type
        asset_types = defaultdict(int)
        for asset in assets:
            asset_type = asset.metadata.get('asset_type', 'unknown') if asset.metadata else 'unknown'
            asset_types[asset_type] += 1
        
        total = len(assets)
        print(f"    Total: {total} assets")
        print(f"    Types:")
        for asset_type, count in sorted(asset_types.items()):
            print(f"      - {asset_type}: {count}")
        
        # Show sample
        if assets:
            sample = assets[0]
            print(f"    Sample asset:")
            print(f"      External ID: {sample.external_id}")
            print(f"      Name: {sample.name}")
            print(f"      Created: {datetime.fromtimestamp(sample.created_time/1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}")
        
    except Exception as e:
        print(f"    Error: {e}")

def inspect_events(client, dataset_id, dataset_name):
    """Inspect events in a dataset"""
    print(f"\n  Events in {dataset_name}:")
    
    try:
        # Get sample of events
        events = client.events.list(
            data_set_ids=[dataset_id],
            limit=1000
        )
        
        if not events:
            print(f"    No events found")
            return
        
        # Count by type and subtype
        event_types = defaultdict(lambda: defaultdict(int))
        for event in events:
            event_type = event.type or 'unknown'
            event_subtype = event.subtype or 'none'
            event_types[event_type][event_subtype] += 1
        
        total = len(events)
        print(f"    Total: {total} events")
        print(f"    Types:")
        for event_type, subtypes in sorted(event_types.items()):
            print(f"      - {event_type}:")
            for subtype, count in sorted(subtypes.items()):
                print(f"          {subtype}: {count}")
        
        # Show most recent event
        if events:
            events_sorted = sorted(events, key=lambda e: e.start_time or 0, reverse=True)
            recent = events_sorted[0]
            print(f"    Most recent event:")
            print(f"      Type: {recent.type}/{recent.subtype}")
            if recent.start_time:
                print(f"      Time: {datetime.fromtimestamp(recent.start_time/1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"      External ID: {recent.external_id}")
        
    except Exception as e:
        print(f"    Error: {e}")

def inspect_timeseries(client, dataset_id, dataset_name):
    """Inspect time series in a dataset"""
    print(f"\n  Time Series in {dataset_name}:")
    
    try:
        # Get sample of time series
        timeseries = client.time_series.list(
            data_set_ids=[dataset_id],
            limit=1000
        )
        
        if not timeseries:
            print(f"    No time series found")
            return
        
        # Count by unit
        ts_units = defaultdict(int)
        for ts in timeseries:
            unit = ts.unit or 'no_unit'
            ts_units[unit] += 1
        
        total = len(timeseries)
        print(f"    Total: {total} time series")
        print(f"    Units:")
        for unit, count in sorted(ts_units.items()):
            print(f"      - {unit}: {count}")
        
        # Show sample
        if timeseries:
            sample = timeseries[0]
            print(f"    Sample time series:")
            print(f"      External ID: {sample.external_id}")
            print(f"      Name: {sample.name}")
            print(f"      Unit: {sample.unit}")
            
            # Get latest datapoint
            try:
                datapoints = client.time_series.data.retrieve_latest(
                    external_id=sample.external_id
                )
                if datapoints and datapoints[0].value:
                    latest = datapoints[0]
                    print(f"      Latest value: {latest.value[0]} at {datetime.fromtimestamp(latest.timestamp[0]/1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}")
            except:
                pass
        
    except Exception as e:
        print(f"    Error: {e}")

def main():
    """Main inspection function"""
    print("="*60)
    print("CDF DATASET INSPECTION TOOL")
    print("="*60)
    print("This shows what data exists in the Plex datasets")
    print()
    
    # Initialize CDF client
    print("Connecting to CDF...")
    try:
        client = init_cdf_client()
        project = client.iam.token.inspect()
        print(f"✓ Connected to project: {project.projects[0]}")
    except Exception as e:
        print(f"✗ Failed to connect to CDF: {e}")
        return
    
    # Get datasets
    datasets = get_dataset_ids()
    if not datasets:
        print("No datasets found in environment variables")
        return
    
    print("\nFound datasets:")
    for name, id in datasets.items():
        # Get dataset info
        try:
            dataset = client.data_sets.retrieve(id=id)
            print(f"  - {name}: {dataset.name} (ID: {id})")
        except:
            print(f"  - {name}: ID {id}")
    
    print("\nInspecting datasets...")
    
    for dataset_name, dataset_id in datasets.items():
        print(f"\n{'='*40}")
        print(f"Dataset: {dataset_name}")
        print(f"{'='*40}")
        
        inspect_assets(client, dataset_id, dataset_name)
        inspect_events(client, dataset_id, dataset_name)
        inspect_timeseries(client, dataset_id, dataset_name)
    
    print("\n" + "="*60)
    print("INSPECTION COMPLETE")
    print("="*60)
    print("\nTo clean these datasets for testing, run:")
    print("  python cleanup_datasets.py")

if __name__ == "__main__":
    main()