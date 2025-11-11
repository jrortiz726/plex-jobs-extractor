#!/usr/bin/env python3
"""
Clean up CDF datasets for testing enhanced extractors
This will delete assets, events, and time series from the Plex datasets
"""

import os
import sys
from dotenv import load_dotenv
from cognite.client import CogniteClient
from cognite.client.config import ClientConfig
from cognite.client.credentials import OAuthClientCredentials
from datetime import datetime
import time

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
            client_name="cleanup-script",
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

def cleanup_assets(client, dataset_id, dataset_name):
    """Delete all assets in a dataset - handles hierarchy properly"""
    print(f"\n  Cleaning assets from {dataset_name}...")
    
    try:
        # List all assets in the dataset
        assets = client.assets.list(
            data_set_ids=[dataset_id],
            limit=None
        )
        
        if not assets:
            print(f"    No assets found")
            return 0
        
        print(f"    Found {len(assets)} assets")
        
        # Build parent-child relationship map
        parent_map = {}
        root_assets = []
        all_assets = {asset.id: asset for asset in assets}
        
        for asset in assets:
            if asset.parent_id and asset.parent_id in all_assets:
                if asset.parent_id not in parent_map:
                    parent_map[asset.parent_id] = []
                parent_map[asset.parent_id].append(asset.id)
            else:
                # Asset has no parent in this dataset or parent doesn't exist
                root_assets.append(asset.id)
        
        # Function to get deletion order (children first)
        def get_deletion_order(asset_id, visited=None):
            if visited is None:
                visited = set()
            
            if asset_id in visited:
                return []
            
            visited.add(asset_id)
            order = []
            
            # First add all children
            if asset_id in parent_map:
                for child_id in parent_map[asset_id]:
                    order.extend(get_deletion_order(child_id, visited))
            
            # Then add this asset
            order.append(asset_id)
            return order
        
        # Get deletion order starting from all roots
        deletion_order = []
        visited = set()
        
        # First process all assets that have parents (to get proper order)
        for asset_id in all_assets.keys():
            if asset_id not in visited:
                deletion_order.extend(get_deletion_order(asset_id, visited))
        
        print(f"    Deleting {len(deletion_order)} assets in hierarchical order...")
        
        # Delete in batches, but maintain order
        batch_size = 100
        deleted_count = 0
        failed_ids = []
        
        for i in range(0, len(deletion_order), batch_size):
            batch = deletion_order[i:i+batch_size]
            
            try:
                client.assets.delete(id=batch)
                deleted_count += len(batch)
                print(f"    Deleted {deleted_count}/{len(deletion_order)} assets...")
            except Exception as e:
                # Try deleting one by one to identify problematic assets
                print(f"    Batch deletion failed, trying one by one...")
                for asset_id in batch:
                    try:
                        client.assets.delete(id=[asset_id])
                        deleted_count += 1
                    except Exception as e2:
                        failed_ids.append(asset_id)
                        # Continue trying to delete others
        
        if failed_ids:
            print(f"    Warning: Could not delete {len(failed_ids)} assets (may have external references)")
        
        print(f"    ✓ Deleted {deleted_count} assets")
        return deleted_count
        
    except Exception as e:
        print(f"    ✗ Error cleaning assets: {e}")
        return 0

def cleanup_events(client, dataset_id, dataset_name):
    """Delete all events in a dataset"""
    print(f"\n  Cleaning events from {dataset_name}...")
    
    try:
        # List all events in the dataset
        events = client.events.list(
            data_set_ids=[dataset_id],
            limit=None
        )
        
        if not events:
            print(f"    No events found")
            return 0
        
        print(f"    Found {len(events)} events")
        
        # Delete in batches
        batch_size = 100
        deleted_count = 0
        
        for i in range(0, len(events), batch_size):
            batch = events[i:i+batch_size]
            event_ids = [event.id for event in batch]
            
            try:
                client.events.delete(id=event_ids)
                deleted_count += len(event_ids)
                print(f"    Deleted {deleted_count}/{len(events)} events...")
            except Exception as e:
                print(f"    Warning: Could not delete some events: {e}")
        
        print(f"    ✓ Deleted {deleted_count} events")
        return deleted_count
        
    except Exception as e:
        print(f"    ✗ Error cleaning events: {e}")
        return 0

def cleanup_timeseries(client, dataset_id, dataset_name):
    """Delete all time series in a dataset"""
    print(f"\n  Cleaning time series from {dataset_name}...")
    
    try:
        # List all time series in the dataset
        timeseries = client.time_series.list(
            data_set_ids=[dataset_id],
            limit=None
        )
        
        if not timeseries:
            print(f"    No time series found")
            return 0
        
        print(f"    Found {len(timeseries)} time series")
        
        # Delete in batches
        batch_size = 100
        deleted_count = 0
        
        for i in range(0, len(timeseries), batch_size):
            batch = timeseries[i:i+batch_size]
            ts_ids = [ts.id for ts in batch]
            
            try:
                client.time_series.delete(id=ts_ids)
                deleted_count += len(ts_ids)
                print(f"    Deleted {deleted_count}/{len(timeseries)} time series...")
            except Exception as e:
                print(f"    Warning: Could not delete some time series: {e}")
        
        print(f"    ✓ Deleted {deleted_count} time series")
        return deleted_count
        
    except Exception as e:
        print(f"    ✗ Error cleaning time series: {e}")
        return 0

def cleanup_sequences(client, dataset_id, dataset_name):
    """Delete all sequences in a dataset"""
    print(f"\n  Cleaning sequences from {dataset_name}...")
    
    try:
        # List all sequences in the dataset
        sequences = client.sequences.list(
            data_set_ids=[dataset_id],
            limit=None
        )
        
        if not sequences:
            print(f"    No sequences found")
            return 0
        
        print(f"    Found {len(sequences)} sequences")
        
        # Delete in batches
        batch_size = 100
        deleted_count = 0
        
        for i in range(0, len(sequences), batch_size):
            batch = sequences[i:i+batch_size]
            seq_ids = [seq.id for seq in batch]
            
            try:
                client.sequences.delete(id=seq_ids)
                deleted_count += len(seq_ids)
                print(f"    Deleted {deleted_count}/{len(sequences)} sequences...")
            except Exception as e:
                print(f"    Warning: Could not delete some sequences: {e}")
        
        print(f"    ✓ Deleted {deleted_count} sequences")
        return deleted_count
        
    except Exception as e:
        print(f"    ✗ Error cleaning sequences: {e}")
        return 0

def main():
    """Main cleanup function"""
    print("="*60)
    print("CDF DATASET CLEANUP TOOL")
    print("="*60)
    print("This will delete ALL data from the Plex datasets!")
    print("This is useful for testing the enhanced extractors with clean data.")
    print()
    
    # Get datasets
    datasets = get_dataset_ids()
    if not datasets:
        print("No datasets found in environment variables")
        return
    
    print("Found datasets:")
    for name, id in datasets.items():
        print(f"  - {name}: {id}")
    
    print()
    response = input("Are you sure you want to delete ALL data from these datasets? (yes/no): ")
    if response.lower() != 'yes':
        print("Cleanup cancelled")
        return
    
    print("\nSelect what to clean:")
    print("1. Everything (assets, events, time series, sequences)")
    print("2. Assets only")
    print("3. Events only")
    print("4. Time series only")
    print("5. Specific dataset only")
    
    choice = input("\nEnter choice (1-5): ")
    
    # Initialize CDF client
    print("\nConnecting to CDF...")
    try:
        client = init_cdf_client()
        project = client.iam.token.inspect()
        print(f"✓ Connected to project: {project.projects[0]}")
    except Exception as e:
        print(f"✗ Failed to connect to CDF: {e}")
        return
    
    # Track statistics
    total_assets = 0
    total_events = 0
    total_timeseries = 0
    total_sequences = 0
    
    if choice == '5':
        # Specific dataset
        print("\nAvailable datasets:")
        dataset_list = list(datasets.items())
        for i, (name, id) in enumerate(dataset_list, 1):
            print(f"{i}. {name}")
        
        dataset_choice = int(input("\nSelect dataset (number): ")) - 1
        if 0 <= dataset_choice < len(dataset_list):
            name, dataset_id = dataset_list[dataset_choice]
            datasets = {name: dataset_id}
        else:
            print("Invalid choice")
            return
    
    # Perform cleanup
    print("\nStarting cleanup...")
    start_time = time.time()
    
    for dataset_name, dataset_id in datasets.items():
        print(f"\n{'='*40}")
        print(f"Dataset: {dataset_name} ({dataset_id})")
        print(f"{'='*40}")
        
        if choice in ['1', '2']:
            count = cleanup_assets(client, dataset_id, dataset_name)
            total_assets += count
        
        if choice in ['1', '3']:
            count = cleanup_events(client, dataset_id, dataset_name)
            total_events += count
        
        if choice in ['1', '4']:
            count = cleanup_timeseries(client, dataset_id, dataset_name)
            total_timeseries += count
        
        if choice == '1':
            count = cleanup_sequences(client, dataset_id, dataset_name)
            total_sequences += count
    
    # Print summary
    duration = time.time() - start_time
    print("\n" + "="*60)
    print("CLEANUP COMPLETE")
    print("="*60)
    print(f"Time taken: {duration:.1f} seconds")
    print(f"Total deleted:")
    if choice in ['1', '2']:
        print(f"  - Assets: {total_assets}")
    if choice in ['1', '3']:
        print(f"  - Events: {total_events}")
    if choice in ['1', '4']:
        print(f"  - Time series: {total_timeseries}")
    if choice == '1':
        print(f"  - Sequences: {total_sequences}")
    print("\nDatasets are now clean and ready for testing enhanced extractors!")

if __name__ == "__main__":
    main()