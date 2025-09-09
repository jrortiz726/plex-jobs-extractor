#!/usr/bin/env python3
"""
Setup CDF Datasets for Plex MES Data Integration

This script creates the required datasets in Cognite Data Fusion for organizing
Plex MES data by functional domain (production, scheduling, quality, etc.)

Usage:
    python setup_datasets.py [--delete-existing]
"""

import os
import sys
import argparse
import logging
from datetime import datetime
from typing import List, Dict
from dotenv import load_dotenv

from cognite.client import CogniteClient
from cognite.client.config import ClientConfig
from cognite.client.credentials import OAuthClientCredentials
from cognite.client.data_classes import DataSet, DataSetWrite
from cognite.client.exceptions import CogniteAPIError

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DatasetManager:
    """Manages CDF dataset creation and configuration for Plex MES data"""
    
    def __init__(self):
        self.client = self._init_cognite_client()
        self.datasets_config = self._get_datasets_config()
        
    def _init_cognite_client(self) -> CogniteClient:
        """Initialize Cognite client with proper authentication"""
        creds = OAuthClientCredentials(
            token_url=os.getenv('CDF_TOKEN_URL'),
            client_id=os.getenv('CDF_CLIENT_ID'),
            client_secret=os.getenv('CDF_CLIENT_SECRET'),
            scopes=["user_impersonation"]  # Using the working scope
        )
        
        config = ClientConfig(
            client_name="plex-dataset-setup",
            base_url=os.getenv('CDF_HOST'),
            project=os.getenv('CDF_PROJECT'),
            credentials=creds
        )
        
        return CogniteClient(config)
    
    def _get_datasets_config(self) -> List[Dict]:
        """Define all datasets to be created"""
        return [
            {
                "external_id": "plex_production",
                "name": "Plex Production Operations",
                "description": "Real-time production operations data from Plex MES including workcenter status, "
                              "production entries, OEE metrics, and throughput measurements",
                "metadata": {
                    "source": "Plex MES",
                    "domain": "production",
                    "update_frequency": "1-5 minutes",
                    "data_types": "time_series,events,assets",
                    "owner": "Production Team",
                    "created_date": datetime.now().isoformat()
                }
            },
            {
                "external_id": "plex_scheduling",
                "name": "Plex Job Scheduling",
                "description": "Job scheduling and planning data including production orders, job operations, "
                              "and schedule adherence metrics",
                "metadata": {
                    "source": "Plex MES",
                    "domain": "scheduling",
                    "update_frequency": "10-15 minutes",
                    "data_types": "assets,events,time_series",
                    "owner": "Planning Team",
                    "created_date": datetime.now().isoformat()
                }
            },
            {
                "external_id": "plex_quality",
                "name": "Plex Quality Management",
                "description": "Quality management data including inspections, non-conformances, "
                              "defect rates, and quality KPIs",
                "metadata": {
                    "source": "Plex MES",
                    "domain": "quality",
                    "update_frequency": "on_event",
                    "data_types": "events,time_series,assets",
                    "owner": "Quality Team",
                    "created_date": datetime.now().isoformat()
                }
            },
            {
                "external_id": "plex_inventory",
                "name": "Plex Inventory Management",
                "description": "Inventory and material management data including stock levels, "
                              "material movements, consumption rates, and warehouse operations",
                "metadata": {
                    "source": "Plex MES",
                    "domain": "inventory",
                    "update_frequency": "15-30 minutes",
                    "data_types": "time_series,events,assets",
                    "owner": "Supply Chain Team",
                    "created_date": datetime.now().isoformat()
                }
            },
            {
                "external_id": "plex_maintenance",
                "name": "Plex Maintenance Management",
                "description": "Equipment maintenance data including work orders, preventive maintenance, "
                              "equipment failures, and maintenance KPIs",
                "metadata": {
                    "source": "Plex MES",
                    "domain": "maintenance",
                    "update_frequency": "on_event",
                    "data_types": "events,time_series,assets",
                    "owner": "Maintenance Team",
                    "created_date": datetime.now().isoformat()
                }
            },
            {
                "external_id": "plex_master",
                "name": "Plex Master Data",
                "description": "Master data and reference information including plant hierarchy, "
                              "equipment registry, part numbers, BOMs, and routing definitions",
                "metadata": {
                    "source": "Plex MES",
                    "domain": "master_data",
                    "update_frequency": "daily",
                    "data_types": "assets,raw_tables",
                    "owner": "Data Management Team",
                    "created_date": datetime.now().isoformat()
                }
            }
        ]
    
    def check_existing_datasets(self) -> Dict[str, DataSet]:
        """Check for existing datasets"""
        logger.info("Checking for existing datasets...")
        existing = {}
        
        for config in self.datasets_config:
            try:
                dataset = self.client.data_sets.retrieve(external_id=config["external_id"])
                if dataset:
                    existing[config["external_id"]] = dataset
                    logger.info(f"  Found existing dataset: {config['external_id']} (ID: {dataset.id})")
            except:
                pass
        
        return existing
    
    def delete_datasets(self, datasets: List[DataSet]):
        """Delete specified datasets"""
        if not datasets:
            return
            
        logger.warning(f"Deleting {len(datasets)} existing datasets...")
        for dataset in datasets:
            try:
                self.client.data_sets.delete(id=dataset.id)
                logger.info(f"  Deleted dataset: {dataset.external_id}")
            except Exception as e:
                logger.error(f"  Failed to delete {dataset.external_id}: {e}")
    
    def create_datasets(self) -> List[DataSet]:
        """Create all configured datasets"""
        logger.info("Creating datasets...")
        created_datasets = []
        
        for config in self.datasets_config:
            try:
                dataset = DataSetWrite(
                    external_id=config["external_id"],
                    name=config["name"],
                    description=config["description"],
                    metadata=config["metadata"]
                )
                
                created = self.client.data_sets.create(dataset)
                created_datasets.append(created)
                logger.info(f"  Created dataset: {config['external_id']} (ID: {created.id})")
                
            except CogniteAPIError as e:
                if "already exists" in str(e).lower():
                    logger.warning(f"  Dataset already exists: {config['external_id']}")
                    # Retrieve the existing dataset
                    existing = self.client.data_sets.retrieve(external_id=config["external_id"])
                    if existing:
                        created_datasets.append(existing)
                else:
                    logger.error(f"  Failed to create {config['external_id']}: {e}")
            except Exception as e:
                logger.error(f"  Unexpected error creating {config['external_id']}: {e}")
        
        return created_datasets
    
    def generate_env_variables(self, datasets: List[DataSet]):
        """Generate environment variable exports for dataset IDs"""
        logger.info("\n" + "="*60)
        logger.info("Environment Variables for .env file:")
        logger.info("="*60)
        
        env_lines = []
        for dataset in datasets:
            if dataset:
                env_var_name = f"CDF_DATASET_{dataset.external_id.upper().replace('_', '')}"
                env_lines.append(f"{env_var_name}={dataset.id}")
                print(f"{env_var_name}={dataset.id}")
        
        # Write to env template file
        env_file = ".env.datasets"
        with open(env_file, "w") as f:
            f.write("# CDF Dataset IDs - Add these to your .env file\n")
            f.write(f"# Generated on {datetime.now().isoformat()}\n\n")
            for line in env_lines:
                f.write(f"{line}\n")
        
        logger.info(f"\nEnvironment variables also saved to: {env_file}")
        logger.info("Add these to your .env file to use datasets in extractors")
    
    def verify_datasets(self, datasets: List[DataSet]):
        """Verify created datasets and show summary"""
        logger.info("\n" + "="*60)
        logger.info("Dataset Summary:")
        logger.info("="*60)
        
        for dataset in datasets:
            if dataset:
                logger.info(f"\n{dataset.name}:")
                logger.info(f"  External ID: {dataset.external_id}")
                logger.info(f"  Internal ID: {dataset.id}")
                logger.info(f"  Description: {dataset.description[:100]}...")
                if dataset.metadata:
                    logger.info(f"  Domain: {dataset.metadata.get('domain', 'N/A')}")
                    logger.info(f"  Update Frequency: {dataset.metadata.get('update_frequency', 'N/A')}")
                    logger.info(f"  Data Types: {dataset.metadata.get('data_types', 'N/A')}")
    
    def run(self, delete_existing: bool = False):
        """Main execution flow"""
        try:
            logger.info("Starting CDF Dataset Setup for Plex MES Integration")
            logger.info(f"CDF Project: {os.getenv('CDF_PROJECT')}")
            logger.info(f"CDF Host: {os.getenv('CDF_HOST')}")
            
            # Check for existing datasets
            existing = self.check_existing_datasets()
            
            if existing and delete_existing:
                response = input(f"\nDelete {len(existing)} existing datasets? (yes/no): ")
                if response.lower() == 'yes':
                    self.delete_datasets(list(existing.values()))
                else:
                    logger.info("Keeping existing datasets")
            
            # Create datasets
            datasets = self.create_datasets()
            
            if datasets:
                # Generate environment variables
                self.generate_env_variables(datasets)
                
                # Verify and summarize
                self.verify_datasets(datasets)
                
                logger.info("\n" + "="*60)
                logger.info("✅ Dataset setup completed successfully!")
                logger.info("="*60)
            else:
                logger.warning("No datasets were created")
                
        except Exception as e:
            logger.error(f"Dataset setup failed: {e}")
            sys.exit(1)


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="Setup CDF datasets for Plex MES data integration"
    )
    parser.add_argument(
        "--delete-existing",
        action="store_true",
        help="Delete existing datasets before creating new ones"
    )
    
    args = parser.parse_args()
    
    # Validate environment variables
    required_vars = ['CDF_HOST', 'CDF_PROJECT', 'CDF_CLIENT_ID', 'CDF_CLIENT_SECRET', 'CDF_TOKEN_URL']
    missing = [var for var in required_vars if not os.getenv(var)]
    
    if missing:
        logger.error(f"Missing required environment variables: {', '.join(missing)}")
        logger.error("Please ensure .env file is configured properly")
        sys.exit(1)
    
    # Run dataset setup
    manager = DatasetManager()
    manager.run(delete_existing=args.delete_existing)


if __name__ == "__main__":
    main()