# Plex-CDF Enhanced Extractors - Current Status

## Overview
This project implements enhanced data extractors that pull data from Plex Manufacturing Cloud and push it to Cognite Data Fusion (CDF). The enhanced extractors are production-ready with async operations, error handling, retry logic, and hierarchical data modeling.

## Current Working Status (as of 2025-09-10)

### ✅ Working Extractors

1. **Jobs Extractor** (`jobs_extractor_enhanced.py`)
   - Fetches scheduling/job data from Plex
   - Creates events in CDF with job details
   - Uses job numbers as IDs (not UUIDs)
   - Handles priority as string values (Low/Medium/High)
   - Status: WORKING

2. **Master Data Extractor** (`master_data_extractor_enhanced.py`)
   - Fetches parts, operations, resources from Plex
   - Creates hierarchical asset structure in CDF
   - BOMs endpoint: `/engineering/v1/boms` (requires partId parameter)
   - Routings: No REST API available (skipped)
   - Resources: Uses `/production/v1/production-definitions/workcenters`
   - Status: WORKING

3. **Production Extractor** (`production_extractor_enhanced.py`)
   - Fetches production entries and workcenter status
   - Creates workcenter status events
   - Uses human-readable workcenter names (e.g., "NFF-01 - Liquid Nitrogen Flash Freezer")
   - Status: WORKING

4. **Inventory Extractor** (`inventory_extractor_enhanced.py`)
   - Fetches locations, containers, WIP data
   - Container movements: API returns 400 (handled gracefully, optional)
   - Creates hierarchical location/container assets
   - Status: WORKING (movements skipped)

5. **Quality Extractor** (`quality_extractor_enhanced.py`)
   - Uses Data Source API (not REST API)
   - Checksheets: DataSource ID 4142
   - Specifications: DataSource ID 6429
   - NCR: No data source available (skipped)
   - Status: PARTIALLY WORKING (Data Source API authentication issues)

6. **Performance Extractor** (`performance_extractor_enhanced.py`)
   - Separate extractor for OEE metrics
   - Uses Data Source API IDs 18765 and 22870
   - Creates time series for OEE, Availability, Performance, Quality
   - Status: NOT TESTED (requires Data Source API access)

### 🔧 Key Fixes Applied

1. **Authentication**
   - OAuth scope must be `["user_impersonation"]` not `.default`
   - CDF client properly configured with OAuth credentials

2. **API Endpoints**
   - Production entries: `beginDate`/`endDate` (not `dateFrom`/`dateTo`)
   - Workcenter status: `/production/v1/control/workcenters/{workcenterId}`
   - Locations: `/inventory/v1/inventory-definitions/locations`
   - Containers: `/inventory/v1/inventory-tracking/containers`
   - Movements: `/inventory/v1/inventory-history/container-location-moves`
   - BOMs: `/engineering/v1/boms` (requires partId parameter)

3. **Data Model**
   - Facility asset must exist first (parent for all other assets)
   - Assets use human-readable names instead of UUIDs
   - Hierarchical structure maintained properly

4. **Critical Issues Fixed**
   - `extraction_interval` max increased to 86400 seconds (24 hours)
   - Job priority handles string values (Low/Medium/High)
   - AsyncCDFWrapper now has `create_time_series` method
   - Cleanup script handles hierarchical deletion

## Environment Variables (.env)

```bash
# CDF Configuration
CDF_HOST=https://westeurope-1.cognitedata.com
CDF_PROJECT=essc-sandbox-44
CDF_CLIENT_ID=<your_client_id>
CDF_CLIENT_SECRET=<your_client_secret>
CDF_TOKEN_URL=https://datamosaix-prod.us.auth0.com/oauth/token
CDF_CLUSTER=westeurope-1

# Plex Configuration
PLEX_API_KEY=<your_api_key>
PLEX_CUSTOMER_ID=340884
PLEX_PCN_CODE=ra-process

# Dataset IDs (created by setup_datasets.py)
CDF_DATASET_PLEXPRODUCTION=869709519571885
CDF_DATASET_PLEXSCHEDULING=7195113081024241
CDF_DATASET_PLEXQUALITY=2881941287917280
CDF_DATASET_PLEXINVENTORY=8139681212033756
CDF_DATASET_PLEXMAINTENANCE=5080535668683118
CDF_DATASET_PLEXMASTER=4945797542267648

# Data Source API (for Quality/Performance)
PLEX_DS_USERNAME=<username>
PLEX_DS_PASSWORD=<password>
PLEX_USE_TEST=false

# Extraction Intervals
MASTER_DATA_EXTRACTION_INTERVAL=86400  # 24 hours
JOBS_EXTRACTION_INTERVAL=300           # 5 minutes
PRODUCTION_EXTRACTION_INTERVAL=300     # 5 minutes
INVENTORY_EXTRACTION_INTERVAL=300      # 5 minutes
QUALITY_EXTRACTION_INTERVAL=300        # 5 minutes
PERFORMANCE_EXTRACTION_INTERVAL=900    # 15 minutes

# Orchestrator Settings
ENABLE_JOBS_EXTRACTOR=true
ENABLE_PRODUCTION_EXTRACTOR=true
ENABLE_INVENTORY_EXTRACTOR=true
ENABLE_MASTER_DATA_EXTRACTOR=true
ENABLE_QUALITY_EXTRACTOR=false  # Disable if Data Source API not working
ENABLE_PERFORMANCE_EXTRACTOR=false  # Requires Data Source API
```

## Known Issues & Limitations

1. **Container Movements API**
   - Returns 400 Bad Request even with correct parameters
   - Might not be available for the demo environment
   - Handled gracefully - extraction continues without movements

2. **Data Source API**
   - Quality and Performance extractors require Data Source API
   - May have authentication or permission issues
   - IDs documented but may need validation

3. **Asset Linking**
   - Some events reference assets that don't exist yet
   - May need to run master data extractor first
   - Part assets need to be created before job events can link to them

4. **Facility Asset**
   - Must be created first before any extraction
   - All other assets depend on it as root parent
   - May need manual creation after cleanup

## Running the Extractors

### Full Orchestrator
```bash
python orchestrator_enhanced.py
```

### Individual Extractors
```bash
python jobs_extractor_enhanced.py
python master_data_extractor_enhanced.py
python production_extractor_enhanced.py
python inventory_extractor_enhanced.py
python quality_extractor_enhanced.py  # Requires Data Source API
python performance_extractor_enhanced.py  # Requires Data Source API
```

### Cleanup & Reset
```bash
# Clean all datasets
python cleanup_datasets.py

# After cleanup, manually create facility asset
python -c "
import os
from dotenv import load_dotenv
from cognite.client import CogniteClient
from cognite.client.config import ClientConfig
from cognite.client.credentials import OAuthClientCredentials
from cognite.client.data_classes import Asset

load_dotenv()

creds = OAuthClientCredentials(
    token_url=os.getenv('CDF_TOKEN_URL'),
    client_id=os.getenv('CDF_CLIENT_ID'),
    client_secret=os.getenv('CDF_CLIENT_SECRET'),
    scopes=['user_impersonation']
)

client = CogniteClient(
    ClientConfig(
        client_name='facility-creator',
        base_url=os.getenv('CDF_HOST'),
        project=os.getenv('CDF_PROJECT'),
        credentials=creds
    )
)

facility_asset = Asset(
    external_id='PCN340884_facility_340884',
    name='RADEMO - Facility',
    description='Facility for PCN 340884',
    metadata={
        'pcn': '340884',
        'facility_name': 'RADEMO',
        'facility_code': 'RA-UNIFIED-DEMO',
        'type': 'facility'
    },
    data_set_id=int(os.getenv('CDF_DATASET_PLEXMASTER'))
)

client.assets.create(facility_asset)
print('Created facility asset')
"
```

## Testing Status

### Test Commands
```bash
# Test with dry run
DRY_RUN=true RUN_ONCE=true python orchestrator_enhanced.py

# Test single extractor
RUN_ONCE=true ENABLE_JOBS_EXTRACTOR=true ENABLE_MASTER_DATA_EXTRACTOR=false \
ENABLE_PRODUCTION_EXTRACTOR=false ENABLE_INVENTORY_EXTRACTOR=false \
ENABLE_QUALITY_EXTRACTOR=false timeout 10 python orchestrator_enhanced.py

# Check what's in CDF
python -c "
from cleanup_datasets import init_cdf_client
client = init_cdf_client()
assets = client.assets.list(external_id_prefix='PCN340884_', limit=100)
print(f'Assets: {len(assets)}')
events = client.events.list(external_id_prefix='PCN340884_', limit=100)
print(f'Events: {len(events)}')
"
```

## Architecture Notes

1. **Base Classes**
   - `BaseExtractor`: Common functionality for all extractors
   - `AsyncCDFWrapper`: Async operations for CDF client
   - `PlexAPIClient`: Handles Plex API authentication and requests

2. **Error Handling**
   - Retry logic with exponential backoff
   - Circuit breaker pattern for repeated failures
   - Graceful degradation for optional features

3. **Data Flow**
   - Plex API → Extractor → Transform → CDF
   - Hierarchical asset structure maintained
   - Events linked to assets where applicable

4. **Performance**
   - Async/await for concurrent operations
   - Batch processing for large datasets
   - Connection pooling for HTTP requests

## Next Steps

1. **Resolve Data Source API Issues**
   - Validate credentials and permissions
   - Test with correct Data Source IDs
   - Enable quality and performance extractors

2. **Improve Asset Linking**
   - Ensure master data runs first
   - Implement asset resolution/waiting logic
   - Add validation for required assets

3. **Production Deployment**
   - Set up proper logging infrastructure
   - Implement monitoring and alerting
   - Configure appropriate extraction intervals

4. **Data Validation**
   - Verify data completeness
   - Validate transformations
   - Ensure proper time series data

## Files Overview

- `orchestrator_enhanced.py`: Main orchestrator managing all extractors
- `base_extractor_enhanced.py`: Base class with common functionality
- `*_extractor_enhanced.py`: Individual enhanced extractors
- `cleanup_datasets.py`: Utility to clean CDF datasets (with hierarchical deletion)
- `setup_datasets.py`: Creates required CDF datasets
- `test_connections.py`: Tests Plex and CDF connectivity
- `.env`: Configuration (not in git)
- `FIXES_APPLIED.md`: Detailed list of all fixes
- `claude.md`: This file - status for future Claude sessions