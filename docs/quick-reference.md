# Plex to CDF Quick Reference Guide

## 🚀 Quick Start

### 1. First Time Setup
```bash
# Install dependencies
pip install cognite-sdk aiohttp python-dotenv

# Create root assets
python create_root_assets.py

# Test connections
python test_connections.py
```

### 2. Run Extractors
```bash
# Run all extractors
python orchestrator.py

# Run individual extractors
python master_data_extractor.py
python jobs_extractor.py
python inventory_extractor.py
```

### 3. Stop Extractors
```bash
# Find and kill orchestrator
ps aux | grep orchestrator | grep -v grep | awk '{print $2}' | xargs kill -9
```

## 🔧 Common Tasks

### Check What's Working
```bash
# Test all endpoints
python test_plex_equipment_api.py

# Test job events specifically
python test_job_events.py

# Check connections
python test_connections.py
```

### Debug Issues
```bash
# Check logs
python orchestrator.py 2>&1 | grep ERROR

# Check specific extractor
python orchestrator.py 2>&1 | grep -E "jobs|Jobs"

# Monitor in real-time
python orchestrator.py 2>&1 | grep -E "created|updated|completed"
```

## 📝 Key Files

| File | Purpose | Status |
|------|---------|--------|
| `base_extractor.py` | Base class for all extractors | ✅ Working |
| `master_data_extractor.py` | Parts, workcenters, buildings | ✅ Working |
| `jobs_extractor.py` | Job events (scheduled, in_progress, completed) | ✅ Working |
| `inventory_extractor.py` | Locations, containers, inventory levels | ✅ Working |
| `sequence_extractor.py` | Job routing sequences | ✅ Working |
| `production_extractor.py` | Production events | ⚠️ Partial |
| `quality_extractor.py` | Quality inspections | ⚠️ Issues |
| `orchestrator.py` | Runs all extractors | ✅ Working |
| `create_root_assets.py` | Creates hierarchy roots | ✅ Working |

## 🎯 Working API Endpoints

```python
# Parts (returns list directly)
/mdm/v1/parts

# Buildings (returns list directly)
/mdm/v1/buildings

# Operations (returns list directly)
/mdm/v1/operations

# Workcenters
/production/v1/production-definitions/workcenters

# Jobs (corrected endpoint)
/scheduling/v1/jobs  # NOT /production/v1/scheduling/jobs

# Inventory
/inventory/v1/inventory-tracking/containers
/inventory/v1/inventory-definitions/locations
```

## ⚠️ Common Issues & Fixes

### Issue: "Reference to unknown parent"
```bash
python create_root_assets.py
```

### Issue: "'list' object has no attribute 'get'"
API returns list directly, not wrapped in 'data':
```python
if isinstance(data, list):
    items = data
elif isinstance(data, dict):
    items = data.get('data', [])
```

### Issue: "Field value should be a number, not a string"
Events need numeric asset IDs, not external IDs. Currently disabled:
```python
asset_ids = []  # Disabled until we can resolve to numeric IDs
```

### Issue: Process won't stop
```bash
ps aux | grep python | grep orchestrator | awk '{print $2}' | xargs kill -9
```

## 📊 Data Model

### Asset Hierarchy
```
PCN340884_FACILITY_ROOT
├── PCN340884_BUILDING_{id}
│   └── PCN340884_WC_{id}         # Workcenters
├── PCN340884_PARTS_LIBRARY
│   └── PCN340884_PART_{id}       # Parts
├── PCN340884_OPERATIONS_ROOT
│   └── PCN340884_OPERATION_{id}  # Operations
├── PCN340884_INVENTORY_ROOT
│   └── PCN340884_LOCATION_{id}   # Locations
└── PCN340884_BOM_ROOT
    └── PCN340884_BOM_{id}         # BOMs
```

### Event Types
- **Jobs**: `type='job'`, subtypes: `scheduled`, `in_progress`, `completed`
- **Production**: `type='production'`, subtypes: `cycle_complete`, `scrap`
- **Quality**: `type='quality'`, subtypes: `inspection`, `ncr`
- **Inventory**: `type='inventory'`, subtypes: `receipt`, `issue`, `transfer`

### Datasets
- `PLEXMASTER` (4945797542267648): Master data
- `PLEXSCHEDULING` (7195113081024241): Job events
- `PLEXPRODUCTION` (869709519571885): Production events
- `PLEXINVENTORY` (8139681212033756): Inventory data
- `PLEXQUALITY`: Quality events (not set)
- `PLEXMAINTENANCE`: Maintenance (not set)

## 🔍 Monitor Extraction

### Check Status
```bash
# See what's being created/updated
python orchestrator.py 2>&1 | grep -E "created|updated"

# Monitor specific extractor
python orchestrator.py 2>&1 | grep -i inventory

# Check for errors
python orchestrator.py 2>&1 | grep ERROR
```

### View in CDF
1. Go to CDF Portal → Data management → Data sets
2. Look for datasets starting with `PLEX`
3. Check Assets, Events, and Time Series tabs

## 🛠️ Development Tips

### Always Use Base Extractor
```python
from base_extractor import BaseExtractor, BaseExtractorConfig

class YourExtractor(BaseExtractor):
    def get_required_datasets(self) -> List[str]:
        return ['master']  # or ['scheduling', 'production']
    
    async def extract(self):
        # Your extraction logic
```

### Handle API Response Formats
```python
data = await self.fetch_plex_data(endpoint)
if isinstance(data, list):
    items = data
elif isinstance(data, dict):
    items = data.get('data', [])
else:
    items = []
```

### Use Deduplication Helper
```python
result = self.dedup_helper.upsert_assets(assets)
print(f"Created: {len(result['created'])}, Updated: {len(result['updated'])}")
```

### NO RAW Tables
```python
# DON'T DO THIS
self.cognite_client.raw.rows.insert(...)

# DO THIS INSTEAD
self.dedup_helper.upsert_assets(assets)  # For Assets
self.dedup_helper.create_events_batch(events)  # For Events
```

## 📚 More Information

- [Implementation Status](./implementation-status.md) - Detailed status
- [Implementation Plan](../implementation_plan.md) - Original plan
- [Data Model](./plex-cdf-data-model.md) - Complete data model
- [API Documentation](./plex-api-reference.md) - Plex API details