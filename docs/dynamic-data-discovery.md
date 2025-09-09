# Dynamic Data Discovery

## Overview

The extractors now support **automatic discovery** of data from Plex, eliminating the need to hardcode specific IDs in the configuration. When no IDs are specified, the extractors will fetch ALL available data for the PCN.

## How It Works

### Production & Master Data Extractors

Both extractors now automatically fetch all workcenters using:
```
GET https://connect.plex.com/production/v1/production-definitions/workcenters
```

**Configuration:**
```bash
# Leave empty to fetch ALL workcenters for the PCN
WORKCENTER_IDS=

# Or specify specific workcenters to limit extraction
WORKCENTER_IDS=WC001,WC002,WC003
```

**Behavior:**
- If `WORKCENTER_IDS` is **empty**: Fetches all active workcenters from the API
- If `WORKCENTER_IDS` is **specified**: Only processes the listed workcenters

### Inventory Extractor

Already supports dynamic discovery for containers and locations:

**Configuration:**
```bash
# Leave empty to fetch ALL containers
CONTAINER_IDS=

# Leave empty to fetch ALL locations  
LOCATION_IDS=
```

**API Endpoints Used:**
- Containers: `GET /inventory/v1/containers`
- Locations: `GET /inventory/v1/locations`

## Benefits

### 1. Automatic Discovery
- No need to know workcenter IDs in advance
- Automatically includes new workcenters as they're added
- Reduces configuration complexity

### 2. Complete Data Coverage
- Ensures no workcenters are missed
- Gets all active production resources
- Comprehensive inventory tracking

### 3. PCN-Specific
- Only fetches data for the configured PCN
- Maintains complete data isolation between facilities
- Supports multi-facility deployments

## Implementation Details

### Production Extractor

```python
async def fetch_all_workcenters(self):
    """Fetch all workcenters for the PCN from Plex API"""
    data = await self.fetch_plex_data("/production/v1/production-definitions/workcenters")
    
    workcenters = []
    for wc in data:
        if wc.get('status') == 'Active':
            workcenters.append({
                'id': wc.get('id'),
                'name': wc.get('name'),
                'type': wc.get('type')
            })
    
    return workcenters

async def extract_workcenter_status(self):
    # If no IDs configured, fetch all
    if not self.config.workcenter_ids:
        workcenters = await self.fetch_all_workcenters()
        workcenter_ids = [wc['id'] for wc in workcenters]
    else:
        workcenter_ids = self.config.workcenter_ids
```

### Asset Creation

When dynamically fetching workcenters, the extractors also capture:
- Workcenter name (from API)
- Workcenter type
- Status (only processes Active workcenters)

This creates more accurate asset metadata in CDF:
```python
Asset(
    external_id=f"PCN{pcn}_WC_{wc_id}",
    name=wc_info.get('name'),  # Actual name from API
    metadata={
        'workcenter_type': wc_info.get('type'),
        'status': 'Active'
    }
)
```

## Configuration Examples

### Fetch Everything (Recommended)
```bash
# .env
WORKCENTER_IDS=
CONTAINER_IDS=
LOCATION_IDS=
```

### Mixed Mode
```bash
# Fetch all workcenters but specific containers
WORKCENTER_IDS=
CONTAINER_IDS=C001,C002,C003
LOCATION_IDS=L001,L002
```

### Specific Resources Only
```bash
# Only extract specific resources
WORKCENTER_IDS=WC001,WC002
CONTAINER_IDS=C001
LOCATION_IDS=L001
```

## Testing Dynamic Discovery

### Check What Will Be Fetched

```python
# test_discovery.py
import asyncio
from production_extractor import PlexProductionExtractor, ProductionConfig

async def test():
    config = ProductionConfig.from_env()
    extractor = PlexProductionExtractor(config)
    
    workcenters = await extractor.fetch_all_workcenters()
    print(f"Found {len(workcenters)} workcenters:")
    for wc in workcenters:
        print(f"  - {wc['id']}: {wc['name']} ({wc['type']})")

asyncio.run(test())
```

### Run with Discovery

```bash
# Clear workcenter IDs to enable discovery
export WORKCENTER_IDS=""

# Run extractor
python production_extractor.py
```

## Performance Considerations

### Initial Load
- First run will take longer as it discovers all resources
- Subsequent runs use cached asset information
- Consider using state tracking to avoid re-processing

### API Rate Limits
- Discovery adds initial API calls
- Once discovered, normal extraction proceeds
- Rate limiting still applies (100 req/min default)

### Large Facilities
- Facilities with many workcenters will generate more data
- Use batch processing (already implemented)
- Monitor memory usage for very large extractions

## Troubleshooting

### No Workcenters Found
```
WARNING: No workcenters found
```
**Solutions:**
- Verify PCN is correct
- Check API credentials have proper permissions
- Ensure workcenters exist and are Active

### Partial Data
If some workcenters are missing:
1. Check their status (only Active are fetched)
2. Verify API response includes all expected fields
3. Enable debug logging to see raw API responses

### Performance Issues
For facilities with hundreds of workcenters:
1. Consider specifying critical workcenters only
2. Increase batch sizes
3. Run extractors separately (not all at once)

## Future Enhancements

1. **Caching Discovery Results**
   - Cache workcenter list for X hours
   - Reduce API calls on subsequent runs

2. **Filtering Options**
   - Filter by workcenter type
   - Filter by department or area
   - Support wildcards (e.g., "WC*")

3. **Discovery for Other Entities**
   - Auto-discover parts
   - Auto-discover jobs
   - Auto-discover quality specifications

4. **Smart Discovery**
   - Only fetch changed workcenters
   - Track additions/removals
   - Alert on configuration changes