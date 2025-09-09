# Plex Comprehensive Data Extraction Implementation Plan

## Feature Overview
Implement comprehensive data extraction from Plex MES covering master data, operational data, and inventory management with proper PCN support and CDF dataset organization.

## ⚠️ CRITICAL REQUIREMENTS
- **ALL extractors MUST be PCN-aware** - Every ID must include `PCN{number}_` prefix
- **Use MultiTenantNaming class** for all ID generation
- **Assign to correct datasets** based on data domain
- **Include facility metadata** in all CDF objects

## Data Categories & CDF Dataset Mapping

### 1. Master Data → `plex_master` dataset
- **Workcenters**: Manufacturing equipment and work areas
- **Buildings/Facilities**: Physical locations
- **Parts/Items**: Product definitions and specifications  
- **BOMs**: Bill of Materials hierarchies
- **Routings**: Manufacturing process definitions

### 2. Operational Data → `plex_production` & `plex_scheduling` datasets
- **Jobs**: Production orders → `plex_scheduling`
- **Operations**: Job operation details → `plex_scheduling`
- **Production History**: Historical production records → `plex_production`
- **Scrap Log**: Quality/scrap events → `plex_production`
- **Schedule**: Production schedules → `plex_scheduling`

### 3. Inventory Data → `plex_inventory` dataset
- **Containers**: Physical containers/bins
- **Locations**: Storage locations
- **Lots**: Material lot tracking
- **Inventory Movements**: Material transactions
- **WIP**: Work in Progress tracking
- **Container Status**: Real-time container status

## Plex API Endpoints Analysis

### Master Data Endpoints
```yaml
# Workcenters
GET /manufacturing/v1/workcenters
GET /manufacturing/v1/workcenters/{workcenterId}

# Parts/Items  
GET /inventory/v1/parts
GET /inventory/v1/parts/{partId}

# BOMs
GET /engineering/v1/boms
GET /engineering/v1/boms/{bomId}/components

# Routings
GET /engineering/v1/routings
GET /engineering/v1/routings/{routingId}/operations

# Buildings/Facilities
GET /facilities/v1/buildings
GET /facilities/v1/buildings/{buildingId}/areas
```

### Operational Data Endpoints
```yaml
# Jobs (existing)
GET /scheduling/v1/jobs
GET /scheduling/v1/jobs/{jobId}/operations

# Production History
GET /production/v1/production-history
  params: dateFrom, dateTo, workcenterId

# Scrap Log
GET /quality/v1/scrap-entries
  params: dateFrom, dateTo, reasonCode

# Schedule
GET /scheduling/v1/production-schedule
GET /scheduling/v1/schedule-adherence
```

### Inventory Endpoints
```yaml
# Containers
GET /inventory/v1/containers
GET /inventory/v1/containers/{containerId}/status

# Locations
GET /inventory/v1/locations
GET /inventory/v1/locations/{locationId}/inventory

# Lots
GET /inventory/v1/lots
GET /inventory/v1/lots/{lotId}/transactions

# Inventory Movements
GET /inventory/v1/movements
  params: dateFrom, dateTo, movementType

# WIP
GET /inventory/v1/wip
GET /inventory/v1/wip/{jobId}

# Container Status (real-time)
GET /inventory/v1/containers/{containerId}/real-time-status
```

## Implementation Architecture

### 1. Master Data Extractor (`master_data_extractor.py`)

```python
class MasterDataExtractor:
    """
    Extracts and maintains master data in CDF
    - Runs daily or on-demand
    - Creates asset hierarchies
    - Updates reference data in RAW tables
    """
    
    async def extract_workcenters():
        # Create workcenter assets with PCN prefix
        # Hierarchy: PCN{pcn}_FACILITY → PCN{pcn}_BUILDING_{id} → PCN{pcn}_WC_{id}
        
    async def extract_parts():
        # Store in RAW tables for reference
        # Table: plex_{pcn}_parts
        
    async def extract_boms():
        # Create BOM hierarchy as assets
        # Parent-child relationships
        
    async def extract_routings():
        # Store routing definitions
        # Link to workcenters and operations
```

### 2. Operational Data Extractor (enhance existing)

```python
class OperationalDataExtractor:
    """
    Enhanced production and scheduling data extraction
    - Builds on existing jobs_extractor_v2.py
    - Adds production history and scrap tracking
    """
    
    async def extract_production_history():
        # Historical production records as events
        # Time series for production metrics
        
    async def extract_scrap_log():
        # Scrap events with reason codes
        # Quality metrics time series
        
    async def extract_schedule():
        # Schedule data as events
        # Schedule adherence metrics
```

### 3. Inventory Extractor (`inventory_extractor.py`)

```python
class InventoryExtractor:
    """
    Real-time and historical inventory data
    - Container tracking
    - Material movements
    - WIP monitoring
    """
    
    async def extract_containers():
        # Container assets with current status
        # Real-time location tracking
        
    async def extract_inventory_movements():
        # Movement events with full traceability
        # Material flow time series
        
    async def extract_wip():
        # WIP levels by job/workcenter
        # WIP value calculations
```

## CDF Data Models

### Asset Hierarchies

```
PCN{pcn}_ENTERPRISE
├── PCN{pcn}_FACILITY
│   ├── PCN{pcn}_BUILDING_{id}
│   │   ├── PCN{pcn}_WC_{id} (Workcenters)
│   │   └── PCN{pcn}_LOCATION_{id} (Storage)
│   └── PCN{pcn}_PRODUCTION_SCHEDULE
│       └── PCN{pcn}_JOB_{id}
└── PCN{pcn}_INVENTORY
    ├── PCN{pcn}_CONTAINER_{id}
    └── PCN{pcn}_LOT_{id}
```

### Event Types

```yaml
Master Data Events:
  - master_data_update (when reference data changes)
  - bom_revision (BOM updates)
  - routing_change (process changes)

Operational Events:
  - production_entry (existing)
  - scrap_entry (quality issues)
  - schedule_update (schedule changes)
  - job_lifecycle (existing)

Inventory Events:
  - material_movement (transfers)
  - inventory_adjustment (counts)
  - container_status_change
  - lot_transaction
```

### Time Series

```yaml
Production Metrics:
  - PCN{pcn}_TS_WC_{id}_THROUGHPUT
  - PCN{pcn}_TS_WC_{id}_SCRAP_RATE
  - PCN{pcn}_TS_WC_{id}_OEE

Inventory Metrics:
  - PCN{pcn}_TS_LOC_{id}_INVENTORY_LEVEL
  - PCN{pcn}_TS_PART_{id}_WIP_QUANTITY
  - PCN{pcn}_TS_CONTAINER_{id}_FILL_LEVEL

Schedule Metrics:
  - PCN{pcn}_TS_SCHEDULE_ADHERENCE
  - PCN{pcn}_TS_SCHEDULE_EFFICIENCY
```

### RAW Tables

```yaml
Master Data Tables:
  Database: plex_{pcn}_master
  Tables:
    - parts (part definitions)
    - boms (BOM structures)
    - routings (process definitions)
    - workcenter_specs (equipment specs)
    
Reference Tables:
  Database: plex_{pcn}_reference
  Tables:
    - scrap_reasons (reason codes)
    - movement_types (transaction types)
    - unit_of_measure (UOM conversions)
```

## Implementation Phases

### Phase 1: Master Data Foundation (Week 1)
1. Create `master_data_extractor.py` with PCN support
2. Implement workcenter and building extraction
3. Create asset hierarchies in CDF
4. Set up RAW tables for parts and BOMs

### Phase 2: Enhanced Operational Data (Week 2)
1. Extend `production_extractor_v2.py` with history
2. Add scrap log extraction and events
3. Implement schedule extraction
4. Create quality metrics time series

### Phase 3: Inventory Management (Week 3)
1. Create `inventory_extractor.py`
2. Implement container and location tracking
3. Add material movement events
4. Create WIP monitoring dashboards

### Phase 4: Integration & Testing (Week 4)
1. Cross-reference validation between domains
2. Performance optimization for large datasets
3. Error handling and retry logic
4. Documentation and deployment

## Configuration Updates

### Environment Variables (.env)
```bash
# Master Data Configuration
MASTER_EXTRACTION_INTERVAL=86400  # Daily (24 hours)
MASTER_BATCH_SIZE=10000

# Inventory Configuration  
INVENTORY_EXTRACTION_INTERVAL=300  # 5 minutes
CONTAINER_IDS=C001,C002,C003  # Containers to track
LOCATION_IDS=L001,L002,L003  # Locations to monitor

# API Rate Limits
PLEX_API_RATE_LIMIT=100  # Requests per minute
PLEX_API_CONCURRENT=5  # Concurrent connections
```

### Dataset Configuration
```python
# Ensure datasets exist for each domain
datasets = {
    'master': CDF_DATASET_PLEXMASTER,
    'production': CDF_DATASET_PLEXPRODUCTION,
    'scheduling': CDF_DATASET_PLEXSCHEDULING,
    'inventory': CDF_DATASET_PLEXINVENTORY,
    'quality': CDF_DATASET_PLEXQUALITY
}
```

## Testing Strategy

### Unit Tests
- Test PCN prefix generation for all ID types
- Validate data transformations
- Mock API responses for offline testing

### Integration Tests
- Test with multiple PCNs simultaneously
- Verify dataset assignments
- Check cross-references between domains

### Performance Tests
- Large dataset handling (>100k records)
- API rate limit compliance
- Memory usage optimization

## Success Criteria

1. ✅ All extractors include PCN prefixes
2. ✅ Data correctly assigned to domain datasets
3. ✅ No data collision between facilities
4. ✅ Complete asset hierarchies created
5. ✅ All events include proper metadata
6. ✅ Time series data flowing correctly
7. ✅ RAW tables populated with master data
8. ✅ Error handling for API failures
9. ✅ Incremental extraction working
10. ✅ Documentation complete

## Risk Mitigation

### API Limitations
- **Risk**: Plex API rate limits or missing endpoints
- **Mitigation**: Implement adaptive rate limiting, cache responses

### Data Volume
- **Risk**: Large historical data causing timeouts
- **Mitigation**: Chunk requests by date range, parallel processing

### Data Quality
- **Risk**: Inconsistent or missing data from Plex
- **Mitigation**: Validation rules, default values, error logging

### PCN Changes
- **Risk**: Facility changes causing data issues
- **Mitigation**: Strict PCN validation, separate state management

## Next Steps

1. Review Plex API documentation for exact endpoint specifications
2. Create `master_data_extractor.py` with workcenter extraction
3. Update existing extractors with new operational endpoints
4. Implement inventory tracking system
5. Create monitoring dashboards in CDF

## Dependencies

- Existing `MultiTenantNaming` class for ID generation
- `jobs_extractor_v2.py` and `production_extractor_v2.py` as base
- CDF datasets already created
- Plex API access with appropriate permissions
- Environment variables configured

## Validation Checklist

- [ ] All IDs include PCN prefix
- [ ] Correct dataset assignment per domain
- [ ] Facility metadata in all objects
- [ ] Error handling implemented
- [ ] Incremental extraction working
- [ ] Documentation updated
- [ ] Tests passing
- [ ] Performance acceptable