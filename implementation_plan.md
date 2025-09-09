# Plex to CDF Implementation Plan

## Current State Assessment

### Available Plex APIs
Based on our testing, the following REST API endpoints are confirmed available:
- ✅ `/mdm/v1/parts` - Parts master data
- ✅ `/mdm/v1/buildings` - Building/facility data  
- ✅ `/mdm/v1/operations` - Operation definitions
- ✅ `/mdm/v1/part-operations` - Part routings
- ✅ `/production/v1/production-definitions/workcenters` - Workcenters
- ✅ `/production/v1/production-history/workcenter-status-entries` - Status
- ✅ `/production/v1/production-history/production-entries` - Production logs
- ✅ `/production/v1/scheduling/jobs` - Jobs
- ✅ `/engineering/v1/boms` - Bill of materials
- ✅ `/inventory/v1/inventory-tracking/containers` - Containers
- ✅ `/inventory/v1/inventory-definitions/locations` - Locations

### Available Data Source APIs (Quality)
Self-serviceable quality data sources:
- ✅ Checksheets_Get (4142)
- ✅ Specification_Get (6429)
- ✅ Control_Plan_Lines_Export (233636)
- ✅ Inspection_Modes_Get (4760)
- ✅ Sample_Plans_Get (2158)

### Missing/Unavailable APIs
- ❌ Equipment/Machine details API
- ❌ Tool management API
- ❌ Work instructions API
- ❌ Document management API (for files)
- ❌ Maintenance work orders API
- ❌ Employee/operator API
- ❌ Customer orders API

## Revised Implementation Plan

### Phase 1: Core Manufacturing Data (Current Focus)
**Goal**: Establish foundational asset hierarchy and job tracking

#### 1.1 Asset Hierarchy
```
Facility (Root)
├── Buildings (from /mdm/v1/buildings)
│   └── Workcenters (from /production/v1/production-definitions/workcenters)
├── Parts Library (logical grouping)
│   └── Parts (from /mdm/v1/parts)
└── Inventory
    └── Locations (from /inventory/v1/inventory-definitions/locations)
        └── Containers (from /inventory/v1/inventory-tracking/containers)
```

#### 1.2 Events Implementation
- **Job Events**: Create event for every job, even without production
  - Source: `/production/v1/scheduling/jobs`
  - Type: `job`
  - Metadata: job_number, part_number, quantity_ordered, due_date
  
- **Production Events**: Track actual production
  - Source: `/production/v1/production-history/production-entries`
  - Type: `production`
  - Metadata: quantity_produced, scrap, cycle_time, operator

#### 1.3 Sequences for Job Tracking
- **Job Routing Sequence**: Track job progress through operations
  - Source: `/mdm/v1/part-operations` + job status
  - Columns: operation_number, workcenter, status, quantity_complete

### Phase 2: Quality Integration
**Goal**: Add quality events and specifications

#### 2.1 Quality Events
- **Checksheet Events**
  - Source: Data Source API (4142)
  - Type: `quality.inspection`
  
- **Specification Events**
  - Source: Data Source API (6429)
  - Type: `quality.specification`

#### 2.2 Quality Documents (if accessible)
- Investigate Spec_Docs data sources for document URLs
- Store as CDF Files if documents can be retrieved

### Phase 3: Enhanced Production Tracking
**Goal**: Add time series and OEE calculations

#### 3.1 Time Series
- Workcenter output rate (calculated from production events)
- Workcenter availability (from status entries)
- Inventory levels (from container data)
- Quality metrics (defect rates from checksheets)

#### 3.2 Calculated Metrics
- OEE = Availability × Performance × Quality
- First Pass Yield from quality data
- Cycle time variations

### Phase 4: Advanced Features (Future)
**Goal**: Add missing data through alternative sources

#### 4.1 Equipment/Tools
- Option 1: Create as child assets of workcenters
- Option 2: Use custom data source if available
- Option 3: Manual entry through UI

#### 4.2 Work Instructions
- Option 1: Check if URLs exist in operation data
- Option 2: Use specification documents as proxy
- Option 3: Upload manually as CDF Files

#### 4.3 Maintenance
- Option 1: Create events from downtime reasons
- Option 2: Integrate with external CMMS if available

## Implementation Priority

### Immediate Actions (Week 1)
1. ✅ Fix deduplication in all extractors
2. 🔄 Refactor job extractor to create events for ALL jobs
3. 🔄 Create sequences for job routing tracking
4. 🔄 Implement production events with proper linking

### Short Term (Week 2-3)
1. Add operation data from `/mdm/v1/operations`
2. Implement quality events from Data Source API
3. Create time series for key metrics
4. Build OEE calculations

### Medium Term (Month 2)
1. Investigate document retrieval options
2. Add calculated KPIs and dashboards
3. Implement data quality monitoring
4. Create relationship graphs

## Code Refactoring Needed

### 1. Jobs Extractor (`jobs_extractor.py`)
```python
# Current: Creates assets for jobs
# New: Create events for ALL jobs, including scheduled

async def extract_jobs(self):
    jobs = await self.fetch_all_jobs()
    for job in jobs:
        event = Event(
            external_id=f"{PCN}_JOB_{job_id}",
            type="job",
            subtype=job_status,  # scheduled, in_progress, completed
            start_time=scheduled_start,
            end_time=scheduled_end or actual_end,
            metadata={...},
            asset_ids=[workcenter_id, part_id]
        )
```

### 2. Production Extractor (`production_extractor.py`)
```python
# Add: Create production events linked to job events

async def extract_production_events(self):
    entries = await self.fetch_production_entries()
    for entry in entries:
        event = Event(
            external_id=f"{PCN}_PROD_{job_id}_{timestamp}",
            type="production",
            subtype="cycle_complete",
            metadata={...},
            asset_ids=[job_event_id, workcenter_id]
        )
```

### 3. New: Sequence Extractor (`sequence_extractor.py`)
```python
# Create sequences for job routing

async def create_job_routing_sequence(self, job):
    operations = await self.fetch_part_operations(job.part_id)
    sequence = Sequence(
        external_id=f"{PCN}_ROUTE_{job_id}",
        columns=[...],
        rows=operations_with_status
    )
```

## Success Metrics

1. **Data Completeness**
   - 100% of jobs tracked as events
   - 100% of production linked to jobs
   - Asset hierarchy fully populated

2. **Data Quality**
   - No duplicate assets/events
   - All relationships properly linked
   - Metadata standardized

3. **Performance**
   - Real-time production tracking (<5 min delay)
   - OEE calculated hourly
   - Quality metrics updated per shift

## Next Steps

1. Review and approve this plan
2. Begin refactoring extractors for event-based model
3. Test sequence creation for job tracking
4. Implement quality event extraction
5. Create dashboard mockups in CDF