# Plex to CDF Data Model Mapping

## Overview
This document defines the optimal mapping of Plex MES data to Cognite Data Fusion (CDF) resources, leveraging all CDF resource types for maximum value.

## CDF Resource Types

### 1. **Assets** (Physical & Logical Hierarchy)
Assets represent physical and logical entities in a hierarchical structure.

#### Asset Hierarchy:
```
Facility (Root)
├── Buildings
│   ├── Production Areas
│   │   ├── Workcenters
│   │   │   ├── Equipment/Machines
│   │   │   │   └── Tools
│   │   │   └── Inspection Stations
│   │   └── Storage Locations
│   │       └── Containers/Bins
├── Warehouses
│   └── Inventory Locations
└── Quality Labs
    └── Test Equipment
```

#### Detailed Asset Mappings:

**Facility Asset**
- External ID: `{PCN}_FACILITY_ROOT`
- Name: Facility name
- Metadata:
  - pcn: Plant Customer Number
  - facility_code
  - timezone
  - country
  - production_types

**Building Asset**
- External ID: `{PCN}_BUILDING_{building_id}`
- Parent: Facility
- Metadata:
  - building_code
  - building_type (Production/Warehouse/Office)
  - address, city, state, zip
  - area_sqft
  - active_status

**Workcenter Asset**
- External ID: `{PCN}_WC_{workcenter_id}`
- Parent: Building or Production Area
- Metadata:
  - workcenter_code
  - workcenter_type
  - capacity_per_hour
  - efficiency_target
  - oee_target
  - current_status
  - default_operators

**Equipment/Machine Asset**
- External ID: `{PCN}_EQUIPMENT_{equipment_id}`
- Parent: Workcenter
- Metadata:
  - equipment_type
  - manufacturer
  - model
  - serial_number
  - installation_date
  - maintenance_schedule
  - current_status

**Tool Asset**
- External ID: `{PCN}_TOOL_{tool_id}`
- Parent: Equipment or Workcenter
- Metadata:
  - tool_type
  - tool_number
  - calibration_due
  - usage_count
  - max_usage

**Part/Product Asset** (Master Data)
- External ID: `{PCN}_PART_{part_id}`
- Parent: Parts Library (logical grouping)
- Metadata:
  - part_number
  - revision
  - description
  - unit_of_measure
  - standard_cost
  - weight
  - material_type

### 2. **Events** (Discrete Occurrences)
Events represent things that happen at specific times or over time periods.

#### Event Types:

**Job Event** (Even with no production)
- External ID: `{PCN}_JOB_{job_id}`
- Type: `job`
- Subtype: `scheduled` | `in_progress` | `completed` | `cancelled`
- Start Time: Scheduled start
- End Time: Actual completion (or scheduled end)
- Metadata:
  - job_number
  - part_number
  - quantity_ordered
  - quantity_completed
  - priority
  - customer_order
  - due_date
- Asset Links: Workcenter, Part

**Production Event** (Actual work performed)
- External ID: `{PCN}_PROD_{job_id}_{timestamp}`
- Type: `production`
- Subtype: `cycle_complete` | `partial_complete` | `scrap`
- Timestamp: When production recorded
- Metadata:
  - job_id
  - operation_number
  - quantity_produced
  - scrap_quantity
  - operator_id
  - shift
  - cycle_time
- Asset Links: Job, Workcenter, Equipment

**Quality Event**
- External ID: `{PCN}_QUALITY_{inspection_id}`
- Type: `quality`
- Subtype: `inspection` | `ncr` | `audit` | `calibration`
- Metadata:
  - inspection_type
  - pass_fail
  - defect_codes
  - inspector
  - checksheet_id
  - measurements (JSON)

**Maintenance Event**
- External ID: `{PCN}_MAINT_{work_order_id}`
- Type: `maintenance`
- Subtype: `preventive` | `corrective` | `breakdown`
- Metadata:
  - work_order_number
  - maintenance_type
  - technician
  - parts_used
  - downtime_minutes

**Inventory Movement Event**
- External ID: `{PCN}_INV_MOVE_{transaction_id}`
- Type: `inventory`
- Subtype: `receipt` | `issue` | `transfer` | `adjustment`
- Metadata:
  - from_location
  - to_location
  - quantity
  - lot_number
  - reason_code

**Changeover Event**
- External ID: `{PCN}_CHANGEOVER_{timestamp}`
- Type: `changeover`
- Metadata:
  - from_part
  - to_part
  - changeover_duration
  - workcenter_id

### 3. **Sequences** (Ordered Operations)
Sequences represent ordered lists of operations or steps.

**Job Routing Sequence**
- External ID: `{PCN}_ROUTE_{job_id}`
- Columns:
  - operation_number (integer)
  - operation_code (string)
  - workcenter_id (string)
  - setup_time (number)
  - cycle_time (number)
  - status (string): pending | in_progress | complete
  - actual_start (timestamp)
  - actual_end (timestamp)
  - quantity_complete (number)

**Production Log Sequence**
- External ID: `{PCN}_PRODLOG_{job_id}_{date}`
- Columns:
  - timestamp
  - event_type (start | stop | produce | scrap)
  - quantity
  - operator
  - reason_code

**Quality Inspection Sequence**
- External ID: `{PCN}_INSPECTION_{job_id}`
- Columns:
  - step_number
  - characteristic
  - nominal_value
  - measured_value
  - pass_fail
  - timestamp

### 4. **Time Series** (Continuous Measurements)
Time series for continuous or regularly sampled data.

**Production Metrics**
- `{PCN}_WC_{id}_output_rate` - Parts per hour
- `{PCN}_WC_{id}_oee` - Overall Equipment Effectiveness
- `{PCN}_WC_{id}_availability` - Availability percentage
- `{PCN}_WC_{id}_performance` - Performance percentage
- `{PCN}_WC_{id}_quality` - Quality percentage
- `{PCN}_WC_{id}_cycle_time` - Actual cycle time

**Inventory Levels**
- `{PCN}_LOC_{id}_inventory_level` - Current inventory quantity
- `{PCN}_PART_{id}_total_onhand` - Total on-hand across locations

**Quality Metrics**
- `{PCN}_PART_{id}_defect_rate` - Defect percentage
- `{PCN}_WC_{id}_first_pass_yield` - FPY percentage

**Machine Telemetry** (if available)
- `{PCN}_EQUIPMENT_{id}_temperature`
- `{PCN}_EQUIPMENT_{id}_pressure`
- `{PCN}_EQUIPMENT_{id}_vibration`
- `{PCN}_EQUIPMENT_{id}_power_consumption`

### 5. **Files** (Documents & Media)
Files for work instructions, drawings, images, and documents.

**Work Instructions**
- External ID: `{PCN}_WI_{part_id}_{operation}`
- File Type: PDF, HTML, Video
- Metadata:
  - instruction_type
  - revision
  - effective_date
  - part_number
  - operation_number

**Part Drawings**
- External ID: `{PCN}_DRAWING_{part_id}_{revision}`
- File Type: PDF, DWG, STEP
- Metadata:
  - drawing_number
  - revision
  - release_date

**Quality Documents**
- External ID: `{PCN}_QUALITY_DOC_{document_id}`
- Types:
  - Control plans
  - FMEA documents
  - Inspection checksheets
  - Certificates of conformance

**Images/Photos**
- External ID: `{PCN}_IMAGE_{context}_{id}`
- Types:
  - Product photos
  - Defect images
  - Setup photos

### 6. **Relationships** (Asset Links)
Define relationships between assets:

- Workcenter → Equipment (contains)
- Equipment → Tools (uses)
- Job → Part (produces)
- Job → Workcenter (runs_at)
- Part → BOM Components (consists_of)
- Container → Location (stored_at)

## Plex API Endpoints Needed

### REST API Endpoints:
1. `/production/v1/production-definitions/workcenters` - Workcenters
2. `/production/v1/production-history/workcenter-status-entries` - Status
3. `/production/v1/production-history/production-entries` - Production logs
4. `/production/v1/scheduling/jobs` - Jobs
5. `/mdm/v1/buildings` - Buildings
6. `/mdm/v1/parts` - Parts master
7. `/mdm/v1/part-operations` - Routings
8. `/engineering/v1/boms` - Bill of materials
9. `/inventory/v1/inventory-tracking/containers` - Containers
10. `/inventory/v1/inventory-definitions/locations` - Locations
11. `/quality/v1/inspections` - Quality inspections
12. `/maintenance/v1/work-orders` - Maintenance

### Data Source API:
- Quality checksheets
- Control plans
- Specifications
- Test results
- Document attachments

### Additional APIs to Investigate:
1. **Document Management API** - For work instructions and drawings
2. **Equipment API** - For machine/equipment details
3. **Tool Management API** - For tooling data
4. **Operator/Labor API** - For workforce data
5. **Customer Orders API** - For order context
6. **Shift Calendar API** - For shift patterns

## Implementation Priority

### Phase 1: Core Manufacturing
1. Assets: Facility, Buildings, Workcenters
2. Events: Jobs (even without production)
3. Events: Production entries
4. Time Series: Basic production metrics

### Phase 2: Quality & Inventory
1. Events: Quality inspections
2. Assets: Containers and Locations
3. Events: Inventory movements
4. Time Series: Inventory levels

### Phase 3: Advanced Features
1. Sequences: Job routing tracking
2. Files: Work instructions
3. Assets: Equipment and Tools
4. Events: Maintenance activities

### Phase 4: Optimization
1. Time Series: OEE calculations
2. Sequences: Production patterns
3. Files: Quality documents
4. Relationships: Full asset graph

## Benefits of This Model

1. **Complete Digital Twin**: Full representation of physical factory
2. **Event Tracking**: Every significant occurrence is captured
3. **Sequenced Operations**: Track job progress through operations
4. **Document Access**: Work instructions and specs at point of use
5. **Analytics Ready**: Data structured for ML and analytics
6. **Contextualized**: All data linked to relevant assets
7. **Time-Aware**: Historical tracking of all changes

## Next Steps

1. Verify API access to all endpoints
2. Test document/file retrieval capabilities
3. Implement Phase 1 extractors
4. Create CDF dashboards and views
5. Build data quality monitoring