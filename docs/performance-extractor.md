# Performance Extractor Documentation

## Overview
The Performance Extractor is a specialized extractor that handles all OEE (Overall Equipment Effectiveness) and performance metrics using the Plex Data Source API. This extractor is separate from the production extractor to maintain clean separation between REST API and Data Source API calls.

## Why Separate from Production?

1. **Different API Types**: 
   - Production uses REST API with API key authentication
   - Performance uses Data Source API with Basic authentication

2. **Different Concerns**:
   - Production focuses on production events and entries
   - Performance focuses on metrics and KPIs

3. **Different Data Models**:
   - Production data is event-based
   - Performance data is metric/time-series based

4. **Independent Scaling**:
   - Can run on different schedules
   - Can be disabled independently
   - Different resource requirements

## Data Sources Used

### ID 18765: Daily_Performance_Report_Get
Provides daily aggregated performance metrics including:
- Production quantity
- Reject quantity
- Scrap quantity
- Downtime
- Planned production time
- Calculated OEE components

### ID 22870: Workcenter_Performance_Simple_Get
Provides real-time performance metrics for specific workcenters:
- Current performance percentage
- Workcenter status
- Part being produced

## Configuration

### Environment Variables
```bash
# Enable/disable the performance extractor
ENABLE_PERFORMANCE_EXTRACTOR=true

# Data Source API credentials (required)
PLEX_DS_USERNAME=your_username
PLEX_DS_PASSWORD=your_password
PLEX_PCN_CODE=ra-process
PLEX_USE_TEST=false

# Extraction settings
EXTRACT_DAILY_PERFORMANCE=true
EXTRACT_REALTIME_PERFORMANCE=true
PERFORMANCE_LOOKBACK_DAYS=7
PERFORMANCE_EXTRACTION_INTERVAL=900  # 15 minutes
```

## Data Flow

```
Plex Data Source API
    ├── Daily Performance (ID 18765)
    │   └── Historical OEE metrics
    └── Real-time Performance (ID 22870)
        └── Current workcenter performance
                ↓
    Performance Extractor
        ├── Parse metrics
        ├── Calculate OEE (A × P × Q)
        └── Create time series
                ↓
    Cognite Data Fusion
        ├── OEE time series
        ├── Availability time series
        ├── Performance time series
        └── Quality time series
```

## OEE Calculation

OEE = Availability × Performance × Quality

Where:
- **Availability** = (Planned Time - Downtime) / Planned Time
- **Performance** = Actual Production / Theoretical Production
- **Quality** = Good Parts / Total Parts

## Time Series Created

For each workcenter, the extractor creates:
1. `{PCN}_ts_oee_{workcenter_id}` - Overall OEE percentage
2. `{PCN}_ts_availability_{workcenter_id}` - Availability percentage
3. `{PCN}_ts_performance_{workcenter_id}` - Performance percentage
4. `{PCN}_ts_quality_{workcenter_id}` - Quality percentage

## Running the Extractor

### Standalone
```bash
python performance_extractor_enhanced.py
```

### Via Orchestrator
```bash
ENABLE_PERFORMANCE_EXTRACTOR=true python orchestrator_enhanced.py
```

### Testing
```bash
# Test connection to Data Source API
python -c "
from performance_extractor_enhanced import DataSourceAPIClient
client = DataSourceAPIClient(
    username='$PLEX_DS_USERNAME',
    password='$PLEX_DS_PASSWORD',
    pcn_code='$PLEX_PCN_CODE'
)
print('Client initialized successfully')
"
```

## Troubleshooting

### No Data Extracted
1. Check Data Source API credentials are correct
2. Verify PCN code matches your environment
3. Check if workcenters exist in the system
4. Verify date ranges for historical data

### Authentication Errors (403)
- Verify username/password are correct
- Check if using test vs production environment
- Ensure user has permissions for the data sources

### Empty Results
- Some workcenters may not have performance data
- Historical data may not be available for all dates
- Check workcenter keys match between systems

## Integration with Other Extractors

The performance extractor complements:
- **Production Extractor**: Provides metrics for production events
- **Jobs Extractor**: Links performance to scheduled jobs
- **Quality Extractor**: Quality component of OEE calculation

## Future Enhancements

1. **Additional Metrics**: Add more KPIs like TEEP, cycle time
2. **Shift-based Analysis**: Break down performance by shift
3. **Predictive Analytics**: Use historical data for predictions
4. **Alert Generation**: Create alerts for low OEE
5. **Workcenter Mapping**: Better mapping between REST and DS APIs