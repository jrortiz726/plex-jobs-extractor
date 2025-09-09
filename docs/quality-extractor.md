# Quality Extractor Documentation

## Overview

The Quality Extractor fetches quality-related data from Plex MES using the **Data Source API**, which is different from the standard REST API used by other extractors. This extractor handles quality specifications, test tracking, non-conformance records (NCRs), problem reports, and audit data.

## Key Differences: Data Source API vs REST API

### Data Source API
- Uses stored procedures wrapped as data sources
- Requires special credentials (request from Plex Customer Care)
- Basic authentication with username/password
- More powerful but requires specific data source IDs
- Returns tabular data (columns and rows)

### REST API
- Standard RESTful endpoints
- Uses API keys and customer IDs
- Simpler but limited to predefined endpoints
- Returns JSON objects

## Authentication

### Required Credentials

```bash
# Data Source API credentials (different from regular API)
PLEX_DS_USERNAME=your-datasource-username
PLEX_DS_PASSWORD=your-datasource-password
```

**Important**: These are NOT the same as your Plex login credentials. You must request developer access from Plex Customer Care.

## Data Sources Used

### Primary Quality Data Sources

| Data Source | ID | Purpose |
|------------|-----|---------|
| Specification_Get | 6429 | Detailed specification data |
| Specification_Picker_Get | 5112 | List of specifications |
| Specifications_By_Part_Part_Operation_Get_Picker | 230339 | Part-specific specs |
| Spec_Docs_Job_Get | 10838 | Job quality documents |
| Test_Commands_Get | 16635 | Test command list |
| Test_Command_Get | 16637 | Detailed test data |
| Test_Command_Instruction_By_Serial_Get | 18470 | Serial-specific tests |
| Test_Command_Instructions_By_Part_Get | 18469 | Part test instructions |
| Spec_Docs_Get | 14715 | All specification documents |
| Spec_Docs_Part_Get | 21711 | Part-specific documents |

## Data Extracted

### 1. Specifications & Check Sheets
- Part specifications with tolerances
- Dimensional requirements
- Check sheet definitions
- Measurement parameters

### 2. Test Commands
- Test procedures and instructions
- Sampling requirements
- Test frequency and types
- Pass/fail criteria

### 3. Non-Conformance Records (NCRs)
- Quality issues and deviations
- Problem descriptions
- Resolution status
- Affected parts and jobs

### 4. Quality Documents
- Audit reports
- Problem control forms
- Quality certifications
- Inspection records

## CDF Data Model

### Events Created

```python
# Specification Event
{
    'external_id': 'PCN340884_EVT_specification_12345_1234567890',
    'type': 'quality_specification',
    'subtype': 'checksheet',
    'metadata': {
        'specification_no': 'SPEC-001',
        'part_no': 'PART-123',
        'nominal': '10.0',
        'upper_limit': '10.5',
        'lower_limit': '9.5'
    }
}

# NCR Event
{
    'external_id': 'PCN340884_EVT_quality_issue_67890_1234567890',
    'type': 'quality_issue',
    'subtype': 'ncr',
    'metadata': {
        'document_no': 'NCR-2024-001',
        'severity': 'Major',
        'status': 'Open'
    }
}
```

### Time Series Metrics

- `PCN340884_TS_QUALITY_FACILITY_defect_rate` - Defect percentage
- `PCN340884_TS_QUALITY_FACILITY_first_pass_yield` - FPY percentage
- `PCN340884_TS_QUALITY_FACILITY_ncr_count` - NCR count
- `PCN340884_TS_QUALITY_FACILITY_audit_score` - Audit scores

## API Request Format

### Example Data Source Request

```python
# Request to get specifications
POST /api/datasources/5112/execute?format=2
Authorization: Basic {base64_encoded_credentials}
Content-Type: application/json

{
    "Active_Flag": true,
    "Part_Type": "",
    "Part_Status": "Production"
}
```

### Response Format

```json
{
    "outputs": {},
    "rows": [
        {
            "Specification_Key": 12345,
            "Specification_No": "SPEC-001",
            "Name": "Diameter Check",
            "Part_No": "82695",
            "Nominal": 10.0,
            "Upper_Limit": 10.5,
            "Lower_Limit": 9.5
        }
    ],
    "rowLimitedExceeded": false,
    "transactionNo": "1234567890"
}
```

## Running the Quality Extractor

### Standalone Mode

```bash
# Test connections first
python test_connections.py

# Run quality extractor
python quality_extractor.py
```

### With Orchestrator

```bash
# Run all extractors including quality
python orchestrator.py --mode continuous

# Run only quality extractor
python orchestrator.py --mode once --extractors quality
```

### Docker

```bash
docker run -d \
  --name quality-extractor \
  --env-file .env \
  plex-cdf-extractor \
  python quality_extractor.py
```

## Configuration

### Environment Variables

```bash
# Data Source API Credentials
PLEX_DS_USERNAME=webservice_user
PLEX_DS_PASSWORD=secure_password

# Test environment (optional)
PLEX_USE_TEST=false

# Quality-specific settings
QUALITY_EXTRACTION_INTERVAL=300  # 5 minutes
QUALITY_EXTRACTION_START_DATE=2024-01-01T00:00:00Z
QUALITY_DAYS_BACK=30
QUALITY_BATCH_SIZE=1000

# Dataset ID
CDF_DATASET_PLEXQUALITY=2881941287917280
```

## Troubleshooting

### Common Issues

#### 1. Authentication Failure
```
Error: 401 Not Authorized
```
**Solution**: Verify Data Source API credentials are correct and account has access.

#### 2. Data Source Access Denied
```
Error: DATA_SOURCE_ACCESS_DENIED - You do not have access to data source '6429'
```
**Solution**: Contact Plex Customer Care to grant access to required data sources.

#### 3. Input Not Found
```
Error: DATA_SOURCE_INPUT_NOT_FOUND - Input 'Part_Key' was not found
```
**Solution**: Check data source metadata for correct input names (case-sensitive).

#### 4. No Data Returned
- Verify date range parameters
- Check if quality data exists in Plex
- Ensure proper PCN is configured

### Debugging

Enable debug logging:
```python
logging.basicConfig(level=logging.DEBUG)
```

Test specific data source:
```python
async def test_data_source():
    config = QualityConfig.from_env()
    async with PlexDataSourceClient(config) as client:
        # Get metadata first
        metadata = await client.get_data_source_metadata(6429)
        print(f"Inputs: {metadata['inputs']}")
        
        # Execute data source
        result = await client.execute_data_source(
            6429,
            {'Specification_Key': 12345}
        )
        print(f"Result: {result}")
```

## Performance Considerations

### Rate Limiting
- Data Source API has different rate limits than REST API
- Default: 100 requests/minute
- Use batch operations where possible

### Data Volume
- Specifications can be numerous (thousands)
- Use date ranges to limit data
- Process in batches to avoid memory issues

### Optimization Tips
1. Cache frequently accessed data
2. Use incremental extraction with state tracking
3. Filter by active/production status
4. Limit historical data range

## Integration with Other Extractors

### Cross-References
- Job IDs from `jobs_extractor` can fetch job-specific quality data
- Part numbers link to `master_data_extractor`
- Container IDs connect to `inventory_extractor`

### Data Flow
```
Jobs Extractor → Job IDs → Quality Extractor → Job Quality Events
                                ↓
                         Quality Metrics → CDF Time Series
```

## Future Enhancements

1. **Real-time Quality Alerts**
   - Monitor NCR creation
   - Alert on specification violations

2. **Statistical Process Control (SPC)**
   - Calculate control limits
   - Trend analysis

3. **Quality Dashboard**
   - Defect Pareto charts
   - First pass yield trends
   - NCR aging reports

4. **Integration with MES Events**
   - Link quality issues to production events
   - Correlate defects with equipment

## Support

For issues with:
- **Data Source API Access**: Contact Plex Customer Care
- **Extractor Code**: Review CLAUDE.md and this documentation
- **CDF Integration**: Check Cognite documentation
- **PCN/Multi-facility**: Ensure all IDs include PCN prefix