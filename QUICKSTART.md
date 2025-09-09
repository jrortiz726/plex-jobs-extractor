# Quick Start Guide

## Minimal Configuration Required

The Plex-CDF extractors are designed to work with **minimal configuration**. They automatically discover and extract all available data for your PCN.

## Step 1: Essential Configuration Only

Create a `.env` file with just the required credentials:

```bash
# REQUIRED: Cognite Data Fusion
CDF_HOST=https://westeurope-1.cognitedata.com
CDF_PROJECT=your-project
CDF_CLIENT_ID=your-client-id
CDF_CLIENT_SECRET=your-client-secret
CDF_TOKEN_URL=https://your-auth0.auth0.com/oauth/token

# REQUIRED: Plex API
PLEX_API_KEY=your-plex-api-key
PLEX_CUSTOMER_ID=340884  # Your PCN

# REQUIRED: Facility Info
FACILITY_NAME=Main Manufacturing Plant

# OPTIONAL: For Quality Extractor (request from Plex)
PLEX_DS_USERNAME=your-datasource-username
PLEX_DS_PASSWORD=your-datasource-password
```

**That's it!** The extractors will automatically:
- ✅ Fetch ALL workcenters for your PCN
- ✅ Discover ALL containers in inventory
- ✅ Find ALL storage locations
- ✅ Extract data for ALL active resources

## Step 2: Create Datasets

Run the setup script to create CDF datasets:

```bash
python setup_datasets.py
```

Copy the dataset IDs to your `.env` file:

```bash
CDF_DATASET_PLEXPRODUCTION=869709519571885
CDF_DATASET_PLEXSCHEDULING=7195113081024241
CDF_DATASET_PLEXQUALITY=2881941287917280
CDF_DATASET_PLEXINVENTORY=8139681212033756
CDF_DATASET_PLEXMAINTENANCE=5080535668683118
CDF_DATASET_PLEXMASTER=4945797542267648
```

## Step 3: Test & Run

```bash
# Test connections
python test_connections.py

# Run all extractors
python orchestrator.py --mode continuous

# Or run specific extractor
python production_extractor.py
```

## What Gets Extracted Automatically

### Production Data (ALL Workcenters)
- Workcenter status and utilization
- Production metrics (OEE, throughput)
- Scrap rates and quality metrics
- No configuration needed!

### Jobs & Scheduling (ALL Jobs)
- Active production jobs
- Job operations and progress
- Schedule adherence
- Automatically tracks all jobs

### Inventory (ALL Containers & Locations)
- Container status and fill levels
- Storage locations
- Material movements
- WIP tracking
- Complete inventory visibility

### Master Data (ALL Resources)
- Parts and BOMs
- Routings
- Equipment specifications
- Automatically synced daily

### Quality (Based on Data Sources)
- Specifications and check sheets
- NCRs and problem reports
- Test commands
- Audit results

## Advanced: Limiting Scope (Optional)

If you want to limit extraction to specific resources:

```bash
# Only extract specific workcenters
WORKCENTER_IDS=WC001,WC002,WC003

# Only track specific containers
CONTAINER_IDS=C001,C002

# Only monitor specific locations
LOCATION_IDS=L001,L002
```

**But this is NOT recommended!** Let the extractors discover everything automatically.

## Default Extraction Intervals

The orchestrator runs extractors on these schedules:

- **Master Data**: Daily (24 hours)
- **Jobs**: Every 5 minutes
- **Production**: Every 5 minutes
- **Inventory**: Every 5 minutes
- **Quality**: Every 5 minutes

Adjust in `.env` if needed:

```bash
JOBS_EXTRACTION_INTERVAL=300        # seconds
PRODUCTION_EXTRACTION_INTERVAL=300
```

## Why Dynamic Discovery?

1. **Zero Maintenance**: New workcenters/containers are included automatically
2. **Complete Coverage**: Never miss data from new resources
3. **Simpler Setup**: No need to know IDs in advance
4. **PCN-Specific**: Only fetches data for your facility

## Deployment

### Quick Local Test
```bash
./start.sh
```

### Docker
```bash
docker-compose up -d
```

### Production
See [DEPLOYMENT.md](DEPLOYMENT.md) for cloud deployment options.

## Common Issues

### "No workcenters found"
- Verify your PCN is correct
- Check API key has proper permissions

### "Rate limited"
- Extractors handle this automatically
- Adjust `PLEX_API_RATE_LIMIT` if needed

### Missing Data
- Check extraction date range: `EXTRACTION_START_DATE`
- Verify resources are marked as Active in Plex

## Next Steps

1. Monitor the logs: `docker-compose logs -f`
2. Check CDF for incoming data
3. Set up alerting for extraction failures
4. Create dashboards in CDF

## Getting Help

- Check [CLAUDE.md](CLAUDE.md) for detailed requirements
- Review logs in `orchestrator.log`
- Test individual extractors in isolation
- Use `--dry-run` mode to verify configuration