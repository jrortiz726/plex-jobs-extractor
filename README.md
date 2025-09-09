# Plex-Cognite Data Fusion Extractors

Comprehensive data extraction from Plex MES to Cognite Data Fusion (CDF) with **automatic resource discovery** and multi-facility support.

## 🚀 Key Features

### Automatic Discovery
- **No configuration needed** - Extractors automatically discover ALL resources
- Fetches all workcenters, containers, and locations for your PCN
- New resources are included automatically
- Zero maintenance required

### Multi-Facility Support (PCN-Aware)
- Complete data isolation between facilities
- Every ID includes PCN prefix (e.g., `PCN340884_WC_001`)
- Deploy once per facility or run multiple PCNs

### Comprehensive Data Coverage
- **Production**: Workcenters, OEE, throughput, utilization
- **Jobs**: Active jobs, operations, schedule adherence  
- **Inventory**: Containers, locations, movements, WIP
- **Quality**: Specifications, NCRs, test results, audits
- **Master Data**: Parts, BOMs, routings, equipment

## 📋 Prerequisites

- Python 3.9+
- Plex API credentials
- Cognite Data Fusion project access
- (Optional) Plex Data Source API credentials for quality data

## 🎯 Quick Start

### 1. Minimal Configuration

Create `.env` with just the essentials:

```bash
# Cognite Data Fusion
CDF_HOST=https://westeurope-1.cognitedata.com
CDF_PROJECT=your-project
CDF_CLIENT_ID=your-client-id
CDF_CLIENT_SECRET=your-secret
CDF_TOKEN_URL=https://your-auth0.auth0.com/oauth/token

# Plex API
PLEX_API_KEY=your-api-key
PLEX_CUSTOMER_ID=340884  # Your PCN

# Facility
FACILITY_NAME=Main Manufacturing Plant
```

**That's it!** No need to specify workcenter, container, or location IDs.

### 2. Install & Setup

```bash
# Install dependencies
pip install -r requirements.txt

# Create CDF datasets
python setup_datasets.py

# Test connections
python test_connections.py
```

### 3. Run Extractors

```bash
# Run all extractors with orchestrator
python orchestrator.py --mode continuous

# Or use the interactive menu
./start.sh
```

## 🔄 How Automatic Discovery Works

### No Configuration Required

```bash
# Traditional approach (NOT needed anymore!)
WORKCENTER_IDS=WC001,WC002,WC003  ❌
CONTAINER_IDS=C001,C002,C003      ❌
LOCATION_IDS=L001,L002,L003       ❌

# New approach - just leave empty!
WORKCENTER_IDS=   ✅  # Fetches ALL workcenters
CONTAINER_IDS=    ✅  # Fetches ALL containers  
LOCATION_IDS=     ✅  # Fetches ALL locations
```

The extractors automatically call Plex APIs to discover all resources:
- `/production/v1/production-definitions/workcenters` - Gets all workcenters
- `/inventory/v1/containers` - Gets all containers
- `/inventory/v1/locations` - Gets all storage locations

## 📊 Data Architecture

### CDF Datasets
- `plex_production` - Real-time production metrics
- `plex_scheduling` - Jobs and operations
- `plex_quality` - Quality and compliance
- `plex_inventory` - Material tracking
- `plex_master` - Reference data

### Data Models
- **Assets**: Hierarchical equipment and locations
- **Events**: Production events, quality issues
- **Time Series**: Metrics, utilization, fill levels

### Naming Convention
All IDs include PCN prefix for multi-facility support:
```
PCN340884_WC_123        # Workcenter
PCN340884_JOB_456       # Job
PCN340884_CONTAINER_789 # Container
```

## 🚢 Deployment Options

### Local Development
```bash
python orchestrator.py --mode continuous
```

### Docker
```bash
docker-compose up -d
```

### Cloud Deployment
- **Azure**: Container Instances or AKS
- **AWS**: ECS Fargate or EKS
- **GCP**: Cloud Run or GKE
- **On-Premise**: Linux VM with systemd

See [DEPLOYMENT.md](DEPLOYMENT.md) for detailed instructions.

## 📁 Project Structure

```
plex-cognite/
├── orchestrator.py           # Manages all extractors
├── jobs_extractor.py         # Jobs and scheduling
├── production_extractor.py   # Production metrics
├── inventory_extractor.py    # Inventory tracking
├── quality_extractor.py      # Quality data (Data Source API)
├── master_data_extractor.py  # Reference data
├── multi_facility_config.py  # PCN-aware naming
├── setup_datasets.py         # CDF dataset setup
├── test_connections.py       # Connection testing
├── docker-compose.yml        # Docker orchestration
├── deploy/                   # Deployment scripts
│   ├── kubernetes.yaml
│   ├── azure-container-instance.sh
│   └── aws-ecs-deploy.sh
└── docs/                     # Documentation
    ├── CLAUDE.md            # AI assistant context
    ├── dynamic-data-discovery.md
    └── quality-extractor.md
```

## 🔧 Configuration

### Environment Variables

See [.env.example](.env.example) for all options. Key settings:

```bash
# Extraction intervals (seconds)
MASTER_EXTRACTION_INTERVAL=86400  # Daily
JOBS_EXTRACTION_INTERVAL=300      # 5 minutes
PRODUCTION_EXTRACTION_INTERVAL=300
INVENTORY_EXTRACTION_INTERVAL=300
QUALITY_EXTRACTION_INTERVAL=300

# Date range
EXTRACTION_START_DATE=2024-01-01T00:00:00Z
```

### Advanced: Limiting Scope

If you need to limit extraction to specific resources:

```bash
# Only specific workcenters (not recommended)
WORKCENTER_IDS=WC001,WC002

# Only specific containers
CONTAINER_IDS=C001,C002
```

## 📈 Monitoring

### Health Checks
- Orchestrator sends heartbeat events to CDF
- Each extractor reports status and errors
- Automatic retry on failures

### Logs
- Console output with timestamps
- File logging to `orchestrator.log`
- Docker logs: `docker-compose logs -f`

### CDF Events
Monitor extraction status in CDF:
- `orchestrator_heartbeat` - System health
- `extractor_error` - Extraction failures

## 🐛 Troubleshooting

### No Data Extracted
1. Check `test_connections.py` passes
2. Verify PCN is correct
3. Check resources are Active in Plex
4. Review extraction date range

### Rate Limiting
- Extractors handle automatically with backoff
- Adjust `PLEX_API_RATE_LIMIT` if needed

### Authentication Issues
- CDF uses `user_impersonation` scope (not `.default`)
- Quality extractor needs separate Data Source API credentials

## 📚 Documentation

- [QUICKSTART.md](QUICKSTART.md) - Get running quickly
- [DEPLOYMENT.md](DEPLOYMENT.md) - Production deployment
- [docs/dynamic-data-discovery.md](docs/dynamic-data-discovery.md) - How auto-discovery works
- [docs/quality-extractor.md](docs/quality-extractor.md) - Quality data extraction
- [CLAUDE.md](CLAUDE.md) - Technical requirements and patterns

## 🤝 Contributing

1. Follow PCN-aware naming conventions
2. Use `MultiTenantNamingConvention` class
3. Support dynamic discovery by default
4. Test with multiple PCNs
5. Update documentation

## 📄 License

[Your License Here]

## 🆘 Support

- Check logs: `orchestrator.log`
- Test connections: `python test_connections.py`
- Review [CLAUDE.md](CLAUDE.md) for requirements
- Contact Plex support for API issues
- Contact Cognite support for CDF issues