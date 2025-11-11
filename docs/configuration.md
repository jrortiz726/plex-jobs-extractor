# Configuration Guide

## Environment Variables

All enhanced extractors are configured through environment variables in the `.env` file.

## Required Configuration

### CDF Authentication
```bash
# Cognite Data Fusion Configuration
CDF_HOST=https://westeurope-1.cognitedata.com
CDF_PROJECT=essc-sandbox-44
CDF_TOKEN_URL=https://login.microsoftonline.com/16b3c013-d300-468d-ac64-7eda0820b6d3/oauth2/v2.0/token
CDF_CLIENT_ID=<your-client-id>
CDF_CLIENT_SECRET=<your-client-secret>

# CRITICAL: OAuth scope MUST be exactly this:
# The extractors use scope=["user_impersonation"] in code
```

### Plex API Configuration
```bash
# Plex ERP API Configuration
PLEX_API_KEY=<your-plex-api-key>
PLEX_CUSTOMER_ID=340884
PLEX_BASE_URL=https://connect.plex.com  # NOT cloud.plex.com
```

### CDF Dataset IDs
```bash
# Dataset IDs for each domain
CDF_DATASET_PLEXSCHEDULING=7195113081024241
CDF_DATASET_PLEXPRODUCTION=869709519571885
CDF_DATASET_PLEXINVENTORY=8139681212033756
CDF_DATASET_PLEXMASTER=4945797542267648
CDF_DATASET_PLEXQUALITY=2881941287917280
CDF_DATASET_PLEXMAINTENANCE=5080535668683118
```

### Facility Configuration
```bash
# Facility Information
FACILITY_NAME=RADEMO
FACILITY_CODE=DEFAULT
FACILITY_TIMEZONE=UTC
FACILITY_COUNTRY=US
```

## Orchestrator Configuration

### Extractor Enable/Disable
```bash
# Enable/disable individual extractors
ENABLE_JOBS_EXTRACTOR=true
ENABLE_MASTER_DATA_EXTRACTOR=true
ENABLE_PRODUCTION_EXTRACTOR=true
ENABLE_INVENTORY_EXTRACTOR=true
ENABLE_QUALITY_EXTRACTOR=true
```

### Performance Configuration
```bash
# Concurrent execution limits
MAX_CONCURRENT_EXTRACTORS=5        # Max extractors running in parallel
MAX_CONCURRENT_API_CALLS=10        # Max concurrent API calls per extractor
BATCH_SIZE=1000                    # Records per batch
```

### Execution Mode
```bash
# Operational modes
DRY_RUN=false                      # true: No CDF writes, false: Normal operation
RUN_ONCE=true                      # true: Single run, false: Continuous
RUN_CONTINUOUS=false               # Alternative to RUN_ONCE
```

### Scheduling Configuration
```bash
# Extraction intervals (seconds)
EXTRACTION_INTERVAL=300            # Default: 5 minutes
JOBS_EXTRACTION_INTERVAL=60        # Jobs specific interval
MASTER_EXTRACTION_INTERVAL=3600    # Master data: 1 hour
PRODUCTION_EXTRACTION_INTERVAL=300 # Production: 5 minutes
INVENTORY_EXTRACTION_INTERVAL=600  # Inventory: 10 minutes
QUALITY_EXTRACTION_INTERVAL=900    # Quality: 15 minutes
```

### Health Monitoring
```bash
# Health check configuration
HEALTH_CHECK_INTERVAL=60          # Seconds between health checks
HEALTH_CHECK_ENABLED=true         # Enable/disable health monitoring
ERROR_THRESHOLD=10                # Max errors before circuit breaker
```

## Advanced Configuration

### Retry Configuration
```bash
# Retry logic settings
MAX_RETRY_ATTEMPTS=3              # Maximum retry attempts
RETRY_INITIAL_DELAY=1             # Initial retry delay (seconds)
RETRY_MAX_DELAY=60                # Maximum retry delay
RETRY_EXPONENTIAL_BASE=2          # Exponential backoff base
```

### Logging Configuration
```bash
# Logging settings
LOG_LEVEL=INFO                    # DEBUG, INFO, WARNING, ERROR, CRITICAL
LOG_FORMAT=json                   # json or text
LOG_FILE=extraction.log           # Log file path
LOG_ROTATION=daily                # daily, size, time
LOG_RETENTION_DAYS=30             # Days to keep logs
```

### API Configuration
```bash
# API client settings
HTTP_TIMEOUT=30                   # HTTP request timeout (seconds)
CONNECTION_POOL_SIZE=10           # Connection pool size
HTTP2_ENABLED=true                # Enable HTTP/2
VERIFY_SSL=true                   # SSL certificate verification
```

### Change Detection (Master Data)
```bash
# Change detection strategy
CHANGE_DETECTION_STRATEGY=HASH    # HASH, TIMESTAMP, VERSION, ALWAYS
CHANGE_DETECTION_FIELD=updated_at # Field to use for timestamp strategy
```

### Quality Extractor Specific
```bash
# Data Source API (for quality extractor)
PLEX_DS_API_USER=<username>
PLEX_DS_API_PASSWORD=<password>
QUALITY_USE_DS_API=true           # Use Data Source API for quality data
```

## Configuration Examples

### Development Configuration
```bash
# .env.development
DRY_RUN=true
RUN_ONCE=true
LOG_LEVEL=DEBUG
MAX_CONCURRENT_EXTRACTORS=2
BATCH_SIZE=100
EXTRACTION_INTERVAL=60
```

### Production Configuration
```bash
# .env.production
DRY_RUN=false
RUN_ONCE=false
LOG_LEVEL=INFO
MAX_CONCURRENT_EXTRACTORS=10
BATCH_SIZE=1000
EXTRACTION_INTERVAL=300
ERROR_THRESHOLD=5
```

### Testing Configuration
```bash
# .env.test
DRY_RUN=true
RUN_ONCE=true
LOG_LEVEL=DEBUG
MAX_CONCURRENT_EXTRACTORS=1
BATCH_SIZE=10
ENABLE_JOBS_EXTRACTOR=true
ENABLE_MASTER_DATA_EXTRACTOR=false
ENABLE_PRODUCTION_EXTRACTOR=false
ENABLE_INVENTORY_EXTRACTOR=false
ENABLE_QUALITY_EXTRACTOR=false
```

## Configuration Validation

### Test Configuration
```python
# test_config.py
from base_extractor_enhanced import ExtractorConfig
from pydantic import ValidationError

try:
    config = ExtractorConfig()
    print("✓ Configuration valid")
    print(f"  PCN: {config.plex_customer_id}")
    print(f"  CDF Project: {config.cdf_project}")
    print(f"  Dry Run: {config.dry_run}")
except ValidationError as e:
    print("✗ Configuration invalid:")
    print(e)
```

### Verify Required Variables
```bash
# Check all required variables are set
python -c "
import os
required = [
    'PLEX_API_KEY',
    'PLEX_CUSTOMER_ID',
    'CDF_HOST',
    'CDF_PROJECT',
    'CDF_CLIENT_ID',
    'CDF_CLIENT_SECRET',
    'CDF_TOKEN_URL'
]
missing = [var for var in required if not os.getenv(var)]
if missing:
    print('Missing:', ', '.join(missing))
else:
    print('All required variables set')
"
```

## Environment-Specific Files

### File Structure
```
.env                  # Default configuration
.env.local           # Local overrides (git ignored)
.env.development     # Development settings
.env.staging         # Staging settings
.env.production      # Production settings
```

### Loading Order
1. `.env` - Base configuration
2. `.env.{environment}` - Environment-specific
3. `.env.local` - Local overrides (highest priority)

### Using Different Environments
```bash
# Load specific environment
export ENV=production
python orchestrator_enhanced.py

# Or directly specify
ENV=staging python orchestrator_enhanced.py
```

## Security Best Practices

### 1. Never Commit Secrets
```bash
# .gitignore
.env
.env.local
.env.*.local
*_secret*
*_key*
```

### 2. Use Secret Management
For production, consider:
- Azure Key Vault
- AWS Secrets Manager
- HashiCorp Vault
- Kubernetes Secrets

### 3. Rotate Credentials
- Regularly rotate API keys
- Update OAuth client secrets
- Monitor for exposed credentials

### 4. Minimal Permissions
- Use read-only credentials where possible
- Limit OAuth scopes to minimum required
- Use dataset-specific permissions in CDF

## Configuration Templates

### Create from Template
```bash
# Copy template
cp .env.example .env

# Edit with your values
nano .env
```

### Template File (.env.example)
```bash
# CDF Configuration
CDF_HOST=https://westeurope-1.cognitedata.com
CDF_PROJECT=your-project
CDF_TOKEN_URL=https://login.microsoftonline.com/your-tenant/oauth2/v2.0/token
CDF_CLIENT_ID=your-client-id
CDF_CLIENT_SECRET=your-client-secret

# Plex Configuration
PLEX_API_KEY=your-api-key
PLEX_CUSTOMER_ID=your-pcn
PLEX_BASE_URL=https://connect.plex.com

# Dataset IDs (get from CDF)
CDF_DATASET_PLEXSCHEDULING=
CDF_DATASET_PLEXPRODUCTION=
CDF_DATASET_PLEXINVENTORY=
CDF_DATASET_PLEXMASTER=
CDF_DATASET_PLEXQUALITY=
CDF_DATASET_PLEXMAINTENANCE=

# Facility
FACILITY_NAME=Your Facility
FACILITY_CODE=FAC01
FACILITY_TIMEZONE=UTC
FACILITY_COUNTRY=US

# Extractors
ENABLE_JOBS_EXTRACTOR=true
ENABLE_MASTER_DATA_EXTRACTOR=true
ENABLE_PRODUCTION_EXTRACTOR=true
ENABLE_INVENTORY_EXTRACTOR=true
ENABLE_QUALITY_EXTRACTOR=true

# Performance
MAX_CONCURRENT_EXTRACTORS=5
BATCH_SIZE=1000

# Mode
DRY_RUN=false
RUN_ONCE=true
```

## Troubleshooting Configuration

### Common Issues

1. **Variables not loading**: Ensure using `BaseSettings` not `BaseModel`
2. **Type errors**: Check data types match (int vs string)
3. **Missing variables**: Use `python-dotenv` to load .env
4. **Wrong scope**: Must use `["user_impersonation"]` for CDF

### Debug Configuration Loading
```python
# debug_config.py
import os
from dotenv import load_dotenv

# Force reload
load_dotenv(override=True)

# Print all env vars
for key, value in os.environ.items():
    if key.startswith(('PLEX_', 'CDF_')):
        # Mask secrets
        if 'SECRET' in key or 'KEY' in key:
            value = '***' + value[-4:] if len(value) > 4 else '***'
        print(f"{key}={value}")
```

## Migration from v1 to v2

### Key Changes
1. **OAuth Scope**: Changed from `.default` to `user_impersonation`
2. **Base Class**: Changed from `BaseModel` to `BaseSettings`
3. **New Variables**: Added facility configuration
4. **Orchestrator**: New orchestrator configuration section

### Migration Steps
1. Backup existing .env: `cp .env .env.backup`
2. Add new required variables (see above)
3. Update OAuth scope in code if modified
4. Test with dry-run mode first
5. Verify with `python test_connections.py`