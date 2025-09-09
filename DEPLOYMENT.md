# Plex-CDF Extractor Deployment Guide

## Overview

The Plex-CDF extractors are designed to run **outside of CDF** as standalone applications that push data to CDF via APIs. CDF does not provide native hosting for custom extractors - they must be deployed on your own infrastructure.

## Architecture

```
┌─────────────────┐     API Calls      ┌──────────────┐
│                 │ ──────────────────> │              │
│  Plex MES API   │                     │   Your       │
│                 │ <────────────────── │   Extractor  │
└─────────────────┘     Fetch Data      │   Service    │
                                        │              │
                                        └──────────────┘
                                               │
                                               │ Push Data
                                               │
                                               ▼
                                        ┌──────────────┐
                                        │              │
                                        │  Cognite     │
                                        │  Data Fusion │
                                        │              │
                                        └──────────────┘
```

## Deployment Options

### 1. Orchestrator Mode (Recommended)

The `orchestrator.py` manages all extractors with proper scheduling:

```bash
# Run all extractors continuously
python orchestrator.py --mode continuous

# Run once for testing
python orchestrator.py --mode once

# Test each extractor
python orchestrator.py --mode test
```

**Features:**
- Automatic scheduling (configurable intervals)
- Error handling and retry logic
- Health monitoring and alerts
- Graceful shutdown
- Status reporting to CDF

### 2. Docker Deployment (Recommended for Production)

#### Local Docker

```bash
# Build the image
docker build -t plex-cdf-extractor .

# Run with environment file
docker run -d \
  --name plex-extractor \
  --env-file .env \
  -v $(pwd)/logs:/app/logs \
  -v $(pwd)/state:/app/state \
  --restart unless-stopped \
  plex-cdf-extractor
```

#### Docker Compose

```bash
# Start all services
docker-compose up -d

# View logs
docker-compose logs -f plex-extractor

# Stop services
docker-compose down
```

### 3. Kubernetes Deployment

For production environments with Kubernetes:

```bash
# Create namespace and deploy
kubectl apply -f deploy/kubernetes.yaml

# Check status
kubectl get pods -n plex-extractors

# View logs
kubectl logs -n plex-extractors -l app=plex-extractor -f
```

### 4. Cloud Platform Deployments

#### Azure Container Instances

```bash
# Deploy to Azure
chmod +x deploy/azure-container-instance.sh
./deploy/azure-container-instance.sh
```

#### AWS ECS (Fargate)

```bash
# Deploy to AWS
chmod +x deploy/aws-ecs-deploy.sh
./deploy/aws-ecs-deploy.sh
```

#### Google Cloud Run

```bash
# Build and deploy
gcloud builds submit --tag gcr.io/PROJECT-ID/plex-extractor
gcloud run deploy plex-extractor \
  --image gcr.io/PROJECT-ID/plex-extractor \
  --platform managed \
  --region europe-west1 \
  --env-vars-file .env.yaml
```

### 5. Linux VM with Systemd

For traditional VM deployments:

```bash
# Install as systemd service
sudo chmod +x deploy/install-systemd.sh
sudo ./deploy/install-systemd.sh

# Service will auto-start and run continuously
systemctl status plex-extractor
```

## Configuration

### Environment Variables

Create a `.env` file with your configuration:

```bash
# Cognite Data Fusion
CDF_HOST=https://westeurope-1.cognitedata.com
CDF_PROJECT=your-project
CDF_CLIENT_ID=your-client-id
CDF_CLIENT_SECRET=your-secret
CDF_TOKEN_URL=https://your-auth0.auth0.com/oauth/token

# Plex API
PLEX_API_KEY=your-api-key
PLEX_CUSTOMER_ID=340884

# Facility Info
FACILITY_NAME=Main Manufacturing Plant
FACILITY_CODE=MMP

# Dataset IDs (from setup_datasets.py)
CDF_DATASET_PLEXPRODUCTION=869709519571885
CDF_DATASET_PLEXSCHEDULING=7195113081024241
CDF_DATASET_PLEXQUALITY=2881941287917280
CDF_DATASET_PLEXINVENTORY=8139681212033756
CDF_DATASET_PLEXMAINTENANCE=5080535668683118
CDF_DATASET_PLEXMASTER=4945797542267648

# Extraction Intervals (seconds)
MASTER_EXTRACTION_INTERVAL=86400    # Daily
JOBS_EXTRACTION_INTERVAL=300        # 5 minutes
PRODUCTION_EXTRACTION_INTERVAL=300  # 5 minutes
INVENTORY_EXTRACTION_INTERVAL=300   # 5 minutes

# Optional
LOG_LEVEL=INFO
BATCH_SIZE=1000
MAX_RETRIES=3
```

### Multi-Facility Deployment

For multiple facilities, deploy separate instances with different PCN values:

```yaml
# facility-a.env
PLEX_CUSTOMER_ID=340884
FACILITY_NAME=Facility A

# facility-b.env
PLEX_CUSTOMER_ID=123456
FACILITY_NAME=Facility B
```

Run separate containers/services for each facility:

```bash
docker run -d --name extractor-facility-a --env-file facility-a.env plex-extractor
docker run -d --name extractor-facility-b --env-file facility-b.env plex-extractor
```

## Monitoring

### Health Checks

The orchestrator sends regular heartbeat events to CDF:

```python
# Query heartbeat events in CDF
events = client.events.list(
    type="orchestrator_heartbeat",
    metadata={"pcn": "340884"}
)
```

### Metrics

Monitor extraction performance:

```python
# Query extraction metrics
events = client.events.list(
    type="extractor_error",
    subtype="repeated_failure"
)
```

### Logging

- **Docker**: `docker logs plex-extractor`
- **Kubernetes**: `kubectl logs -n plex-extractors -l app=plex-extractor`
- **Systemd**: `journalctl -u plex-extractor -f`
- **File logs**: Check `./logs/orchestrator.log`

## Scaling Considerations

### Horizontal Scaling

For high-volume facilities, run extractors separately:

```bash
# Run each extractor in its own container
docker run -d --name jobs-extractor python jobs_extractor.py
docker run -d --name production-extractor python production_extractor.py
docker run -d --name inventory-extractor python inventory_extractor.py
```

### Rate Limiting

Configure API rate limits in environment:

```bash
PLEX_API_RATE_LIMIT=100  # Requests per minute
PLEX_API_CONCURRENT=5    # Concurrent connections
```

### Resource Requirements

**Minimum:**
- CPU: 0.5 cores
- Memory: 512MB
- Storage: 1GB

**Recommended:**
- CPU: 1-2 cores
- Memory: 2GB
- Storage: 10GB (for logs and state)

## Troubleshooting

### Connection Issues

```bash
# Test connections
python test_connections.py

# Check specific extractor
python orchestrator.py --mode test --extractors jobs
```

### Authentication Failures

1. Verify OAuth scope is `user_impersonation`
2. Check Auth0 configuration
3. Ensure client has proper CDF permissions

### Data Not Appearing in CDF

1. Check PCN prefix in all IDs
2. Verify dataset IDs are correct
3. Check extraction logs for errors
4. Confirm data range with `EXTRACTION_START_DATE`

### High Memory Usage

1. Reduce `BATCH_SIZE` in environment
2. Increase extraction intervals
3. Run extractors separately

## Security Best Practices

1. **Never commit `.env` files** - Use secrets management
2. **Use least privilege** - Grant minimal required permissions
3. **Rotate credentials regularly**
4. **Enable audit logging** in CDF
5. **Use private networks** where possible
6. **Implement firewall rules** to restrict access

## Backup and Recovery

### State Backup

```bash
# Backup state directory
tar -czf state-backup-$(date +%Y%m%d).tar.gz ./state/

# Restore state
tar -xzf state-backup-20240315.tar.gz
```

### Configuration Backup

Store configuration in version control (excluding secrets):

```bash
git add deploy/ *.py requirements.txt
git commit -m "Deployment configuration"
```

## Maintenance

### Updating Extractors

```bash
# Stop service
docker-compose down

# Pull latest code
git pull

# Rebuild and start
docker-compose build
docker-compose up -d
```

### Log Rotation

Configure log rotation to prevent disk filling:

```yaml
# docker-compose.yml
logging:
  driver: json-file
  options:
    max-size: "10m"
    max-file: "3"
```

## Support

For issues:
1. Check logs for error messages
2. Run `test_connections.py` to verify connectivity
3. Review CLAUDE.md for PCN requirements
4. Consult Cognite documentation
5. Contact Plex API support for API issues

## Next Steps

1. Choose deployment platform based on your infrastructure
2. Configure environment variables
3. Deploy using appropriate method
4. Monitor initial extraction runs
5. Set up alerting for failures
6. Document facility-specific configurations