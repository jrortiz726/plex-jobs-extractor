# Deployment Guide

## Deployment Options

### 1. Local Development
```bash
# Install dependencies
pip install -r requirements.txt

# Configure environment
cp .env.example .env
# Edit .env with your credentials

# Test connections
python test_connections.py

# Run orchestrator
python orchestrator_enhanced.py
```

### 2. Docker Deployment

#### Dockerfile
```dockerfile
FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create non-root user
RUN useradd -m -u 1000 extractor && chown -R extractor:extractor /app
USER extractor

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import sys; sys.exit(0)"

# Run orchestrator
CMD ["python", "orchestrator_enhanced.py"]
```

#### Docker Compose
```yaml
version: '3.8'

services:
  plex-extractors:
    build: .
    image: plex-extractors:latest
    container_name: plex-extractors
    restart: unless-stopped
    env_file:
      - .env
    volumes:
      - ./logs:/app/logs
    networks:
      - extractor-network
    deploy:
      resources:
        limits:
          cpus: '2'
          memory: 2G
        reservations:
          cpus: '1'
          memory: 1G

networks:
  extractor-network:
    driver: bridge
```

#### Build and Run
```bash
# Build image
docker build -t plex-extractors:latest .

# Run with docker-compose
docker-compose up -d

# View logs
docker-compose logs -f

# Stop
docker-compose down
```

### 3. Kubernetes Deployment

#### ConfigMap (configmap.yaml)
```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: plex-extractor-config
data:
  PLEX_BASE_URL: "https://connect.plex.com"
  CDF_HOST: "https://westeurope-1.cognitedata.com"
  CDF_PROJECT: "essc-sandbox-44"
  FACILITY_NAME: "RADEMO"
  FACILITY_CODE: "DEFAULT"
  ENABLE_JOBS_EXTRACTOR: "true"
  ENABLE_MASTER_DATA_EXTRACTOR: "true"
  ENABLE_PRODUCTION_EXTRACTOR: "true"
  ENABLE_INVENTORY_EXTRACTOR: "true"
  ENABLE_QUALITY_EXTRACTOR: "true"
  MAX_CONCURRENT_EXTRACTORS: "5"
  DRY_RUN: "false"
  RUN_ONCE: "false"
```

#### Secret (secret.yaml)
```yaml
apiVersion: v1
kind: Secret
metadata:
  name: plex-extractor-secrets
type: Opaque
stringData:
  PLEX_API_KEY: "your-plex-api-key"
  PLEX_CUSTOMER_ID: "340884"
  CDF_CLIENT_ID: "your-client-id"
  CDF_CLIENT_SECRET: "your-client-secret"
  CDF_TOKEN_URL: "https://login.microsoftonline.com/your-tenant/oauth2/v2.0/token"
```

#### Deployment (deployment.yaml)
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: plex-extractors
  labels:
    app: plex-extractors
spec:
  replicas: 1
  selector:
    matchLabels:
      app: plex-extractors
  template:
    metadata:
      labels:
        app: plex-extractors
    spec:
      containers:
      - name: orchestrator
        image: plex-extractors:latest
        imagePullPolicy: Always
        envFrom:
        - configMapRef:
            name: plex-extractor-config
        - secretRef:
            name: plex-extractor-secrets
        resources:
          requests:
            memory: "1Gi"
            cpu: "500m"
          limits:
            memory: "2Gi"
            cpu: "1000m"
        livenessProbe:
          exec:
            command:
            - python
            - -c
            - "import sys; sys.exit(0)"
          initialDelaySeconds: 30
          periodSeconds: 30
        readinessProbe:
          exec:
            command:
            - python
            - -c
            - "import sys; sys.exit(0)"
          initialDelaySeconds: 10
          periodSeconds: 10
```

#### Deploy to Kubernetes
```bash
# Create namespace
kubectl create namespace plex-extractors

# Apply configurations
kubectl apply -f configmap.yaml -n plex-extractors
kubectl apply -f secret.yaml -n plex-extractors
kubectl apply -f deployment.yaml -n plex-extractors

# Check status
kubectl get pods -n plex-extractors
kubectl logs -f deployment/plex-extractors -n plex-extractors
```

### 4. Azure Container Instances

#### Deploy Script (deploy-aci.sh)
```bash
#!/bin/bash

# Variables
RESOURCE_GROUP="plex-extractors-rg"
LOCATION="westeurope"
CONTAINER_NAME="plex-extractors"
IMAGE="plexextractors.azurecr.io/plex-extractors:latest"

# Create resource group
az group create --name $RESOURCE_GROUP --location $LOCATION

# Create container
az container create \
  --resource-group $RESOURCE_GROUP \
  --name $CONTAINER_NAME \
  --image $IMAGE \
  --cpu 2 \
  --memory 2 \
  --restart-policy Always \
  --environment-variables \
    PLEX_BASE_URL="https://connect.plex.com" \
    CDF_HOST="https://westeurope-1.cognitedata.com" \
    DRY_RUN="false" \
  --secure-environment-variables \
    PLEX_API_KEY="$PLEX_API_KEY" \
    CDF_CLIENT_SECRET="$CDF_CLIENT_SECRET"
```

### 5. AWS ECS Deployment

#### Task Definition (task-definition.json)
```json
{
  "family": "plex-extractors",
  "taskRoleArn": "arn:aws:iam::account-id:role/ecsTaskRole",
  "executionRoleArn": "arn:aws:iam::account-id:role/ecsExecutionRole",
  "networkMode": "awsvpc",
  "requiresCompatibilities": ["FARGATE"],
  "cpu": "1024",
  "memory": "2048",
  "containerDefinitions": [
    {
      "name": "plex-extractors",
      "image": "your-ecr-repo/plex-extractors:latest",
      "essential": true,
      "environment": [
        {"name": "PLEX_BASE_URL", "value": "https://connect.plex.com"},
        {"name": "CDF_HOST", "value": "https://westeurope-1.cognitedata.com"},
        {"name": "DRY_RUN", "value": "false"}
      ],
      "secrets": [
        {
          "name": "PLEX_API_KEY",
          "valueFrom": "arn:aws:secretsmanager:region:account:secret:plex-api-key"
        },
        {
          "name": "CDF_CLIENT_SECRET",
          "valueFrom": "arn:aws:secretsmanager:region:account:secret:cdf-client-secret"
        }
      ],
      "logConfiguration": {
        "logDriver": "awslogs",
        "options": {
          "awslogs-group": "/ecs/plex-extractors",
          "awslogs-region": "eu-west-1",
          "awslogs-stream-prefix": "ecs"
        }
      }
    }
  ]
}
```

## Production Considerations

### 1. High Availability

#### Multi-Region Deployment
```yaml
# Deploy to multiple regions
regions:
  - westeurope
  - northeurope
  - westus2
```

#### Load Balancing
- Use multiple extractor instances
- Partition work by facility or data type
- Implement leader election for coordination

### 2. Monitoring and Alerting

#### Prometheus Metrics
```python
# Add to extractors
from prometheus_client import Counter, Histogram, Gauge

extraction_counter = Counter('extractions_total', 'Total extractions', ['extractor', 'status'])
extraction_duration = Histogram('extraction_duration_seconds', 'Extraction duration', ['extractor'])
active_extractors = Gauge('active_extractors', 'Number of active extractors')
```

#### Grafana Dashboard
```json
{
  "dashboard": {
    "title": "Plex Extractors",
    "panels": [
      {
        "title": "Extraction Rate",
        "targets": [
          {
            "expr": "rate(extractions_total[5m])"
          }
        ]
      },
      {
        "title": "Error Rate",
        "targets": [
          {
            "expr": "rate(extractions_total{status='error'}[5m])"
          }
        ]
      }
    ]
  }
}
```

### 3. Logging

#### Centralized Logging
```yaml
# Fluentd configuration
<source>
  @type forward
  port 24224
</source>

<match plex.**>
  @type elasticsearch
  host elasticsearch
  port 9200
  logstash_format true
  logstash_prefix plex-extractors
</match>
```

### 4. Security

#### Secret Management
```bash
# Azure Key Vault
az keyvault secret set \
  --vault-name plex-vault \
  --name plex-api-key \
  --value $PLEX_API_KEY

# AWS Secrets Manager
aws secretsmanager create-secret \
  --name plex-api-key \
  --secret-string $PLEX_API_KEY
```

#### Network Security
```yaml
# Network policies
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: plex-extractor-policy
spec:
  podSelector:
    matchLabels:
      app: plex-extractors
  policyTypes:
  - Ingress
  - Egress
  egress:
  - to:
    - namespaceSelector: {}
    ports:
    - protocol: TCP
      port: 443  # HTTPS only
```

### 5. Backup and Recovery

#### State Backup
```bash
# Backup extractor state
kubectl exec -n plex-extractors deployment/plex-extractors -- \
  python -c "import pickle; pickle.dump(state, open('state.pkl', 'wb'))"

kubectl cp plex-extractors/plex-extractors-xxx:/app/state.pkl ./backups/state-$(date +%Y%m%d).pkl
```

#### Disaster Recovery
1. Regular CDF dataset backups
2. Configuration backups
3. Documented recovery procedures
4. Regular DR testing

## Deployment Checklist

### Pre-Deployment
- [ ] Test connections to Plex and CDF
- [ ] Verify all required environment variables
- [ ] Run dry-run mode successfully
- [ ] Review security configurations
- [ ] Set up monitoring and alerting

### Deployment
- [ ] Deploy to staging environment first
- [ ] Run smoke tests
- [ ] Monitor initial extraction cycles
- [ ] Verify data in CDF
- [ ] Check error rates and performance

### Post-Deployment
- [ ] Monitor health metrics
- [ ] Review logs for errors
- [ ] Verify data quality in CDF
- [ ] Document any issues
- [ ] Update runbooks

## Rollback Procedures

### Immediate Rollback
```bash
# Kubernetes
kubectl rollout undo deployment/plex-extractors -n plex-extractors

# Docker
docker-compose down
docker-compose up -d --scale plex-extractors=0
docker run -d --env-file .env.backup plex-extractors:previous

# Manual
kill -SIGTERM $(pgrep -f orchestrator_enhanced.py)
python orchestrator_enhanced.py  # with previous version
```

### Data Rollback
```python
# If bad data was written to CDF
from cleanup_datasets import cleanup_events
cleanup_events(client, dataset_id, start_time=rollback_time)
```

## Performance Tuning

### Resource Allocation
```yaml
# Based on load testing
resources:
  small:  # <1000 records/hour
    cpu: 0.5
    memory: 512Mi
  medium:  # 1000-10000 records/hour
    cpu: 1
    memory: 1Gi
  large:  # >10000 records/hour
    cpu: 2
    memory: 2Gi
```

### Scaling Strategy
```bash
# Horizontal scaling
kubectl scale deployment/plex-extractors --replicas=3

# Vertical scaling
kubectl set resources deployment/plex-extractors \
  --requests=memory=2Gi,cpu=1 \
  --limits=memory=4Gi,cpu=2
```

## Maintenance

### Regular Tasks
- Weekly: Review error logs
- Monthly: Update dependencies
- Quarterly: Performance review
- Yearly: Security audit

### Update Procedures
```bash
# Test updates in staging
docker build -t plex-extractors:staging .
docker run --env-file .env.staging plex-extractors:staging

# Deploy to production
docker tag plex-extractors:staging plex-extractors:latest
docker push plex-extractors:latest
kubectl rollout restart deployment/plex-extractors
```