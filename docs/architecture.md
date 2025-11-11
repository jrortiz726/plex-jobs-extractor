# Architecture Overview

## System Architecture

```
┌─────────────────┐     ┌─────────────────────┐     ┌──────────────────┐
│                 │     │                     │     │                  │
│   Plex ERP      │────▶│  Enhanced          │────▶│  Cognite Data    │
│   (Source)      │     │  Extractors        │     │  Fusion (CDF)    │
│                 │     │                     │     │                  │
└─────────────────┘     └─────────────────────┘     └──────────────────┘
        ▲                         │                           │
        │                         │                           │
        └─────────────────────────┴───────────────────────────┘
                        Orchestrator Control Loop
```

## Component Architecture

### Core Components

#### 1. Base Extractor (base_extractor_enhanced.py)
Abstract base class providing common functionality:
- **Configuration Management**: Pydantic-based settings from .env
- **Authentication**: OAuth2 for CDF, API key for Plex
- **Async Operations**: AsyncCDFWrapper for concurrent CDF operations
- **Error Handling**: Retry logic, circuit breaker, error aggregation
- **ID Resolution**: Maps external IDs to CDF asset IDs
- **Logging**: Structured logging with context
- **Metrics**: Performance and health metrics collection

```python
class EnhancedBaseExtractor(ABC):
    def __init__(self, config: ExtractorConfig):
        self.config = config
        self.client = self._init_cdf_client()
        self.async_cdf = AsyncCDFWrapper(self.client)
        self.id_resolver = IDResolver(self.client)
        self.retry_handler = RetryHandler()
        self.naming = NamingConvention(config.facility.pcn)
```

#### 2. Orchestrator (orchestrator_enhanced.py)
Manages all extractors with:
- **Concurrent Execution**: Runs multiple extractors in parallel
- **Health Monitoring**: Tracks extractor status and success rates
- **Configuration**: Enables/disables extractors via environment
- **Graceful Shutdown**: Handles SIGINT/SIGTERM signals
- **Metrics Aggregation**: Collects metrics from all extractors

```python
class EnhancedOrchestrator:
    async def start(self):
        # Create extractor tasks
        for extractor_type in self.enabled_extractors:
            task = asyncio.create_task(
                self._run_extractor(extractor_type)
            )
        # Monitor health
        asyncio.create_task(self._monitor_health())
```

### Data Flow Architecture

#### 1. Extraction Pipeline
```
Plex API → Fetch → Transform → Validate → Upsert → CDF
           ↓        ↓          ↓          ↓
         Retry   Normalize   Schema    Dedupe
```

#### 2. Asset Hierarchy
```
Facility (Root)
├── Master Data
│   ├── Parts Library
│   ├── Operations Library
│   └── Resources Library
├── Production
│   ├── Workcenters
│   └── Production Lines
├── Inventory
│   ├── Locations
│   └── Containers
└── Quality
    ├── Inspection Points
    └── Control Plans
```

#### 3. Event Model
Events link to assets via external IDs:
- **Jobs**: Link to workcenters and parts
- **Production**: Link to workcenters and operations
- **Inventory**: Link to containers and locations
- **Quality**: Link to parts and inspection points

### Key Design Patterns

#### 1. Async/Await Pattern
All I/O operations use async/await for concurrency:
```python
async def extract(self):
    tasks = [
        self._extract_parts(),
        self._extract_operations(),
        self._extract_resources()
    ]
    results = await asyncio.gather(*tasks)
```

#### 2. Repository Pattern
Separates data access from business logic:
```python
class PartRepository:
    async def fetch_all(self) -> List[Part]:
        response = await self.api_client.get("/parts")
        return [Part(**item) for item in response]
```

#### 3. Factory Pattern
Creates appropriate extractors based on configuration:
```python
def create_extractor(extractor_type: ExtractorType):
    match extractor_type:
        case ExtractorType.JOBS:
            return EnhancedJobsExtractor(config)
        case ExtractorType.MASTER:
            return EnhancedMasterDataExtractor(config)
```

#### 4. Circuit Breaker Pattern
Prevents cascade failures:
```python
class CircuitBreaker:
    def __init__(self, failure_threshold=5, timeout=60):
        self.failure_count = 0
        self.last_failure_time = None
        self.state = "CLOSED"  # CLOSED, OPEN, HALF_OPEN
```

### Data Models

#### Asset Model
```python
@dataclass
class AssetData:
    external_id: str
    name: str
    description: Optional[str]
    parent_external_id: Optional[str]
    metadata: Dict[str, Any]
    dataset_id: int
```

#### Event Model
```python
@dataclass
class EventData:
    external_id: str
    type: str
    subtype: Optional[str]
    start_time: int
    end_time: Optional[int]
    asset_external_ids: List[str]
    metadata: Dict[str, Any]
    dataset_id: int
```

#### Time Series Model
```python
@dataclass
class TimeSeriesData:
    external_id: str
    name: str
    unit: Optional[str]
    asset_id: Optional[int]
    metadata: Dict[str, Any]
    dataset_id: int
```

### Security Architecture

#### Authentication
1. **Plex API**: API key in headers
2. **CDF OAuth2**: Client credentials flow with scope "user_impersonation"

#### Data Security
1. **Credentials**: Stored in environment variables, never in code
2. **Logging**: Sensitive data redacted from logs
3. **TLS**: All API communications over HTTPS
4. **Access Control**: Dataset-level permissions in CDF

### Scalability Considerations

#### Horizontal Scaling
- Extractors can run on multiple instances
- Use Redis/database for coordination
- Partition work by facility or data type

#### Vertical Scaling
- Increase concurrent operations via MAX_CONCURRENT_EXTRACTORS
- Adjust batch sizes for optimal throughput
- Use connection pooling for API clients

#### Performance Optimization
1. **Batch Operations**: Group API calls when possible
2. **Caching**: ID resolver caches for session
3. **Incremental Updates**: Change detection for master data
4. **Async I/O**: Non-blocking operations throughout

### Monitoring & Observability

#### Metrics
- Extraction success/failure rates
- API call latency
- Records processed per minute
- Error rates by type

#### Health Checks
- Extractor status (running, stopped, error)
- API connectivity
- CDF dataset accessibility
- Resource utilization

#### Logging
Structured JSON logging with:
- Correlation IDs for request tracing
- Context (PCN, facility, extractor)
- Performance metrics
- Error details with stack traces

### Deployment Architecture

#### Container Deployment
```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .
CMD ["python", "orchestrator_enhanced.py"]
```

#### Kubernetes Deployment
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: plex-extractors
spec:
  replicas: 1
  template:
    spec:
      containers:
      - name: orchestrator
        image: plex-extractors:latest
        envFrom:
        - secretRef:
            name: plex-credentials
```

### Fault Tolerance

#### Retry Strategy
- Exponential backoff with jitter
- Maximum 3 attempts by default
- Different strategies for different error types

#### Error Recovery
1. **Transient Errors**: Automatic retry
2. **API Limits**: Backoff and queue
3. **Data Errors**: Log and continue
4. **Fatal Errors**: Alert and stop

#### State Management
- Stateless extractors (can restart anytime)
- Progress tracking via CDF metadata
- Idempotent operations (safe to retry)

## Technology Stack

### Core Technologies
- **Python 3.11+**: Modern Python with type hints
- **asyncio**: Asynchronous I/O
- **httpx**: Async HTTP client with HTTP/2
- **pydantic**: Data validation and settings
- **cognite-sdk**: Official CDF Python SDK

### Supporting Libraries
- **structlog**: Structured logging
- **tenacity**: Retry logic
- **python-dotenv**: Environment management
- **pytest-asyncio**: Async testing

## Future Enhancements

### Planned Features
1. **GraphQL Support**: For more efficient Plex queries
2. **WebSocket Streaming**: Real-time data updates
3. **Machine Learning**: Anomaly detection in extracted data
4. **Data Quality Scoring**: Automated quality metrics
5. **Multi-facility Support**: Parallel extraction from multiple facilities

### Architecture Evolution
1. **Event-Driven**: Move to event-driven architecture
2. **Microservices**: Split extractors into separate services
3. **Message Queue**: Add Kafka/RabbitMQ for decoupling
4. **Data Lake**: Raw data storage before transformation
5. **CDC Support**: Change Data Capture for real-time sync