# Troubleshooting Guide

## Common Issues and Solutions

### 1. CDF Authentication Failures

#### Problem: "403 Forbidden" or "401 Unauthorized" when writing to CDF

**Symptoms:**
```
Error: 403 Forbidden - insufficient permissions
```

**Root Cause:** Incorrect OAuth scope configuration

**Solution:**
```python
# In base_extractor_enhanced.py (~line 180)
# WRONG:
scopes=[f"{self.config.cdf_host}/.default"]

# CORRECT:
scopes=["user_impersonation"]
```

**Verification:**
```bash
python test_connections.py
# Should show "✓ CDF connection successful"
```

---

### 2. Environment Variables Not Loading

#### Problem: Pydantic validation errors or missing configuration

**Symptoms:**
```
pydantic.error_wrappers.ValidationError: 1 validation error for ExtractorConfig
```

**Root Cause:** Using `BaseModel` instead of `BaseSettings`

**Solution:**
```python
# WRONG:
from pydantic import BaseModel
class ExtractorConfig(BaseModel):
    ...

# CORRECT:
from pydantic_settings import BaseSettings
class ExtractorConfig(BaseSettings):
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "ignore"  # Important!
```

**Verification:**
```python
# Test configuration loading
from base_extractor_enhanced import ExtractorConfig
config = ExtractorConfig()
print(config.plex_customer_id)  # Should print your PCN
```

---

### 3. Parent Asset Reference Errors

#### Problem: "Reference to unknown parent with externalId"

**Symptoms:**
```
Error: Reference to unknown parent with externalId PCN340884_facility_340884
```

**Root Cause:** Facility asset doesn't exist

**Solution:**
The `ensure_facility_asset()` method was added to create the facility asset:
```python
# In base_extractor_enhanced.py
async def ensure_facility_asset(self) -> None:
    """Ensure the facility asset exists"""
    facility_asset = Asset(
        external_id=f"PCN{pcn}_facility_{pcn}",
        name=f"{facility_name} - Facility",
        ...
    )
    await self.async_cdf.upsert_assets([facility_asset])
```

**Manual Fix:**
```bash
# Create facility asset manually
python test_facility_asset.py
```

---

### 4. Dataclass Field Ordering Errors

#### Problem: "non-default argument follows default argument"

**Symptoms:**
```
TypeError: non-default argument 'result' follows default argument
```

**Root Cause:** Incorrect field ordering in dataclasses

**Solution:**
```python
# WRONG:
@dataclass
class InspectionResult:
    id: InspectionId
    checksheet_id: Optional[ChecksheetId] = None
    result: str  # Error: non-default after default

# CORRECT:
@dataclass
class InspectionResult:
    id: InspectionId
    result: str  # Required fields first
    checksheet_id: Optional[ChecksheetId] = None  # Optional fields last
```

---

### 5. HTTP/2 Support Missing

#### Problem: httpx requires HTTP/2 support

**Symptoms:**
```
ImportError: Install httpx[http2] for HTTP/2 support
```

**Solution:**
```bash
pip install 'httpx[http2]'
# or
pip install -r requirements.txt
```

---

### 6. Plex API Connection Issues

#### Problem: 404 or 400 errors from Plex API

**Common Causes:**
1. Wrong base URL
2. Missing or incorrect headers
3. Invalid endpoint

**Solution:**
```python
# Correct Plex API configuration
PLEX_BASE_URL = "https://connect.plex.com"  # NOT cloud.plex.com
headers = {
    "X-Plex-Connect-Api-Key": api_key,
    "X-Plex-Connect-Customer-Id": customer_id,
}
```

**Verification:**
```bash
python test_connections.py
# Should show "✓ Plex API connection successful"
```

---

### 7. No Data Being Extracted

#### Problem: Extractors run but don't extract any data

**Possible Causes:**
1. Empty API responses (demo environment)
2. Date range filters too restrictive
3. Incorrect dataset IDs

**Debugging Steps:**
```bash
# 1. Check API responses
DRY_RUN=true python jobs_extractor_enhanced.py

# 2. Verify dataset IDs exist
python -c "
from cognite.client import CogniteClient
client = CogniteClient()
datasets = client.data_sets.list()
for d in datasets:
    print(f'{d.name}: {d.id}')
"

# 3. Check logs for "no_X_found" messages
grep "no_.*_found" logs/*.log
```

---

### 8. Async Event Loop Errors

#### Problem: "RuntimeError: This event loop is already running"

**Cause:** Jupyter notebooks or nested async calls

**Solution:**
```python
# For Jupyter notebooks
import nest_asyncio
nest_asyncio.apply()

# For scripts, use asyncio.run()
if __name__ == "__main__":
    asyncio.run(main())
```

---

### 9. Memory Issues with Large Datasets

#### Problem: Out of memory when processing large datasets

**Solution:**
```python
# Use batch processing
BATCH_SIZE = 1000
for i in range(0, len(items), BATCH_SIZE):
    batch = items[i:i+BATCH_SIZE]
    await process_batch(batch)
```

**Environment Variables:**
```bash
# Reduce batch sizes
export MAX_BATCH_SIZE=500
export MAX_CONCURRENT_OPERATIONS=3
```

---

### 10. StateTracker AttributeError

#### Problem: "'StateTracker' object has no attribute 'get_state'"

**Symptoms:**
```
AttributeError: 'StateTracker' object has no attribute 'get_state'
```

**Cause:** Incorrect method names used in master_data_extractor

**Solution:**
```python
# WRONG:
self.last_full_refresh = self.state_tracker.get_state('last_full_refresh', default)
self.state_tracker.set_state('last_full_refresh', value)

# CORRECT:
last_refresh_time = self.state_tracker.get_last_extraction_time('master_full_refresh')
self.state_tracker.set_last_extraction_time('master_full_refresh', value)
```

---

### 11. Jobs Parsing Errors

#### Problem: "int() argument must be a string... not 'NoneType'"

**Symptoms:**
```
job_parse_error: int() argument must be a string, not 'NoneType'
```

**Cause:** Some fields are null in Plex demo data

**Status:** Non-critical warning, handled gracefully

**Solution:** Already handled with try/except in code:
```python
try:
    quantity = int(job.get('quantity', 0)) if job.get('quantity') else 0
except (TypeError, ValueError):
    self.logger.warning("job_parse_error", job_id=job_id)
    quantity = 0
```

---

## Debugging Tools

### 1. Test Connections
```bash
python test_connections.py
```
Verifies both Plex API and CDF connectivity

### 2. Inspect Datasets
```bash
python inspect_datasets.py
```
Shows what data exists in CDF without modifying

### 3. Cleanup Datasets
```bash
python cleanup_datasets.py
```
Safely removes all data for fresh testing

### 4. Dry Run Mode
```bash
DRY_RUN=true python orchestrator_enhanced.py
```
Runs extractors without writing to CDF

### 5. Verbose Logging
```bash
export LOG_LEVEL=DEBUG
python orchestrator_enhanced.py
```

### 6. Single Extractor Testing
```bash
# Test individual extractors
python jobs_extractor_enhanced.py
python master_data_extractor_enhanced.py
```

---

## Error Messages Reference

| Error Message | Likely Cause | Solution |
|--------------|--------------|----------|
| "403 Forbidden" | Wrong OAuth scope | Use "user_impersonation" scope |
| "Reference to unknown parent" | Missing facility asset | Run ensure_facility_asset() |
| "Validation error for ExtractorConfig" | .env not loading | Use BaseSettings |
| "non-default argument follows default" | Dataclass field order | Reorder fields |
| "Plex API client error: 400" | Invalid request params | Check date formats |
| "Plex API resource not found" | Wrong endpoint | Verify API documentation |
| "int() argument... NoneType" | Null fields in data | Non-critical, handled |

---

## Performance Issues

### Slow Extraction
1. Increase concurrent operations: `MAX_CONCURRENT_EXTRACTORS=10`
2. Increase batch size: `BATCH_SIZE=1000`
3. Use connection pooling: Already implemented in httpx

### High Memory Usage
1. Reduce batch size: `BATCH_SIZE=100`
2. Enable streaming for large responses
3. Process data incrementally

### API Rate Limits
1. Implement exponential backoff (already done)
2. Reduce concurrent operations
3. Add delays between requests if needed

---

## Getting Help

### Log Files
Check logs for detailed error information:
```bash
# View recent errors
grep ERROR extraction_log.txt | tail -20

# View warnings
grep WARNING extraction_log.txt | tail -20
```

### Debug Mode
Enable debug logging:
```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

### Support Checklist
Before requesting help:
1. ✓ Check this troubleshooting guide
2. ✓ Verify .env configuration
3. ✓ Run test_connections.py
4. ✓ Check logs for specific errors
5. ✓ Try dry-run mode
6. ✓ Test individual extractors
7. ✓ Review CLAUDE.md for session context

---

## Recovery Procedures

### Full Reset
```bash
# 1. Clean all datasets
python cleanup_datasets.py

# 2. Verify clean
python inspect_datasets.py

# 3. Test connections
python test_connections.py

# 4. Run with dry-run
DRY_RUN=true python orchestrator_enhanced.py

# 5. Run actual extraction
python orchestrator_enhanced.py
```

### Partial Recovery
```bash
# Resume specific extractors
ENABLE_JOBS_EXTRACTOR=true \
ENABLE_MASTER_DATA_EXTRACTOR=false \
python orchestrator_enhanced.py
```

### Emergency Stop
```bash
# Graceful shutdown
kill -SIGTERM <orchestrator_pid>

# Force stop
kill -SIGKILL <orchestrator_pid>
```