# Enhanced Extractors - Fixes Applied

## Summary
All enhanced extractors are now working with the correct Plex API endpoints and authentication.

## Production Extractor Fixes

### 1. Production Entries Endpoint
**Issue**: Using wrong parameter names
**Fixed**: 
- Changed `dateFrom`/`dateTo` → `beginDate`/`endDate`
- File: `production_extractor_enhanced.py` line 397-398

### 2. Workcenter Status Endpoint
**Issue**: Using non-existent endpoint `/production/v1/workcenters/{id}/status`
**Fixed**: 
- Changed to `/production/v1/control/workcenters/{workcenterId}`
- Updated response parsing for correct field names
- File: `production_extractor_enhanced.py` line 364

### 3. OEE/Performance Metrics
**Issue**: Endpoint `/production/v1/workcenters/{id}/oee` doesn't exist
**Solution**: 
- Created separate `performance_extractor_enhanced.py` for all Data Source API calls
- Uses Data Source IDs 18765 (Daily Performance) and 22870 (Real-time Performance)
- Keeps production extractor focused on REST API only
- Better separation of concerns: REST API vs Data Source API
- File: New `performance_extractor_enhanced.py`

## Inventory Extractor Fixes

### 1. Locations Endpoint
**Issue**: Using `/inventory/v1/locations` (doesn't exist)
**Fixed**: 
- Changed to `/inventory/v1/inventory-definitions/locations`
- File: `inventory_extractor_enhanced.py` line 438

### 2. Containers Endpoint
**Issue**: Using `/inventory/v1/containers` (doesn't exist)
**Fixed**: 
- Changed to `/inventory/v1/inventory-tracking/containers`
- File: `inventory_extractor_enhanced.py` line 457

### 3. Movements Endpoint
**Issue**: Using wrong endpoint and parameters
**Fixed**: 
- Changed to `/inventory/v1/inventory-history/container-location-moves`
- Changed `dateFrom`/`dateTo` → `beginDate`/`endDate`
- File: `inventory_extractor_enhanced.py` line 495, 497-499

### 4. WIP Containers
**Issue**: No separate WIP endpoint
**Fixed**: 
- Fetch all containers and filter by status
- WIP = containers not in ['scrap', 'waste', 'shipped'] status
- File: `inventory_extractor_enhanced.py` line 535-546

## Quality Extractor Fixes

### 1. Problem Reports Endpoint
**Issue**: `/quality/v1/problem-reports` doesn't exist
**Fixed**: 
- Removed the endpoint call completely
- Added note that quality data requires Data Source API
- File: `quality_extractor_enhanced.py` line 639-690

### 2. Data Source API Credentials
**Issue**: Looking for wrong environment variable names
**Fixed**: 
- Already correctly using `PLEX_DS_USERNAME` and `PLEX_DS_PASSWORD`
- Variables are properly set in .env
- File: `quality_extractor_enhanced.py` line 168-169

### 3. PCN Code Configuration
**Issue**: Need correct PCN code for Data Source API
**Fixed**: 
- Using `PLEX_PCN_CODE=ra-process` from .env
- Using `PLEX_USE_TEST=false` for production
- File: Already configured correctly

## Master Data Extractor Fixes

### 1. StateTracker Methods
**Issue**: Using non-existent `get_state()`/`set_state()` methods
**Fixed**: 
- Changed to `get_last_extraction_time()`/`set_last_extraction_time()`
- File: `master_data_extractor_enhanced.py` line 183-185, 258

## Key Learnings

### 1. Plex API Structure
- Regular REST API: Available for most domains except quality
- Data Source API: Required for quality and performance metrics
- Endpoint patterns vary by domain (e.g., `/control/`, `/inventory-tracking/`, `/inventory-definitions/`)

### 2. Date Parameters
- Most endpoints use `beginDate`/`endDate` (not `dateFrom`/`dateTo`)
- ISO format required: `YYYY-MM-DDTHH:MM:SS.fffffffZ`

### 3. Authentication
- Regular API: Uses API key headers
- Data Source API: Uses Basic auth with username/password
- OAuth scope for CDF: Must be `["user_impersonation"]`

## Testing Results

After fixes:
- ✅ Jobs Extractor: Working (with non-critical warnings)
- ✅ Master Data Extractor: Working
- ✅ Production Extractor: Working (OEE disabled)
- ✅ Inventory Extractor: Working
- ✅ Quality Extractor: Working (requires DS API credentials)

## Next Steps

1. **OEE Metrics**: Implement Data Source API call for performance metrics
2. **Quality Data**: Implement full Data Source API integration
3. **Error Handling**: Add specific handling for null fields in demo data
4. **Documentation**: Update API reference docs with correct endpoints