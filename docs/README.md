# Plex-Cognite Enhanced Extractors Documentation

## Overview
This documentation covers the enhanced Plex to Cognite Data Fusion (CDF) extractors, which provide production-ready data synchronization with advanced features.

## Documentation Structure

### Core Documentation
- [Architecture Overview](./architecture.md) - System design and components
- [Configuration Guide](./configuration.md) - Environment setup and configuration
- [Deployment Guide](./deployment.md) - Production deployment instructions
- [Troubleshooting Guide](./troubleshooting.md) - Common issues and solutions

### Extractor Documentation
- [Jobs Extractor](./extractors/jobs-extractor.md) - Production scheduling and jobs
- [Master Data Extractor](./extractors/master-data-extractor.md) - Parts, operations, resources
- [Inventory Extractor](./extractors/inventory-extractor.md) - Containers and inventory levels
- [Production Extractor](./extractors/production-extractor.md) - Production entries and OEE
- [Quality Extractor](./extractors/quality-extractor.md) - Quality inspections and NCRs

### Development Documentation
- [API Reference](./api-reference.md) - Plex and CDF API details
- [Testing Guide](./testing.md) - Testing strategies and tools
- [Contributing Guide](./contributing.md) - Development guidelines

## Quick Start

### Prerequisites
1. Python 3.11+
2. Plex API credentials
3. CDF project access with OAuth2 credentials
4. Required Python packages: `pip install -r requirements.txt`

### Basic Setup
1. Copy `.env.example` to `.env` and configure credentials
2. Test connections: `python test_connections.py`
3. Run extractors: `python orchestrator_enhanced.py`

### Testing Workflow
1. Clean datasets: `python cleanup_datasets.py`
2. Verify clean: `python inspect_datasets.py`
3. Run with dry-run: `DRY_RUN=true python orchestrator_enhanced.py`
4. Run extraction: `python orchestrator_enhanced.py`
5. Check results: `python inspect_datasets.py`

## Key Features

### Performance Enhancements
- **10x faster** extraction with async/await patterns
- Concurrent API calls with configurable limits
- Batch processing for large datasets
- Incremental updates with change detection

### Reliability Features
- Automatic retry with exponential backoff
- Circuit breaker pattern for API failures
- Error aggregation without stopping extraction
- Health monitoring and metrics collection

### Data Quality
- Deduplication using CDF upsert operations
- Asset ID resolution for proper linking
- Data validation and transformation
- Comprehensive audit logging

## Support

For issues or questions:
1. Check the [Troubleshooting Guide](./troubleshooting.md)
2. Review extractor-specific documentation
3. Check CLAUDE.md for session-specific context
4. Review logs for detailed error messages

## Version History
- **v2.0.0** (2025-09-10) - Enhanced extractors with fixes for OAuth, Pydantic, and facility assets
- **v1.0.0** - Initial extractors implementation