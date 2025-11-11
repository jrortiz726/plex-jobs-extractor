#!/usr/bin/env python3
"""
Enhanced Plex-CDF Extractor Orchestrator

Manages all enhanced extractors with concurrent execution, health monitoring,
and advanced scheduling capabilities.
"""

import asyncio
import os
import signal
import sys
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Any, Set
from dataclasses import dataclass, field
from enum import Enum
import traceback
from pathlib import Path

from dotenv import load_dotenv
import structlog
from pydantic import BaseModel, Field, validator

# Load environment variables
load_dotenv()

# Configure structured logging
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.dict_tracebacks,
        structlog.dev.ConsoleRenderer()
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger()


class ExtractorStatus(Enum):
    """Status of an extractor"""
    IDLE = "idle"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    DISABLED = "disabled"


class ExtractorType(Enum):
    """Types of extractors with their default intervals"""
    JOBS = ("jobs", 300)  # 5 minutes
    PRODUCTION = ("production", 300)  # 5 minutes
    INVENTORY = ("inventory", 600)  # 10 minutes
    MASTER_DATA = ("master_data", 3600)  # 1 hour
    QUALITY = ("quality", 300)  # 5 minutes
    PERFORMANCE = ("performance", 900)  # 15 minutes - uses Data Source API
    
    def __init__(self, name: str, default_interval: int):
        self.extractor_name = name
        self.default_interval = default_interval


@dataclass
class ExtractorHealth:
    """Health status of an extractor"""
    name: str
    status: ExtractorStatus
    last_run: Optional[datetime] = None
    last_success: Optional[datetime] = None
    last_error: Optional[str] = None
    run_count: int = 0
    error_count: int = 0
    success_rate: float = 0.0
    average_duration: float = 0.0
    next_run: Optional[datetime] = None


@dataclass
class ExtractorMetrics:
    """Metrics for an extractor run"""
    start_time: datetime
    end_time: Optional[datetime] = None
    duration: Optional[float] = None
    records_processed: int = 0
    events_created: int = 0
    assets_created: int = 0
    assets_updated: int = 0
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)


class OrchestratorConfig(BaseModel):
    """Configuration for the orchestrator"""
    
    # Extractor intervals (seconds)
    jobs_interval: int = Field(default=300, env="JOBS_EXTRACTION_INTERVAL")
    production_interval: int = Field(default=300, env="PRODUCTION_EXTRACTION_INTERVAL")
    inventory_interval: int = Field(default=600, env="INVENTORY_EXTRACTION_INTERVAL")
    master_data_interval: int = Field(default=3600, env="MASTER_DATA_EXTRACTION_INTERVAL")
    quality_interval: int = Field(default=300, env="QUALITY_EXTRACTION_INTERVAL")
    performance_interval: int = Field(default=900, env="PERFORMANCE_EXTRACTION_INTERVAL")
    
    # Enable/disable extractors
    enable_jobs: bool = Field(default=True, env="ENABLE_JOBS_EXTRACTOR")
    enable_production: bool = Field(default=True, env="ENABLE_PRODUCTION_EXTRACTOR")
    enable_inventory: bool = Field(default=True, env="ENABLE_INVENTORY_EXTRACTOR")
    enable_master_data: bool = Field(default=True, env="ENABLE_MASTER_DATA_EXTRACTOR")
    enable_quality: bool = Field(default=True, env="ENABLE_QUALITY_EXTRACTOR")
    enable_performance: bool = Field(default=False, env="ENABLE_PERFORMANCE_EXTRACTOR")
    
    # Orchestrator settings
    max_concurrent_extractors: int = Field(default=3, env="MAX_CONCURRENT_EXTRACTORS")
    health_check_interval: int = Field(default=60, env="HEALTH_CHECK_INTERVAL")
    metrics_retention_days: int = Field(default=7, env="METRICS_RETENTION_DAYS")
    graceful_shutdown_timeout: int = Field(default=30, env="GRACEFUL_SHUTDOWN_TIMEOUT")
    
    # Retry settings
    max_retries: int = Field(default=3, env="MAX_RETRIES")
    retry_delay: int = Field(default=60, env="RETRY_DELAY")
    
    # Run mode
    run_once: bool = Field(default=False, env="RUN_ONCE")
    dry_run: bool = Field(default=False, env="DRY_RUN")
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


class EnhancedOrchestrator:
    """Enhanced orchestrator for managing all extractors"""
    
    def __init__(self, config: Optional[OrchestratorConfig] = None):
        self.config = config or OrchestratorConfig()
        self.logger = logger.bind(component="orchestrator")
        
        # Extractor states
        self.extractors: Dict[ExtractorType, Any] = {}
        self.health: Dict[ExtractorType, ExtractorHealth] = {}
        self.metrics: Dict[ExtractorType, List[ExtractorMetrics]] = {}
        self.tasks: Dict[ExtractorType, asyncio.Task] = {}
        
        # Control flags
        self.running = False
        self.shutdown_event = asyncio.Event()
        self.semaphore = asyncio.Semaphore(self.config.max_concurrent_extractors)
        
        # Initialize health tracking
        self._initialize_health()
    
    def _initialize_health(self):
        """Initialize health tracking for all extractors"""
        for extractor_type in ExtractorType:
            self.health[extractor_type] = ExtractorHealth(
                name=extractor_type.extractor_name,
                status=ExtractorStatus.IDLE if self._is_enabled(extractor_type) else ExtractorStatus.DISABLED
            )
            self.metrics[extractor_type] = []
    
    def _is_enabled(self, extractor_type: ExtractorType) -> bool:
        """Check if an extractor is enabled"""
        enable_map = {
            ExtractorType.JOBS: self.config.enable_jobs,
            ExtractorType.PRODUCTION: self.config.enable_production,
            ExtractorType.INVENTORY: self.config.enable_inventory,
            ExtractorType.MASTER_DATA: self.config.enable_master_data,
            ExtractorType.QUALITY: self.config.enable_quality,
            ExtractorType.PERFORMANCE: self.config.enable_performance,
        }
        return enable_map.get(extractor_type, False)
    
    def _get_interval(self, extractor_type: ExtractorType) -> int:
        """Get the configured interval for an extractor"""
        interval_map = {
            ExtractorType.JOBS: self.config.jobs_interval,
            ExtractorType.PRODUCTION: self.config.production_interval,
            ExtractorType.INVENTORY: self.config.inventory_interval,
            ExtractorType.MASTER_DATA: self.config.master_data_interval,
            ExtractorType.QUALITY: self.config.quality_interval,
            ExtractorType.PERFORMANCE: self.config.performance_interval,
        }
        return interval_map.get(extractor_type, extractor_type.default_interval)
    
    async def _load_extractor(self, extractor_type: ExtractorType) -> Optional[Any]:
        """Dynamically load an enhanced extractor"""
        try:
            if extractor_type == ExtractorType.JOBS:
                from jobs_extractor_enhanced import EnhancedJobsExtractor, JobsExtractorConfig
                # Try to load from env using from_env if available, otherwise use default
                try:
                    config = JobsExtractorConfig.from_env()
                except:
                    config = JobsExtractorConfig()
                return EnhancedJobsExtractor(config)
            
            elif extractor_type == ExtractorType.PRODUCTION:
                from production_extractor_enhanced import EnhancedProductionExtractor, ProductionExtractorConfig
                try:
                    config = ProductionExtractorConfig.from_env()
                except:
                    config = ProductionExtractorConfig()
                return EnhancedProductionExtractor(config)
            
            elif extractor_type == ExtractorType.INVENTORY:
                from inventory_extractor_enhanced import EnhancedInventoryExtractor, InventoryExtractorConfig
                try:
                    config = InventoryExtractorConfig.from_env()
                except:
                    config = InventoryExtractorConfig()
                return EnhancedInventoryExtractor(config)
            
            elif extractor_type == ExtractorType.MASTER_DATA:
                from master_data_extractor_enhanced import EnhancedMasterDataExtractor, MasterDataExtractorConfig
                try:
                    config = MasterDataExtractorConfig.from_env()
                except:
                    config = MasterDataExtractorConfig()
                return EnhancedMasterDataExtractor(config)
            
            elif extractor_type == ExtractorType.QUALITY:
                from quality_extractor_enhanced import EnhancedQualityExtractor, QualityExtractorConfig
                try:
                    config = QualityExtractorConfig.from_env()
                except:
                    config = QualityExtractorConfig()
                return EnhancedQualityExtractor(config)
            
            elif extractor_type == ExtractorType.PERFORMANCE:
                from performance_extractor_enhanced import EnhancedPerformanceExtractor, PerformanceConfig
                config = PerformanceConfig.from_env()
                return EnhancedPerformanceExtractor(config)
            
            else:
                self.logger.error(f"Unknown extractor type: {extractor_type}")
                return None
                
        except ImportError as e:
            self.logger.warning(
                f"Could not load {extractor_type.extractor_name} extractor",
                error=str(e)
            )
            return None
        except Exception as e:
            self.logger.error(
                f"Error loading {extractor_type.extractor_name} extractor",
                error=str(e),
                traceback=traceback.format_exc()
            )
            return None
    
    async def _run_extractor(self, extractor_type: ExtractorType) -> ExtractorMetrics:
        """Run a single extractor and collect metrics"""
        metrics = ExtractorMetrics(start_time=datetime.now(timezone.utc))
        
        try:
            async with self.semaphore:
                # Update health status
                self.health[extractor_type].status = ExtractorStatus.RUNNING
                self.health[extractor_type].last_run = metrics.start_time
                
                # Load extractor if not already loaded
                if extractor_type not in self.extractors:
                    extractor = await self._load_extractor(extractor_type)
                    if not extractor:
                        raise Exception(f"Failed to load {extractor_type.extractor_name} extractor")
                    self.extractors[extractor_type] = extractor
                
                extractor = self.extractors[extractor_type]
                
                # Run extraction
                self.logger.info(f"Running {extractor_type.extractor_name} extractor")
                
                if self.config.dry_run:
                    self.logger.info(f"DRY RUN: Would run {extractor_type.extractor_name}")
                    await asyncio.sleep(1)  # Simulate work
                    result = {"status": "dry_run"}
                else:
                    result = await extractor.extract()
                
                # Update metrics
                metrics.end_time = datetime.now(timezone.utc)
                metrics.duration = (metrics.end_time - metrics.start_time).total_seconds()
                
                if isinstance(result, dict):
                    metrics.records_processed = result.get("records_processed", 0)
                    metrics.events_created = result.get("events_created", 0)
                    metrics.assets_created = result.get("assets_created", 0)
                    metrics.assets_updated = result.get("assets_updated", 0)
                
                # Update health
                self.health[extractor_type].status = ExtractorStatus.COMPLETED
                self.health[extractor_type].last_success = metrics.end_time
                self.health[extractor_type].run_count += 1
                self._update_success_rate(extractor_type)
                
                self.logger.info(
                    f"Completed {extractor_type.extractor_name} extraction",
                    duration=metrics.duration,
                    records=metrics.records_processed
                )
                
        except Exception as e:
            metrics.end_time = datetime.now(timezone.utc)
            metrics.duration = (metrics.end_time - metrics.start_time).total_seconds()
            metrics.errors.append(str(e))
            
            # Update health
            self.health[extractor_type].status = ExtractorStatus.FAILED
            self.health[extractor_type].last_error = str(e)
            self.health[extractor_type].error_count += 1
            self.health[extractor_type].run_count += 1
            self._update_success_rate(extractor_type)
            
            self.logger.error(
                f"Failed to run {extractor_type.extractor_name} extractor",
                error=str(e),
                traceback=traceback.format_exc()
            )
        
        finally:
            # Store metrics
            self.metrics[extractor_type].append(metrics)
            self._cleanup_old_metrics(extractor_type)
            
            # Update average duration
            self._update_average_duration(extractor_type)
        
        return metrics
    
    def _update_success_rate(self, extractor_type: ExtractorType):
        """Update success rate for an extractor"""
        health = self.health[extractor_type]
        if health.run_count > 0:
            health.success_rate = (health.run_count - health.error_count) / health.run_count
    
    def _update_average_duration(self, extractor_type: ExtractorType):
        """Update average duration for an extractor"""
        recent_metrics = self.metrics[extractor_type][-10:]  # Last 10 runs
        durations = [m.duration for m in recent_metrics if m.duration]
        if durations:
            self.health[extractor_type].average_duration = sum(durations) / len(durations)
    
    def _cleanup_old_metrics(self, extractor_type: ExtractorType):
        """Remove old metrics beyond retention period"""
        cutoff = datetime.now(timezone.utc) - timedelta(days=self.config.metrics_retention_days)
        self.metrics[extractor_type] = [
            m for m in self.metrics[extractor_type]
            if m.start_time > cutoff
        ]
    
    async def _extractor_loop(self, extractor_type: ExtractorType):
        """Main loop for running an extractor periodically"""
        interval = self._get_interval(extractor_type)
        
        while self.running:
            try:
                # Calculate next run time
                next_run = datetime.now(timezone.utc) + timedelta(seconds=interval)
                self.health[extractor_type].next_run = next_run
                
                # Run the extractor
                await self._run_extractor(extractor_type)
                
                if self.config.run_once:
                    break
                
                # Wait for next run or shutdown
                try:
                    await asyncio.wait_for(
                        self.shutdown_event.wait(),
                        timeout=interval
                    )
                    break  # Shutdown requested
                except asyncio.TimeoutError:
                    continue  # Continue to next iteration
                    
            except Exception as e:
                self.logger.error(
                    f"Error in {extractor_type.extractor_name} loop",
                    error=str(e),
                    traceback=traceback.format_exc()
                )
                
                # Wait before retrying
                await asyncio.sleep(self.config.retry_delay)
    
    async def _health_monitor(self):
        """Monitor and report health of all extractors"""
        while self.running:
            try:
                await asyncio.sleep(self.config.health_check_interval)
                
                # Print health status
                self._print_health_status()
                
            except Exception as e:
                self.logger.error(
                    "Error in health monitor",
                    error=str(e)
                )
    
    def _print_health_status(self):
        """Print current health status of all extractors"""
        print("\n" + "="*60)
        print(f"ORCHESTRATOR HEALTH STATUS - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*60)
        
        for extractor_type, health in self.health.items():
            if health.status == ExtractorStatus.DISABLED:
                continue
            
            status_symbol = {
                ExtractorStatus.IDLE: "⏸",
                ExtractorStatus.RUNNING: "▶",
                ExtractorStatus.COMPLETED: "✓",
                ExtractorStatus.FAILED: "✗",
            }.get(health.status, "?")
            
            print(f"\n{status_symbol} {health.name.upper()}")
            print(f"  Status: {health.status.value}")
            print(f"  Runs: {health.run_count} (Success rate: {health.success_rate:.1%})")
            
            if health.last_success:
                time_since = (datetime.now(timezone.utc) - health.last_success).total_seconds()
                print(f"  Last success: {int(time_since)}s ago")
            
            if health.average_duration > 0:
                print(f"  Avg duration: {health.average_duration:.1f}s")
            
            if health.next_run:
                time_until = (health.next_run - datetime.now(timezone.utc)).total_seconds()
                if time_until > 0:
                    print(f"  Next run in: {int(time_until)}s")
            
            if health.last_error:
                print(f"  Last error: {health.last_error[:100]}")
        
        print("\n" + "="*60)
    
    async def start(self):
        """Start the orchestrator"""
        self.logger.info("Starting Enhanced Orchestrator")
        self.running = True
        
        # Start extractor tasks
        for extractor_type in ExtractorType:
            if self._is_enabled(extractor_type):
                self.tasks[extractor_type] = asyncio.create_task(
                    self._extractor_loop(extractor_type)
                )
                self.logger.info(f"Started {extractor_type.extractor_name} extractor")
            else:
                self.logger.info(f"Skipping disabled {extractor_type.extractor_name} extractor")
        
        # Start health monitor
        health_task = asyncio.create_task(self._health_monitor())
        
        # Wait for shutdown or completion
        try:
            if self.config.run_once:
                # Wait for all extractors to complete once
                await asyncio.gather(*self.tasks.values())
            else:
                # Run until shutdown
                await self.shutdown_event.wait()
        finally:
            # Cancel health monitor
            health_task.cancel()
            try:
                await health_task
            except asyncio.CancelledError:
                pass
    
    async def stop(self):
        """Stop the orchestrator gracefully"""
        self.logger.info("Stopping Enhanced Orchestrator")
        self.running = False
        self.shutdown_event.set()
        
        # Wait for tasks to complete with timeout
        if self.tasks:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*self.tasks.values(), return_exceptions=True),
                    timeout=self.config.graceful_shutdown_timeout
                )
            except asyncio.TimeoutError:
                self.logger.warning("Timeout waiting for extractors to stop")
                
                # Cancel remaining tasks
                for task in self.tasks.values():
                    if not task.done():
                        task.cancel()
                
                # Wait for cancellation
                await asyncio.gather(*self.tasks.values(), return_exceptions=True)
        
        # Final health report
        self._print_health_status()
        
        self.logger.info("Enhanced Orchestrator stopped")


async def main():
    """Main entry point"""
    # Load configuration
    config = OrchestratorConfig()
    
    # Create orchestrator
    orchestrator = EnhancedOrchestrator(config)
    
    # Setup signal handlers
    def signal_handler(sig, frame):
        logger.info(f"Received signal {sig}")
        asyncio.create_task(orchestrator.stop())
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Start orchestrator
    try:
        await orchestrator.start()
    except Exception as e:
        logger.error(
            "Fatal error in orchestrator",
            error=str(e),
            traceback=traceback.format_exc()
        )
        sys.exit(1)


if __name__ == "__main__":
    print("="*60)
    print("ENHANCED PLEX-CDF EXTRACTOR ORCHESTRATOR")
    print("="*60)
    print(f"Starting at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("Press Ctrl+C to stop")
    print("="*60)
    
    asyncio.run(main())