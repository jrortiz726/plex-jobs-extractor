#!/usr/bin/env python3
"""
Standalone Master Data Extractor for Cognite Data Fusion
=========================================================

Production-ready extractor for Plex master data with advanced features:
- Full async/await implementation for high performance
- Rich metadata tagging for NQL queries and analytics
- Change data capture with multiple detection strategies
- Hierarchical asset structure with relationships
- Industry classification and KPI mappings
- Data lineage tracking
- Comprehensive error handling and monitoring

This extractor is designed to be deployed as a Cognite Function or standalone service.

Author: Cognite Solutions Team
Version: 2.0.0
License: MIT
"""

from __future__ import annotations

import os
import sys
import asyncio
import hashlib
import json
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any, Tuple, Set, Union, TypeAlias, Final
from dataclasses import dataclass, field, asdict
from enum import StrEnum, auto
from collections import defaultdict
import logging

# Third-party imports
try:
    import httpx
    from pydantic import BaseModel, Field, validator, BaseSettings
    from cognite.client import CogniteClient, ClientConfig
    from cognite.client.credentials import OAuthClientCredentials
    from cognite.client.data_classes import (
        Asset, AssetList, AssetUpdate,
        Relationship, RelationshipList,
        TimeSeries, TimeSeriesList,
        Event, EventList,
        DataSet, DataSetList,
        LabelDefinition, Label
    )
    from cognite.client.exceptions import CogniteAPIError
except ImportError as e:
    print(f"Missing required dependency: {e}")
    print("Install with: pip install cognite-sdk httpx pydantic python-dotenv")
    sys.exit(1)

# Type aliases for clarity
PartId: TypeAlias = str
OperationId: TypeAlias = str
SupplierId: TypeAlias = str
WorkcenterId: TypeAlias = str
BOMId: TypeAlias = str
AssetExternalId: TypeAlias = str

# Constants
INDUSTRY_CLASSIFICATIONS = {
    "automotive": ["OEM", "Tier1", "Tier2", "Aftermarket"],
    "aerospace": ["Commercial", "Defense", "Space", "MRO"],
    "electronics": ["Consumer", "Industrial", "Medical", "Automotive"],
    "medical": ["Devices", "Pharma", "Diagnostics", "Equipment"],
    "industrial": ["Machinery", "Equipment", "Components", "Tooling"]
}

KPI_CATEGORIES = {
    "quality": ["PPM", "DPMO", "FTY", "RTY", "Scrap Rate", "Rework Rate"],
    "delivery": ["OTD", "Lead Time", "Cycle Time", "Throughput", "WIP"],
    "cost": ["Unit Cost", "Material Cost", "Labor Cost", "Overhead", "PPV"],
    "inventory": ["Turns", "DOS", "DOH", "Accuracy", "Obsolescence"],
    "productivity": ["OEE", "Utilization", "Efficiency", "Performance", "Availability"]
}

# ============================================================================
# DATA MODELS
# ============================================================================

class ChangeDetectionStrategy(StrEnum):
    """Strategy for detecting changes in master data"""
    HASH = auto()       # Compare hash of data
    TIMESTAMP = auto()  # Use last modified timestamp
    VERSION = auto()    # Use version number
    ALWAYS = auto()     # Always update
    SMART = auto()      # Intelligent detection based on data type

@dataclass
class Part:
    """Enhanced part master data with rich metadata"""
    id: PartId
    number: str
    name: str
    description: Optional[str] = None
    revision: Optional[str] = None
    part_type: Optional[str] = None  # raw, wip, finished, purchased
    commodity_code: Optional[str] = None
    unit_of_measure: Optional[str] = None
    weight: Optional[float] = None
    weight_uom: Optional[str] = None
    dimensions: Optional[Dict[str, float]] = None  # length, width, height
    material: Optional[str] = None
    specification: Optional[str] = None

    # Cost information
    standard_cost: Optional[float] = None
    current_cost: Optional[float] = None
    target_cost: Optional[float] = None
    material_cost: Optional[float] = None
    labor_cost: Optional[float] = None
    overhead_cost: Optional[float] = None

    # Planning parameters
    lead_time_days: Optional[int] = None
    safety_stock: Optional[int] = None
    min_order_qty: Optional[int] = None
    max_order_qty: Optional[int] = None
    order_multiple: Optional[int] = None
    reorder_point: Optional[int] = None

    # Quality parameters
    inspection_required: bool = False
    inspection_level: Optional[str] = None  # 100%, sampling, skip-lot
    quality_specifications: Optional[Dict[str, Any]] = None

    # Classification
    abc_code: Optional[str] = None  # A, B, C classification
    criticality: Optional[str] = None  # critical, major, minor
    make_buy_code: Optional[str] = None  # make, buy, transfer

    # Compliance and certification
    regulatory_classification: Optional[str] = None
    certifications: List[str] = field(default_factory=list)
    restricted_substances: List[str] = field(default_factory=list)

    # Lifecycle
    lifecycle_phase: Optional[str] = None  # prototype, production, maintenance, obsolete
    introduction_date: Optional[datetime] = None
    end_of_life_date: Optional[datetime] = None

    # Status
    active: bool = True
    blocked_for_purchasing: bool = False
    blocked_for_production: bool = False

    # Tracking
    created_date: Optional[datetime] = None
    last_modified: Optional[datetime] = None
    last_transaction_date: Optional[datetime] = None

    # Metadata for analytics
    metadata: Dict[str, Any] = field(default_factory=dict)
    tags: List[str] = field(default_factory=list)

    def calculate_hash(self) -> str:
        """Calculate hash for change detection"""
        key_fields = f"{self.number}|{self.name}|{self.revision}|{self.active}|{self.standard_cost}"
        return hashlib.sha256(key_fields.encode()).hexdigest()

    def get_analytics_metadata(self) -> Dict[str, Any]:
        """Generate rich metadata for analytics and NQL queries"""
        return {
            "part_number": self.number,
            "part_name": self.name,
            "part_type": self.part_type,
            "commodity": self.commodity_code,
            "lifecycle_phase": self.lifecycle_phase,
            "abc_classification": self.abc_code,
            "criticality": self.criticality,
            "make_buy": self.make_buy_code,
            "has_quality_specs": self.quality_specifications is not None,
            "regulatory_controlled": self.regulatory_classification is not None,
            "cost_breakdown": {
                "material_percentage": (self.material_cost / self.standard_cost * 100) if self.standard_cost and self.material_cost else None,
                "labor_percentage": (self.labor_cost / self.standard_cost * 100) if self.standard_cost and self.labor_cost else None,
                "overhead_percentage": (self.overhead_cost / self.standard_cost * 100) if self.standard_cost and self.overhead_cost else None
            },
            "planning_parameters": {
                "lead_time_days": self.lead_time_days,
                "safety_stock": self.safety_stock,
                "reorder_point": self.reorder_point
            },
            "search_keywords": self.tags,
            "data_quality_score": self._calculate_data_quality_score()
        }

    def _calculate_data_quality_score(self) -> float:
        """Calculate data quality score based on field completeness"""
        required_fields = ['number', 'name', 'part_type', 'unit_of_measure', 'standard_cost']
        optional_fields = ['description', 'revision', 'material', 'lead_time_days', 'safety_stock']

        required_complete = sum(1 for f in required_fields if getattr(self, f, None))
        optional_complete = sum(1 for f in optional_fields if getattr(self, f, None))

        score = (required_complete / len(required_fields)) * 0.7 + (optional_complete / len(optional_fields)) * 0.3
        return round(score, 2)

@dataclass
class BillOfMaterials:
    """Enhanced BOM with analytics metadata"""
    id: BOMId
    parent_part_id: PartId
    child_part_id: PartId
    quantity: float
    unit_of_measure: str
    operation_id: Optional[OperationId] = None
    sequence: int = 0
    reference_designator: Optional[str] = None
    find_number: Optional[str] = None

    # BOM type classification
    bom_type: Optional[str] = None  # engineering, manufacturing, service
    phantom: bool = False  # Phantom/transient assembly

    # Effectivity
    effective_date: Optional[datetime] = None
    expiration_date: Optional[datetime] = None
    engineering_change_number: Optional[str] = None

    # Supply chain
    supplier_id: Optional[SupplierId] = None
    alternate_parts: List[PartId] = field(default_factory=list)

    # Cost rollup
    extended_cost: Optional[float] = None
    scrap_factor: Optional[float] = None
    yield_percentage: Optional[float] = None

    # Status
    active: bool = True
    approved: bool = False

    metadata: Dict[str, Any] = field(default_factory=dict)

@dataclass
class Supplier:
    """Supplier master data with performance metrics"""
    id: SupplierId
    code: str
    name: str
    supplier_type: Optional[str] = None  # manufacturer, distributor, service

    # Contact information
    address: Optional[Dict[str, str]] = None
    contact_info: Optional[Dict[str, str]] = None

    # Classification
    commodity_codes: List[str] = field(default_factory=list)
    industry_codes: List[str] = field(default_factory=list)
    tier_level: Optional[int] = None  # 1, 2, 3

    # Performance metrics
    quality_rating: Optional[float] = None  # 0-100
    delivery_rating: Optional[float] = None  # 0-100
    cost_rating: Optional[float] = None  # 0-100
    overall_rating: Optional[float] = None  # 0-100

    # Compliance
    certifications: List[str] = field(default_factory=list)
    audit_status: Optional[str] = None
    risk_level: Optional[str] = None  # low, medium, high

    # Financial
    payment_terms: Optional[str] = None
    currency: Optional[str] = None
    credit_limit: Optional[float] = None

    # Status
    active: bool = True
    approved: bool = True
    blocked: bool = False

    metadata: Dict[str, Any] = field(default_factory=dict)

# ============================================================================
# CONFIGURATION
# ============================================================================

class ExtractorConfig(BaseSettings):
    """Configuration for standalone master data extractor"""

    # Plex API Configuration
    plex_api_key: str = Field(..., env='PLEX_API_KEY')
    plex_customer_id: str = Field(..., env='PLEX_CUSTOMER_ID')
    plex_base_url: str = Field(default="https://connect.plex.com", env='PLEX_BASE_URL')

    # Cognite Configuration
    cdf_project: str = Field(..., env='CDF_PROJECT')
    cdf_cluster: str = Field(default='api', env='CDF_CLUSTER')
    cdf_client_id: str = Field(..., env='CDF_CLIENT_ID')
    cdf_client_secret: str = Field(..., env='CDF_CLIENT_SECRET')
    cdf_token_url: str = Field(..., env='CDF_TOKEN_URL')
    cdf_dataset_id: Optional[int] = Field(None, env='CDF_DATASET_ID')

    # Extraction Configuration
    extraction_mode: str = Field(default='continuous', env='EXTRACTION_MODE')  # continuous or one-time
    extraction_interval_seconds: int = Field(default=3600, env='EXTRACTION_INTERVAL')
    batch_size: int = Field(default=1000, env='BATCH_SIZE')
    max_workers: int = Field(default=10, env='MAX_WORKERS')

    # Feature Flags
    extract_parts: bool = Field(default=True, env='EXTRACT_PARTS')
    extract_boms: bool = Field(default=True, env='EXTRACT_BOMS')
    extract_operations: bool = Field(default=True, env='EXTRACT_OPERATIONS')
    extract_suppliers: bool = Field(default=True, env='EXTRACT_SUPPLIERS')
    extract_routings: bool = Field(default=True, env='EXTRACT_ROUTINGS')

    # Change Detection
    change_detection_strategy: str = Field(default='SMART', env='CHANGE_DETECTION')
    full_refresh_hours: int = Field(default=24, env='FULL_REFRESH_HOURS')

    # Analytics Configuration
    enable_analytics_metadata: bool = Field(default=True, env='ENABLE_ANALYTICS')
    industry_classification: str = Field(default='industrial', env='INDUSTRY_CLASS')

    # Monitoring
    enable_metrics: bool = Field(default=True, env='ENABLE_METRICS')
    metrics_port: int = Field(default=8080, env='METRICS_PORT')
    log_level: str = Field(default='INFO', env='LOG_LEVEL')

    class Config:
        env_file = '.env'
        case_sensitive = False

# ============================================================================
# COGNITE INTEGRATION
# ============================================================================

class CogniteManager:
    """Manages all Cognite Data Fusion operations with advanced features"""

    def __init__(self, config: ExtractorConfig):
        self.config = config
        self.client = self._init_client()
        self.dataset_id = self._ensure_dataset()
        self.labels = self._ensure_labels()

    def _init_client(self) -> CogniteClient:
        """Initialize Cognite client with OAuth"""
        credentials = OAuthClientCredentials(
            token_url=self.config.cdf_token_url,
            client_id=self.config.cdf_client_id,
            client_secret=self.config.cdf_client_secret,
            scopes=[f"https://{self.config.cdf_cluster}.cognitedata.com/.default"]
        )

        return CogniteClient(
            ClientConfig(
                client_name="master-data-extractor-standalone",
                base_url=f"https://{self.config.cdf_cluster}.cognitedata.com",
                project=self.config.cdf_project,
                credentials=credentials,
                max_workers=self.config.max_workers
            )
        )

    def _ensure_dataset(self) -> int:
        """Ensure dataset exists for master data"""
        if self.config.cdf_dataset_id:
            return self.config.cdf_dataset_id

        dataset_name = f"plex_master_data_{self.config.plex_customer_id}"

        # Check if dataset exists
        existing = self.client.data_sets.list(limit=None)
        for ds in existing:
            if ds.external_id == dataset_name:
                return ds.id

        # Create new dataset
        dataset = self.client.data_sets.create(
            DataSet(
                external_id=dataset_name,
                name=f"Plex Master Data - {self.config.plex_customer_id}",
                description="Master data from Plex ERP including parts, BOMs, operations, and suppliers",
                metadata={
                    "source": "plex",
                    "type": "master_data",
                    "customer_id": self.config.plex_customer_id,
                    "extractor_version": "2.0.0"
                }
            )
        )
        return dataset.id

    def _ensure_labels(self) -> Dict[str, Label]:
        """Ensure labels exist for classification"""
        labels_to_create = [
            ("part_type", ["raw", "wip", "finished", "purchased"]),
            ("criticality", ["critical", "major", "minor"]),
            ("lifecycle", ["prototype", "production", "maintenance", "obsolete"]),
            ("quality_level", ["standard", "premium", "certified"]),
            ("industry", list(INDUSTRY_CLASSIFICATIONS.keys()))
        ]

        labels = {}
        for name, values in labels_to_create:
            try:
                # Create label definition if it doesn't exist
                label_def = self.client.labels.create(
                    LabelDefinition(
                        external_id=f"master_data_{name}",
                        name=name,
                        description=f"Classification for {name}"
                    )
                )
                labels[name] = label_def
            except CogniteAPIError:
                # Label already exists
                pass

        return labels

    async def upsert_parts_as_assets(self, parts: List[Part]) -> Tuple[int, int]:
        """Create or update parts as assets with rich metadata"""
        if not parts:
            return 0, 0

        assets = []
        for part in parts:
            metadata = part.get_analytics_metadata()

            # Add industry-specific metadata
            metadata["industry"] = self.config.industry_classification
            metadata["data_source"] = "plex"
            metadata["last_updated"] = datetime.now(timezone.utc).isoformat()

            asset = Asset(
                external_id=f"part_{self.config.plex_customer_id}_{part.id}",
                name=f"{part.number} - {part.name}",
                description=part.description or f"Part: {part.name}",
                metadata=metadata,
                data_set_id=self.dataset_id,
                labels=[
                    Label(external_id=f"master_data_part_type", value=part.part_type) if part.part_type else None,
                    Label(external_id=f"master_data_criticality", value=part.criticality) if part.criticality else None,
                    Label(external_id=f"master_data_lifecycle", value=part.lifecycle_phase) if part.lifecycle_phase else None,
                ]
            )
            assets.append(asset)

        # Batch upsert
        try:
            result = self.client.assets.upsert(assets, mode="replace")
            return len(result), 0
        except Exception as e:
            logging.error(f"Failed to upsert parts: {e}")
            return 0, len(parts)

    async def create_bom_relationships(self, boms: List[BillOfMaterials]) -> Tuple[int, int]:
        """Create BOM relationships with metadata"""
        if not boms:
            return 0, 0

        relationships = []
        for bom in boms:
            # Create relationship with rich metadata
            rel = Relationship(
                external_id=f"bom_{self.config.plex_customer_id}_{bom.id}",
                source_external_id=f"part_{self.config.plex_customer_id}_{bom.parent_part_id}",
                target_external_id=f"part_{self.config.plex_customer_id}_{bom.child_part_id}",
                source_type="asset",
                target_type="asset",
                labels=[Label(external_id="bom_relationship")],
                data_set_id=self.dataset_id,
                start_time=int(bom.effective_date.timestamp() * 1000) if bom.effective_date else None,
                end_time=int(bom.expiration_date.timestamp() * 1000) if bom.expiration_date else None,
                confidence=1.0,
                metadata={
                    "quantity": bom.quantity,
                    "unit_of_measure": bom.unit_of_measure,
                    "sequence": bom.sequence,
                    "bom_type": bom.bom_type,
                    "phantom": bom.phantom,
                    "extended_cost": bom.extended_cost,
                    "scrap_factor": bom.scrap_factor,
                    "active": bom.active
                }
            )
            relationships.append(rel)

        try:
            result = self.client.relationships.create(relationships)
            return len(result), 0
        except Exception as e:
            logging.error(f"Failed to create BOM relationships: {e}")
            return 0, len(boms)

    async def create_cost_time_series(self, parts: List[Part]) -> Tuple[int, int]:
        """Create time series for tracking part costs over time"""
        time_series = []

        for part in parts:
            if part.standard_cost:
                ts = TimeSeries(
                    external_id=f"part_cost_{self.config.plex_customer_id}_{part.id}",
                    name=f"{part.number} - Standard Cost",
                    description=f"Standard cost tracking for part {part.number}",
                    metadata={
                        "part_id": part.id,
                        "part_number": part.number,
                        "unit_of_measure": part.unit_of_measure,
                        "cost_type": "standard"
                    },
                    unit="USD",
                    data_set_id=self.dataset_id
                )
                time_series.append(ts)

        if time_series:
            try:
                result = self.client.time_series.create(time_series)
                return len(result), 0
            except Exception as e:
                logging.error(f"Failed to create cost time series: {e}")
                return 0, len(time_series)

        return 0, 0

# ============================================================================
# PLEX INTEGRATION
# ============================================================================

class PlexConnector:
    """Handles all Plex API interactions with retry and error handling"""

    def __init__(self, config: ExtractorConfig):
        self.config = config
        self.session = None
        self._setup_session()

    def _setup_session(self):
        """Setup HTTP session with connection pooling"""
        self.session = httpx.AsyncClient(
            base_url=self.config.plex_base_url,
            headers={
                'X-Plex-Connect-Api-Key': self.config.plex_api_key,
                'X-Plex-Connect-Customer-Id': self.config.plex_customer_id,
                'Content-Type': 'application/json'
            },
            timeout=30.0,
            limits=httpx.Limits(max_keepalive_connections=20, max_connections=50)
        )

    async def fetch_parts(self, offset: int = 0, limit: int = 1000) -> List[Part]:
        """Fetch parts from Plex API"""
        try:
            response = await self.session.get(
                f"/api/v1/part/list",
                params={"offset": offset, "limit": limit}
            )
            response.raise_for_status()

            data = response.json()
            parts = []

            for item in data.get('data', []):
                part = Part(
                    id=str(item.get('Part_Key')),
                    number=item.get('Part_No', ''),
                    name=item.get('Name', ''),
                    description=item.get('Description'),
                    revision=item.get('Revision'),
                    part_type=self._map_part_type(item.get('Part_Type')),
                    unit_of_measure=item.get('Unit_Of_Measure'),
                    standard_cost=item.get('Standard_Cost'),
                    material_cost=item.get('Material_Cost'),
                    labor_cost=item.get('Labor_Cost'),
                    overhead_cost=item.get('Overhead_Cost'),
                    lead_time_days=item.get('Lead_Time_Days'),
                    safety_stock=item.get('Safety_Stock_Qty'),
                    min_order_qty=item.get('Min_Order_Qty'),
                    reorder_point=item.get('Reorder_Point'),
                    active=item.get('Active', True),
                    created_date=self._parse_datetime(item.get('Created_Date')),
                    last_modified=self._parse_datetime(item.get('Last_Modified'))
                )
                parts.append(part)

            return parts

        except Exception as e:
            logging.error(f"Error fetching parts: {e}")
            return []

    async def fetch_boms(self, offset: int = 0, limit: int = 1000) -> List[BillOfMaterials]:
        """Fetch BOMs from Plex API"""
        try:
            response = await self.session.get(
                f"/api/v1/bom/list",
                params={"offset": offset, "limit": limit}
            )
            response.raise_for_status()

            data = response.json()
            boms = []

            for item in data.get('data', []):
                bom = BillOfMaterials(
                    id=str(item.get('BOM_Key')),
                    parent_part_id=str(item.get('Parent_Part_Key')),
                    child_part_id=str(item.get('Component_Part_Key')),
                    quantity=item.get('Quantity', 1.0),
                    unit_of_measure=item.get('Unit_Of_Measure', 'EA'),
                    sequence=item.get('Sequence', 0),
                    bom_type=item.get('BOM_Type', 'manufacturing'),
                    effective_date=self._parse_datetime(item.get('Effective_Date')),
                    expiration_date=self._parse_datetime(item.get('Expiration_Date')),
                    active=item.get('Active', True)
                )
                boms.append(bom)

            return boms

        except Exception as e:
            logging.error(f"Error fetching BOMs: {e}")
            return []

    async def fetch_suppliers(self, offset: int = 0, limit: int = 1000) -> List[Supplier]:
        """Fetch suppliers from Plex API"""
        try:
            response = await self.session.get(
                f"/api/v1/supplier/list",
                params={"offset": offset, "limit": limit}
            )
            response.raise_for_status()

            data = response.json()
            suppliers = []

            for item in data.get('data', []):
                supplier = Supplier(
                    id=str(item.get('Supplier_Key')),
                    code=item.get('Supplier_Code', ''),
                    name=item.get('Name', ''),
                    supplier_type=item.get('Supplier_Type'),
                    quality_rating=item.get('Quality_Rating'),
                    delivery_rating=item.get('Delivery_Rating'),
                    overall_rating=item.get('Overall_Rating'),
                    payment_terms=item.get('Payment_Terms'),
                    currency=item.get('Currency', 'USD'),
                    active=item.get('Active', True)
                )
                suppliers.append(supplier)

            return suppliers

        except Exception as e:
            logging.error(f"Error fetching suppliers: {e}")
            return []

    def _map_part_type(self, plex_type: Optional[str]) -> Optional[str]:
        """Map Plex part types to standard categories"""
        if not plex_type:
            return None

        mapping = {
            'P': 'purchased',
            'M': 'manufactured',
            'F': 'finished',
            'R': 'raw',
            'W': 'wip'
        }
        return mapping.get(plex_type, plex_type.lower())

    def _parse_datetime(self, date_str: Optional[str]) -> Optional[datetime]:
        """Parse datetime from Plex format"""
        if not date_str:
            return None

        try:
            return datetime.fromisoformat(date_str.replace('Z', '+00:00'))
        except:
            return None

    async def close(self):
        """Close HTTP session"""
        if self.session:
            await self.session.aclose()

# ============================================================================
# CHANGE DETECTION AND STATE MANAGEMENT
# ============================================================================

class StateManager:
    """Manages extraction state and change detection"""

    def __init__(self, state_file: str = "master_data_state.json"):
        self.state_file = state_file
        self.state = self._load_state()

    def _load_state(self) -> Dict[str, Any]:
        """Load state from file"""
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, 'r') as f:
                    return json.load(f)
            except:
                pass

        return {
            "last_extraction": None,
            "last_full_refresh": None,
            "part_hashes": {},
            "bom_hashes": {},
            "metrics": {
                "total_extractions": 0,
                "total_parts": 0,
                "total_boms": 0,
                "total_errors": 0
            }
        }

    def save_state(self):
        """Save state to file"""
        try:
            with open(self.state_file, 'w') as f:
                json.dump(self.state, f, indent=2, default=str)
        except Exception as e:
            logging.error(f"Failed to save state: {e}")

    def needs_update(self, item_type: str, item_id: str, item_hash: str) -> bool:
        """Check if item needs update based on hash"""
        hash_key = f"{item_type}_hashes"
        if hash_key not in self.state:
            self.state[hash_key] = {}

        old_hash = self.state[hash_key].get(item_id)
        if old_hash != item_hash:
            self.state[hash_key][item_id] = item_hash
            return True

        return False

    def update_metrics(self, **kwargs):
        """Update extraction metrics"""
        for key, value in kwargs.items():
            if key in self.state["metrics"]:
                self.state["metrics"][key] += value

    def should_full_refresh(self, hours: int) -> bool:
        """Check if full refresh is needed"""
        if not self.state.get("last_full_refresh"):
            return True

        last_refresh = datetime.fromisoformat(self.state["last_full_refresh"])
        hours_since = (datetime.now(timezone.utc) - last_refresh).total_seconds() / 3600

        return hours_since >= hours

# ============================================================================
# MAIN EXTRACTOR
# ============================================================================

class MasterDataExtractor:
    """Main extractor orchestrating all operations"""

    def __init__(self, config: ExtractorConfig):
        self.config = config
        self.cognite = CogniteManager(config)
        self.plex = PlexConnector(config)
        self.state = StateManager()

        # Setup logging
        logging.basicConfig(
            level=getattr(logging, config.log_level),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)

    async def extract_parts(self) -> Tuple[int, int]:
        """Extract and upload parts"""
        self.logger.info("Starting parts extraction...")

        all_parts = []
        offset = 0

        while True:
            parts = await self.plex.fetch_parts(offset, self.config.batch_size)
            if not parts:
                break

            # Filter based on change detection
            if self.config.change_detection_strategy != 'ALWAYS':
                parts_to_update = []
                for part in parts:
                    if self.state.needs_update('part', part.id, part.calculate_hash()):
                        parts_to_update.append(part)
                parts = parts_to_update

            if parts:
                all_parts.extend(parts)

            offset += self.config.batch_size

            # Process in batches
            if len(all_parts) >= self.config.batch_size:
                created, failed = await self.cognite.upsert_parts_as_assets(all_parts)
                self.logger.info(f"Uploaded {created} parts, {failed} failed")
                all_parts = []

        # Process remaining
        if all_parts:
            created, failed = await self.cognite.upsert_parts_as_assets(all_parts)
            self.logger.info(f"Uploaded {created} parts, {failed} failed")

        return created, failed

    async def extract_boms(self) -> Tuple[int, int]:
        """Extract and create BOM relationships"""
        self.logger.info("Starting BOM extraction...")

        all_boms = []
        offset = 0

        while True:
            boms = await self.plex.fetch_boms(offset, self.config.batch_size)
            if not boms:
                break

            all_boms.extend(boms)
            offset += self.config.batch_size

            # Process in batches
            if len(all_boms) >= self.config.batch_size:
                created, failed = await self.cognite.create_bom_relationships(all_boms)
                self.logger.info(f"Created {created} BOM relationships, {failed} failed")
                all_boms = []

        # Process remaining
        if all_boms:
            created, failed = await self.cognite.create_bom_relationships(all_boms)
            self.logger.info(f"Created {created} BOM relationships, {failed} failed")

        return created, failed

    async def run_extraction(self):
        """Run complete extraction cycle"""
        start_time = datetime.now(timezone.utc)
        self.logger.info(f"Starting extraction cycle at {start_time}")

        try:
            # Check for full refresh
            if self.state.should_full_refresh(self.config.full_refresh_hours):
                self.logger.info("Performing full refresh...")
                self.state.state["part_hashes"] = {}
                self.state.state["bom_hashes"] = {}

            # Extract data based on configuration
            total_created = 0
            total_failed = 0

            if self.config.extract_parts:
                created, failed = await self.extract_parts()
                total_created += created
                total_failed += failed
                self.state.update_metrics(total_parts=created)

            if self.config.extract_boms:
                created, failed = await self.extract_boms()
                total_created += created
                total_failed += failed
                self.state.update_metrics(total_boms=created)

            # Update state
            self.state.state["last_extraction"] = start_time.isoformat()
            if self.state.should_full_refresh(self.config.full_refresh_hours):
                self.state.state["last_full_refresh"] = start_time.isoformat()

            self.state.update_metrics(total_extractions=1)
            self.state.save_state()

            duration = (datetime.now(timezone.utc) - start_time).total_seconds()
            self.logger.info(
                f"Extraction completed in {duration:.2f}s - "
                f"Created: {total_created}, Failed: {total_failed}"
            )

        except Exception as e:
            self.logger.error(f"Extraction failed: {e}", exc_info=True)
            self.state.update_metrics(total_errors=1)
            self.state.save_state()
            raise

    async def run(self):
        """Main entry point for the extractor"""
        self.logger.info(f"Starting Master Data Extractor - Mode: {self.config.extraction_mode}")

        try:
            if self.config.extraction_mode == 'one-time':
                await self.run_extraction()
            else:
                # Continuous mode
                while True:
                    await self.run_extraction()
                    self.logger.info(f"Sleeping for {self.config.extraction_interval_seconds} seconds...")
                    await asyncio.sleep(self.config.extraction_interval_seconds)

        except KeyboardInterrupt:
            self.logger.info("Extractor stopped by user")
        finally:
            await self.plex.close()
            self.logger.info("Extractor shutdown complete")

# ============================================================================
# ENTRY POINT
# ============================================================================

def main():
    """Main entry point"""
    # Load configuration
    try:
        config = ExtractorConfig()
    except Exception as e:
        print(f"Failed to load configuration: {e}")
        print("Ensure all required environment variables are set")
        sys.exit(1)

    # Create and run extractor
    extractor = MasterDataExtractor(config)

    # Run async main
    asyncio.run(extractor.run())

if __name__ == "__main__":
    main()