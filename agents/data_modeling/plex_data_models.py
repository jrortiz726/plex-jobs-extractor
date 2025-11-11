"""Create CDF Data Modeling resources for Plex RAW datasets."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Sequence

from cognite.client import CogniteClient
from cognite.client.config import ClientConfig
from cognite.client.credentials import OAuthClientCredentials
from cognite.client.data_classes.data_modeling.containers import (
    BTreeIndex,
    ContainerApply,
    ContainerProperty,
)
from cognite.client.data_classes.data_modeling.data_models import DataModelApply
from cognite.client.data_classes.data_modeling.data_types import DirectRelation, Float64, Text, Timestamp
from cognite.client.data_classes.data_modeling.ids import ViewId
from cognite.client.data_classes.data_modeling.spaces import SpaceApply
from cognite.client.data_classes.data_modeling.views import MappedPropertyApply, ViewApply

try:
    from dotenv import load_dotenv  # type: ignore
except ImportError:  # pragma: no cover - optional dependency
    load_dotenv = None


@dataclass(frozen=True)
class ModelContext:
    space: str
    version: str


def _model_context() -> ModelContext:
    space = os.getenv("PLEX_DM_SPACE", "plex_models")
    version = os.getenv("PLEX_DM_VERSION", "v1")
    return ModelContext(space=space, version=version)


def apply_plex_data_model(client: CogniteClient) -> None:
    """Apply spaces, containers, views, and data model definitions."""

    ctx = _model_context()
    _ensure_space(client, ctx)

    containers = _build_containers(ctx)
    client.data_modeling.containers.apply(containers)

    views = _build_views(ctx, containers)
    client.data_modeling.views.apply(views)

    data_models = _build_data_models(ctx, views)
    client.data_modeling.data_models.apply(data_models)


def _ensure_space(client: CogniteClient, ctx: ModelContext) -> None:
    client.data_modeling.spaces.apply(
        SpaceApply(
            space=ctx.space,
            name="Plex Data Models",
            description="Logical data models sourced from Plex RAW tables.",
        )
    )


def _build_containers(ctx: ModelContext) -> Sequence[ContainerApply]:
    jobs = ContainerApply(
        space=ctx.space,
        external_id="jobs_container",
        name="Plex Jobs",
        properties={
            "rowKey": ContainerProperty(type=Text(), nullable=False, description="PCN-prefixed job identifier"),
            "jobNumber": ContainerProperty(type=Text()),
            "status": ContainerProperty(type=Text()),
            "workcenter": ContainerProperty(type=Text()),
            "scheduledStart": ContainerProperty(type=Timestamp()),
            "scheduledEnd": ContainerProperty(type=Timestamp()),
            "quantity": ContainerProperty(type=Float64()),
            "pcn": ContainerProperty(type=Text()),
        },
        indexes={
            "row_key_idx": BTreeIndex(properties=["rowKey"], cursorable=True)
        },
    )

    operations = ContainerApply(
        space=ctx.space,
        external_id="operations_container",
        name="Job Operations",
        properties={
            "rowKey": ContainerProperty(type=Text(), nullable=False),
            "jobExternalId": ContainerProperty(type=Text(), nullable=False),
            "job": ContainerProperty(type=DirectRelation(container=jobs.as_id()), nullable=True),
            "operationId": ContainerProperty(type=Text()),
            "operationCode": ContainerProperty(type=Text()),
            "operationNumber": ContainerProperty(type=Float64()),
            "workcenterId": ContainerProperty(type=Text()),
            "workcenterCode": ContainerProperty(type=Text()),
            "quantity": ContainerProperty(type=Float64()),
            "quantityCompleted": ContainerProperty(type=Float64()),
            "scheduledStartDate": ContainerProperty(type=Timestamp()),
            "dueDate": ContainerProperty(type=Timestamp()),
            "completedDate": ContainerProperty(type=Timestamp()),
            "status": ContainerProperty(type=Text()),
            "unitOfMeasure": ContainerProperty(type=Text()),
            "runTime": ContainerProperty(type=Float64()),
            "setupTime": ContainerProperty(type=Float64()),
            "partOperationType": ContainerProperty(type=Text()),
            "batchOperation": ContainerProperty(type=Text()),
            "pcn": ContainerProperty(type=Text()),
        },
        indexes={
            "row_key_idx": BTreeIndex(properties=["rowKey"], cursorable=True)
        },
    )

    production = ContainerApply(
        space=ctx.space,
        external_id="production_entries_container",
        name="Production Entries",
        properties={
            "rowKey": ContainerProperty(type=Text(), nullable=False),
            "workcenterId": ContainerProperty(type=Text()),
            "partId": ContainerProperty(type=Text()),
            "quantityProduced": ContainerProperty(type=Float64()),
            "quantityScrapped": ContainerProperty(type=Float64()),
            "timestamp": ContainerProperty(type=Timestamp()),
            "pcn": ContainerProperty(type=Text()),
            "jobId": ContainerProperty(type=Text()),
            "jobExternalId": ContainerProperty(type=Text()),
            "jobNumber": ContainerProperty(type=Text()),
            "workcenterCode": ContainerProperty(type=Text()),
            "workcenterName": ContainerProperty(type=Text()),
            "status": ContainerProperty(type=Text()),
            "startTime": ContainerProperty(type=Timestamp()),
            "endTime": ContainerProperty(type=Timestamp()),
            "createdAt": ContainerProperty(type=Timestamp()),
            "completedAt": ContainerProperty(type=Timestamp()),
            "shiftId": ContainerProperty(type=Text()),
            "operatorId": ContainerProperty(type=Text()),
            "productionLineId": ContainerProperty(type=Text()),
            "sequenceNumber": ContainerProperty(type=Float64()),
            "quantityGood": ContainerProperty(type=Float64()),
            "quantityRejected": ContainerProperty(type=Float64()),
        },
        indexes={
            "row_key_idx": BTreeIndex(properties=["rowKey"], cursorable=True)
        },
    )

    inventory = ContainerApply(
        space=ctx.space,
        external_id="inventory_container",
        name="Inventory Containers",
        properties={
            "rowKey": ContainerProperty(type=Text(), nullable=False),
            "partNumber": ContainerProperty(type=Text()),
            "locationId": ContainerProperty(type=Text()),
            "location": ContainerProperty(type=Text()),
            "serialNo": ContainerProperty(type=Text()),
            "inventoryType": ContainerProperty(type=Text()),
            "quantity": ContainerProperty(type=Float64()),
            "quantityInventoryUnit": ContainerProperty(type=Text()),
            "netWeight": ContainerProperty(type=Float64()),
            "grossWeight": ContainerProperty(type=Float64()),
            "tareWeight": ContainerProperty(type=Float64()),
            "partId": ContainerProperty(type=Text()),
            "partNo": ContainerProperty(type=Float64()),
            "partNoRevision": ContainerProperty(type=Text()),
            "partName": ContainerProperty(type=Text()),
            "revision": ContainerProperty(type=Float64()),
            "operationCode": ContainerProperty(type=Text()),
            "operationNo": ContainerProperty(type=Float64()),
            "partOperationId": ContainerProperty(type=Text()),
            "lotNo": ContainerProperty(type=Text()),
            "lotId": ContainerProperty(type=Text()),
            "containerStatus": ContainerProperty(type=Text()),
            "containerType": ContainerProperty(type=Text()),
            "addDateTime": ContainerProperty(type=Timestamp()),
            "updateDateTime": ContainerProperty(type=Timestamp()),
            "containerShelfDateTime": ContainerProperty(type=Timestamp()),
            "heatId": ContainerProperty(type=Text()),
            "heatNo": ContainerProperty(type=Text()),
            "heatCode": ContainerProperty(type=Text()),
            "trackingNo": ContainerProperty(type=Text()),
            "masterUnitId": ContainerProperty(type=Text()),
            "masterUnitNo": ContainerProperty(type=Text()),
            "facility": ContainerProperty(type=Text()),
            "pcn": ContainerProperty(type=Text()),
        },
        indexes={
            "row_key_idx": BTreeIndex(properties=["rowKey"], cursorable=True)
        },
    )

    quality = ContainerApply(
        space=ctx.space,
        external_id="quality_container",
        name="Quality Records",
        properties={
            "rowKey": ContainerProperty(type=Text(), nullable=False),
            "recordType": ContainerProperty(type=Text()),
            "status": ContainerProperty(type=Text()),
            "jobNumber": ContainerProperty(type=Text()),
            "timestamp": ContainerProperty(type=Timestamp()),
            "pcn": ContainerProperty(type=Text()),
            "dataSourceId": ContainerProperty(type=Text()),
            "dataSourceName": ContainerProperty(type=Text()),
            "transactionNo": ContainerProperty(type=Text()),
            "tableIndex": ContainerProperty(type=Float64()),
            "rowIndex": ContainerProperty(type=Float64()),
            "facility": ContainerProperty(type=Text()),
            "inputs": ContainerProperty(type=Text()),
            "description": ContainerProperty(type=Text()),
            "samplePlan": ContainerProperty(type=Text()),
            "variableSampleSize": ContainerProperty(type=Text()),
            "controlSampleSize": ContainerProperty(type=Float64()),
            "samplePlanKey": ContainerProperty(type=Float64()),
            "sampleSize": ContainerProperty(type=Float64()),
            "specificationType": ContainerProperty(type=Text()),
            "specificationNo": ContainerProperty(type=Float64()),
            "standardSpecification": ContainerProperty(type=Text()),
            "partKey": ContainerProperty(type=Float64()),
            "specificationKey": ContainerProperty(type=Float64()),
            "specTypeCode": ContainerProperty(type=Text()),
            "partNo": ContainerProperty(type=Float64()),
            "revision": ContainerProperty(type=Float64()),
            "inspectionModeDescription": ContainerProperty(type=Text()),
            "checksheetNo": ContainerProperty(type=Float64()),
            "operationCode": ContainerProperty(type=Text()),
            "inspectorFirstName": ContainerProperty(type=Text()),
            "inspectorLastName": ContainerProperty(type=Text()),
            "workcenterCode": ContainerProperty(type=Text()),
            "outOfSpec": ContainerProperty(type=Float64()),
            "image": ContainerProperty(type=Text()),
            "hasNullMeasurements": ContainerProperty(type=Float64()),
            "measurementResultsXml": ContainerProperty(type=Text()),
        },
    )

    performance = ContainerApply(
        space=ctx.space,
        external_id="performance_container",
        name="Performance Summaries",
        properties={
            "rowKey": ContainerProperty(type=Text(), nullable=False),
            "workcenterId": ContainerProperty(type=Text()),
            "oee": ContainerProperty(type=Float64()),
            "availability": ContainerProperty(type=Float64()),
            "performance": ContainerProperty(type=Float64()),
            "quality": ContainerProperty(type=Float64()),
            "timestamp": ContainerProperty(type=Timestamp()),
            "pcn": ContainerProperty(type=Text()),
            "recordType": ContainerProperty(type=Text()),
            "startTime": ContainerProperty(type=Timestamp()),
            "endTime": ContainerProperty(type=Timestamp()),
            "goodQuantity": ContainerProperty(type=Float64()),
            "badQuantity": ContainerProperty(type=Float64()),
            "totalQuantity": ContainerProperty(type=Float64()),
            "runTimeHours": ContainerProperty(type=Float64()),
            "plannedRunTimeHours": ContainerProperty(type=Float64()),
            "downtimeHours": ContainerProperty(type=Float64()),
            "workcenterCode": ContainerProperty(type=Text()),
        },
    )

    master = ContainerApply(
        space=ctx.space,
        external_id="master_container",
        name="Master Data",
        properties={
            "rowKey": ContainerProperty(type=Text(), nullable=False),
            "recordType": ContainerProperty(type=Text()),
            "name": ContainerProperty(type=Text()),
            "description": ContainerProperty(type=Text()),
            "pcn": ContainerProperty(type=Text()),
            "inventoryType": ContainerProperty(type=Text()),
            "code": ContainerProperty(type=Text()),
            "type": ContainerProperty(type=Text()),
            "facility": ContainerProperty(type=Text()),
            "id": ContainerProperty(type=Text()),
            "createdById": ContainerProperty(type=Text()),
            "source": ContainerProperty(type=Text()),
            "modifiedDate": ContainerProperty(type=Timestamp()),
            "modifiedById": ContainerProperty(type=Text()),
            "buildingCode": ContainerProperty(type=Text()),
            "productType": ContainerProperty(type=Text()),
            "revision": ContainerProperty(type=Float64()),
            "note": ContainerProperty(type=Text()),
            "status": ContainerProperty(type=Text()),
            "leadTimeDays": ContainerProperty(type=Float64()),
            "group": ContainerProperty(type=Text()),
            "createdDate": ContainerProperty(type=Timestamp()),
            "number": ContainerProperty(type=Float64()),
            "allergens": ContainerProperty(type=Text()),
            "workcenterType": ContainerProperty(type=Text()),
            "workcenterId": ContainerProperty(type=Text()),
            "workcenterCode": ContainerProperty(type=Text()),
            "buildingId": ContainerProperty(type=Text()),
            "plcName": ContainerProperty(type=Text()),
            "ipAddress": ContainerProperty(type=Text()),
            "productionLineId": ContainerProperty(type=Text()),
            "workcenterGroup": ContainerProperty(type=Text()),
            "tankSilo": ContainerProperty(type=Text()),
        },
    )

    return (jobs, operations, production, inventory, quality, performance, master)


def _build_views(ctx: ModelContext, containers: Sequence[ContainerApply]) -> Sequence[ViewApply]:
    container_map = {container.external_id: container for container in containers}

    def map_prop(
        container_key: str,
        prop: str,
        *,
        name: str | None = None,
        description: str | None = None,
    ) -> MappedPropertyApply:
        container = container_map[container_key]
        return MappedPropertyApply(
            container=container.as_id(),
            container_property_identifier=prop,
            name=name,
            description=description,
        )

    jobs_view = ViewApply(
        space=ctx.space,
        external_id="jobs_view",
        version=ctx.version,
        name="Jobs",
        properties={
            "rowKey": map_prop("jobs_container", "rowKey", name="externalId"),
            "jobNumber": map_prop("jobs_container", "jobNumber"),
            "status": map_prop("jobs_container", "status"),
            "scheduledStart": map_prop("jobs_container", "scheduledStart"),
            "scheduledEnd": map_prop("jobs_container", "scheduledEnd"),
            "workcenter": map_prop("jobs_container", "workcenter"),
            "quantity": map_prop("jobs_container", "quantity"),
            "pcn": map_prop("jobs_container", "pcn"),
        },
    )

    operations_view = ViewApply(
        space=ctx.space,
        external_id="operations_view",
        version=ctx.version,
        name="Job Operations",
        properties={
            "rowKey": map_prop("operations_container", "rowKey", name="externalId"),
            "jobExternalId": map_prop("operations_container", "jobExternalId"),
            "job": map_prop("operations_container", "job"),
            "operationId": map_prop("operations_container", "operationId"),
            "operationCode": map_prop("operations_container", "operationCode"),
            "operationNumber": map_prop("operations_container", "operationNumber"),
            "workcenterId": map_prop("operations_container", "workcenterId"),
            "workcenterCode": map_prop("operations_container", "workcenterCode"),
            "quantity": map_prop("operations_container", "quantity"),
            "quantityCompleted": map_prop("operations_container", "quantityCompleted"),
            "scheduledStartDate": map_prop("operations_container", "scheduledStartDate"),
            "dueDate": map_prop("operations_container", "dueDate"),
            "completedDate": map_prop("operations_container", "completedDate"),
            "status": map_prop("operations_container", "status"),
            "unitOfMeasure": map_prop("operations_container", "unitOfMeasure"),
            "runTime": map_prop("operations_container", "runTime"),
            "setupTime": map_prop("operations_container", "setupTime"),
            "partOperationType": map_prop("operations_container", "partOperationType"),
            "batchOperation": map_prop("operations_container", "batchOperation"),
            "pcn": map_prop("operations_container", "pcn"),
        },
    )

    production_view = ViewApply(
        space=ctx.space,
        external_id="production_entries_view",
        version=ctx.version,
        name="Production Entries",
        properties={
            "rowKey": map_prop("production_entries_container", "rowKey", name="externalId"),
            "workcenterId": map_prop("production_entries_container", "workcenterId"),
            "partId": map_prop("production_entries_container", "partId"),
            "quantityProduced": map_prop("production_entries_container", "quantityProduced"),
            "quantityScrapped": map_prop("production_entries_container", "quantityScrapped"),
            "timestamp": map_prop("production_entries_container", "timestamp"),
            "pcn": map_prop("production_entries_container", "pcn"),
            "jobId": map_prop("production_entries_container", "jobId"),
            "jobExternalId": map_prop("production_entries_container", "jobExternalId"),
            "jobNumber": map_prop("production_entries_container", "jobNumber"),
            "workcenterCode": map_prop("production_entries_container", "workcenterCode"),
            "workcenterName": map_prop("production_entries_container", "workcenterName"),
            "status": map_prop("production_entries_container", "status"),
            "startTime": map_prop("production_entries_container", "startTime"),
            "endTime": map_prop("production_entries_container", "endTime"),
            "createdAt": map_prop("production_entries_container", "createdAt"),
            "completedAt": map_prop("production_entries_container", "completedAt"),
            "shiftId": map_prop("production_entries_container", "shiftId"),
            "operatorId": map_prop("production_entries_container", "operatorId"),
            "productionLineId": map_prop("production_entries_container", "productionLineId"),
            "sequenceNumber": map_prop("production_entries_container", "sequenceNumber"),
            "quantityGood": map_prop("production_entries_container", "quantityGood"),
            "quantityRejected": map_prop("production_entries_container", "quantityRejected"),
        },
    )

    inventory_view = ViewApply(
        space=ctx.space,
        external_id="inventory_view",
        version=ctx.version,
        name="Inventory Containers",
        properties={
            "rowKey": map_prop("inventory_container", "rowKey", name="externalId"),
            "partNumber": map_prop("inventory_container", "partNumber"),
            "locationId": map_prop("inventory_container", "locationId"),
            "location": map_prop("inventory_container", "location"),
            "serialNo": map_prop("inventory_container", "serialNo"),
            "inventoryType": map_prop("inventory_container", "inventoryType"),
            "quantity": map_prop("inventory_container", "quantity"),
            "quantityInventoryUnit": map_prop("inventory_container", "quantityInventoryUnit"),
            "netWeight": map_prop("inventory_container", "netWeight"),
            "grossWeight": map_prop("inventory_container", "grossWeight"),
            "tareWeight": map_prop("inventory_container", "tareWeight"),
            "partId": map_prop("inventory_container", "partId"),
            "partNo": map_prop("inventory_container", "partNo"),
            "partNoRevision": map_prop("inventory_container", "partNoRevision"),
            "partName": map_prop("inventory_container", "partName"),
            "revision": map_prop("inventory_container", "revision"),
            "operationCode": map_prop("inventory_container", "operationCode"),
            "operationNo": map_prop("inventory_container", "operationNo"),
            "partOperationId": map_prop("inventory_container", "partOperationId"),
            "lotNo": map_prop("inventory_container", "lotNo"),
            "lotId": map_prop("inventory_container", "lotId"),
            "containerStatus": map_prop("inventory_container", "containerStatus"),
            "containerType": map_prop("inventory_container", "containerType"),
            "addDateTime": map_prop("inventory_container", "addDateTime"),
            "updateDateTime": map_prop("inventory_container", "updateDateTime"),
            "containerShelfDateTime": map_prop("inventory_container", "containerShelfDateTime"),
            "heatId": map_prop("inventory_container", "heatId"),
            "heatNo": map_prop("inventory_container", "heatNo"),
            "heatCode": map_prop("inventory_container", "heatCode"),
            "trackingNo": map_prop("inventory_container", "trackingNo"),
            "masterUnitId": map_prop("inventory_container", "masterUnitId"),
            "masterUnitNo": map_prop("inventory_container", "masterUnitNo"),
            "pcn": map_prop("inventory_container", "pcn"),
        },
    )

    quality_view = ViewApply(
        space=ctx.space,
        external_id="quality_view",
        version=ctx.version,
        name="Quality Records",
        properties={
            "rowKey": map_prop("quality_container", "rowKey", name="externalId"),
            "recordType": map_prop("quality_container", "recordType"),
            "status": map_prop("quality_container", "status"),
            "jobNumber": map_prop("quality_container", "jobNumber"),
            "timestamp": map_prop("quality_container", "timestamp"),
            "pcn": map_prop("quality_container", "pcn"),
            "dataSourceId": map_prop("quality_container", "dataSourceId"),
            "dataSourceName": map_prop("quality_container", "dataSourceName"),
            "transactionNo": map_prop("quality_container", "transactionNo"),
            "tableIndex": map_prop("quality_container", "tableIndex"),
            "rowIndex": map_prop("quality_container", "rowIndex"),
            "facility": map_prop("quality_container", "facility"),
            "inputs": map_prop("quality_container", "inputs"),
            "description": map_prop("quality_container", "description"),
            "samplePlan": map_prop("quality_container", "samplePlan"),
            "variableSampleSize": map_prop("quality_container", "variableSampleSize"),
            "controlSampleSize": map_prop("quality_container", "controlSampleSize"),
            "samplePlanKey": map_prop("quality_container", "samplePlanKey"),
            "sampleSize": map_prop("quality_container", "sampleSize"),
            "specificationType": map_prop("quality_container", "specificationType"),
            "specificationNo": map_prop("quality_container", "specificationNo"),
            "standardSpecification": map_prop("quality_container", "standardSpecification"),
            "partKey": map_prop("quality_container", "partKey"),
            "specificationKey": map_prop("quality_container", "specificationKey"),
            "specTypeCode": map_prop("quality_container", "specTypeCode"),
            "partNo": map_prop("quality_container", "partNo"),
            "revision": map_prop("quality_container", "revision"),
            "inspectionModeDescription": map_prop("quality_container", "inspectionModeDescription"),
            "checksheetNo": map_prop("quality_container", "checksheetNo"),
            "operationCode": map_prop("quality_container", "operationCode"),
            "inspectorFirstName": map_prop("quality_container", "inspectorFirstName"),
            "inspectorLastName": map_prop("quality_container", "inspectorLastName"),
            "workcenterCode": map_prop("quality_container", "workcenterCode"),
            "outOfSpec": map_prop("quality_container", "outOfSpec"),
            "image": map_prop("quality_container", "image"),
            "hasNullMeasurements": map_prop("quality_container", "hasNullMeasurements"),
            "measurementResultsXml": map_prop("quality_container", "measurementResultsXml"),
        },
    )

    performance_view = ViewApply(
        space=ctx.space,
        external_id="performance_view",
        version=ctx.version,
        name="Performance Summaries",
        properties={
            "rowKey": map_prop("performance_container", "rowKey", name="externalId"),
            "workcenterId": map_prop("performance_container", "workcenterId"),
            "oee": map_prop("performance_container", "oee"),
            "availability": map_prop("performance_container", "availability"),
            "performance": map_prop("performance_container", "performance"),
            "quality": map_prop("performance_container", "quality"),
            "timestamp": map_prop("performance_container", "timestamp"),
            "pcn": map_prop("performance_container", "pcn"),
            "recordType": map_prop("performance_container", "recordType"),
            "startTime": map_prop("performance_container", "startTime"),
            "endTime": map_prop("performance_container", "endTime"),
            "goodQuantity": map_prop("performance_container", "goodQuantity"),
            "badQuantity": map_prop("performance_container", "badQuantity"),
            "totalQuantity": map_prop("performance_container", "totalQuantity"),
            "runTimeHours": map_prop("performance_container", "runTimeHours"),
            "plannedRunTimeHours": map_prop("performance_container", "plannedRunTimeHours"),
            "downtimeHours": map_prop("performance_container", "downtimeHours"),
            "workcenterCode": map_prop("performance_container", "workcenterCode"),
        },
    )

    master_view = ViewApply(
        space=ctx.space,
        external_id="master_view",
        version=ctx.version,
        name="Master Data",
        properties={
            "rowKey": map_prop("master_container", "rowKey", name="externalId"),
            "recordType": map_prop("master_container", "recordType"),
            "name": map_prop("master_container", "name"),
            "description": map_prop("master_container", "description"),
            "pcn": map_prop("master_container", "pcn"),
            "inventoryType": map_prop("master_container", "inventoryType"),
            "code": map_prop("master_container", "code"),
            "type": map_prop("master_container", "type"),
            "facility": map_prop("master_container", "facility"),
            "id": map_prop("master_container", "id"),
            "createdById": map_prop("master_container", "createdById"),
            "source": map_prop("master_container", "source"),
            "modifiedDate": map_prop("master_container", "modifiedDate"),
            "modifiedById": map_prop("master_container", "modifiedById"),
            "buildingCode": map_prop("master_container", "buildingCode"),
            "productType": map_prop("master_container", "productType"),
            "revision": map_prop("master_container", "revision"),
            "note": map_prop("master_container", "note"),
            "status": map_prop("master_container", "status"),
            "leadTimeDays": map_prop("master_container", "leadTimeDays"),
            "group": map_prop("master_container", "group"),
            "createdDate": map_prop("master_container", "createdDate"),
            "number": map_prop("master_container", "number"),
            "allergens": map_prop("master_container", "allergens"),
            "workcenterType": map_prop("master_container", "workcenterType"),
            "workcenterId": map_prop("master_container", "workcenterId"),
            "workcenterCode": map_prop("master_container", "workcenterCode"),
            "buildingId": map_prop("master_container", "buildingId"),
            "plcName": map_prop("master_container", "plcName"),
            "ipAddress": map_prop("master_container", "ipAddress"),
            "productionLineId": map_prop("master_container", "productionLineId"),
            "workcenterGroup": map_prop("master_container", "workcenterGroup"),
            "tankSilo": map_prop("master_container", "tankSilo"),
        },
    )

    return (
        jobs_view,
        operations_view,
        production_view,
        inventory_view,
        quality_view,
        performance_view,
        master_view,
    )


def _build_data_models(ctx: ModelContext, views: Sequence[ViewApply]) -> Sequence[DataModelApply]:
    view_ids = [view.as_id() for view in views]
    operational_model = DataModelApply(
        space=ctx.space,
        external_id="plex_operational_model",
        version=ctx.version,
        name="Plex Operational Model",
        description="Normalized operational entities for Plex extractors.",
        views=view_ids,
    )
    return (operational_model,)


def apply_all(client: CogniteClient) -> None:
    """Convenience entry point for scripts/CLI."""
    apply_plex_data_model(client)


__all__ = ["apply_plex_data_model", "apply_all"]


def main() -> None:
    """CLI entry point to apply the Plex data model using env credentials."""

    client = _build_client_from_env()
    ctx = _model_context()
    apply_all(client)
    print(
        "Applied Plex data model in space '{}' with version '{}'".format(
            ctx.space, ctx.version
        )
    )


def _build_client_from_env() -> CogniteClient:
    _load_env_file()

    required = [
        "CDF_HOST",
        "CDF_PROJECT",
        "CDF_CLIENT_ID",
        "CDF_CLIENT_SECRET",
        "CDF_TOKEN_URL",
    ]
    missing = [key for key in required if not os.getenv(key)]
    if missing:
        raise SystemExit(f"Missing required environment variables: {missing}")

    credentials = OAuthClientCredentials(
        token_url=os.environ["CDF_TOKEN_URL"],
        client_id=os.environ["CDF_CLIENT_ID"],
        client_secret=os.environ["CDF_CLIENT_SECRET"],
        scopes=["user_impersonation"],
    )

    config = ClientConfig(
        client_name="plex-data-model-bootstrap",
        base_url=os.environ["CDF_HOST"],
        project=os.environ["CDF_PROJECT"],
        credentials=credentials,
    )
    return CogniteClient(config)


def _load_env_file() -> None:
    env_path = Path(".env")
    if load_dotenv is not None:
        load_dotenv(dotenv_path=env_path if env_path.exists() else None)
        return

    if not env_path.exists():
        return

    for line in env_path.read_text().splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())


if __name__ == "__main__":
    main()
