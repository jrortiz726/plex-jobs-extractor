#!/bin/bash
# Deploy to Azure Container Instances

# Variables
RESOURCE_GROUP="plex-extractors-rg"
CONTAINER_NAME="plex-cdf-extractor"
LOCATION="westeurope"
IMAGE="plexextractor.azurecr.io/plex-cdf-extractor:latest"
REGISTRY_SERVER="plexextractor.azurecr.io"

# Load environment variables from .env file
if [ -f .env ]; then
    export $(cat .env | grep -v '^#' | xargs)
fi

echo "Creating resource group..."
az group create --name $RESOURCE_GROUP --location $LOCATION

echo "Creating Azure Container Registry..."
az acr create --resource-group $RESOURCE_GROUP \
    --name plexextractor \
    --sku Basic \
    --admin-enabled true

echo "Building and pushing Docker image..."
az acr build --registry plexextractor \
    --image plex-cdf-extractor:latest .

echo "Getting ACR credentials..."
ACR_USERNAME=$(az acr credential show --name plexextractor --query username -o tsv)
ACR_PASSWORD=$(az acr credential show --name plexextractor --query passwords[0].value -o tsv)

echo "Creating container instance..."
az container create \
    --resource-group $RESOURCE_GROUP \
    --name $CONTAINER_NAME \
    --image $IMAGE \
    --cpu 1 \
    --memory 2 \
    --registry-login-server $REGISTRY_SERVER \
    --registry-username $ACR_USERNAME \
    --registry-password $ACR_PASSWORD \
    --restart-policy Always \
    --environment-variables \
        CDF_HOST=$CDF_HOST \
        CDF_PROJECT=$CDF_PROJECT \
        PLEX_CUSTOMER_ID=$PLEX_CUSTOMER_ID \
        FACILITY_NAME="$FACILITY_NAME" \
        LOG_LEVEL=INFO \
    --secure-environment-variables \
        CDF_CLIENT_ID=$CDF_CLIENT_ID \
        CDF_CLIENT_SECRET=$CDF_CLIENT_SECRET \
        CDF_TOKEN_URL=$CDF_TOKEN_URL \
        PLEX_API_KEY=$PLEX_API_KEY \
    --location $LOCATION

echo "Container instance created successfully!"
echo "View logs with: az container logs --resource-group $RESOURCE_GROUP --name $CONTAINER_NAME"
echo "View status with: az container show --resource-group $RESOURCE_GROUP --name $CONTAINER_NAME --query instanceView.state"