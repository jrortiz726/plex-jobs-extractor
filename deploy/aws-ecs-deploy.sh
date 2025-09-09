#!/bin/bash
# Deploy to AWS ECS (Elastic Container Service)

# Variables
CLUSTER_NAME="plex-extractors"
SERVICE_NAME="plex-cdf-extractor"
TASK_FAMILY="plex-extractor-task"
REGION="eu-west-1"
ECR_REPOSITORY="plex-cdf-extractor"

# Load environment variables
if [ -f .env ]; then
    export $(cat .env | grep -v '^#' | xargs)
fi

echo "Creating ECR repository..."
aws ecr create-repository --repository-name $ECR_REPOSITORY --region $REGION

echo "Getting ECR login token..."
aws ecr get-login-password --region $REGION | docker login --username AWS --password-stdin $(aws sts get-caller-identity --query Account --output text).dkr.ecr.$REGION.amazonaws.com

echo "Building and pushing Docker image..."
docker build -t $ECR_REPOSITORY .
docker tag $ECR_REPOSITORY:latest $(aws sts get-caller-identity --query Account --output text).dkr.ecr.$REGION.amazonaws.com/$ECR_REPOSITORY:latest
docker push $(aws sts get-caller-identity --query Account --output text).dkr.ecr.$REGION.amazonaws.com/$ECR_REPOSITORY:latest

echo "Creating ECS cluster..."
aws ecs create-cluster --cluster-name $CLUSTER_NAME --region $REGION

echo "Creating task definition..."
cat > task-definition.json <<EOF
{
  "family": "$TASK_FAMILY",
  "networkMode": "awsvpc",
  "requiresCompatibilities": ["FARGATE"],
  "cpu": "512",
  "memory": "2048",
  "containerDefinitions": [
    {
      "name": "plex-extractor",
      "image": "$(aws sts get-caller-identity --query Account --output text).dkr.ecr.$REGION.amazonaws.com/$ECR_REPOSITORY:latest",
      "essential": true,
      "environment": [
        {"name": "CDF_HOST", "value": "$CDF_HOST"},
        {"name": "CDF_PROJECT", "value": "$CDF_PROJECT"},
        {"name": "PLEX_CUSTOMER_ID", "value": "$PLEX_CUSTOMER_ID"},
        {"name": "FACILITY_NAME", "value": "$FACILITY_NAME"},
        {"name": "LOG_LEVEL", "value": "INFO"}
      ],
      "secrets": [
        {"name": "CDF_CLIENT_ID", "valueFrom": "arn:aws:secretsmanager:$REGION:account:secret:plex/cdf_client_id"},
        {"name": "CDF_CLIENT_SECRET", "valueFrom": "arn:aws:secretsmanager:$REGION:account:secret:plex/cdf_client_secret"},
        {"name": "CDF_TOKEN_URL", "valueFrom": "arn:aws:secretsmanager:$REGION:account:secret:plex/cdf_token_url"},
        {"name": "PLEX_API_KEY", "valueFrom": "arn:aws:secretsmanager:$REGION:account:secret:plex/api_key"}
      ],
      "logConfiguration": {
        "logDriver": "awslogs",
        "options": {
          "awslogs-group": "/ecs/plex-extractor",
          "awslogs-region": "$REGION",
          "awslogs-stream-prefix": "ecs"
        }
      }
    }
  ]
}
EOF

aws ecs register-task-definition --cli-input-json file://task-definition.json --region $REGION

echo "Creating ECS service..."
aws ecs create-service \
    --cluster $CLUSTER_NAME \
    --service-name $SERVICE_NAME \
    --task-definition $TASK_FAMILY \
    --desired-count 1 \
    --launch-type FARGATE \
    --network-configuration "awsvpcConfiguration={subnets=[subnet-xxx],securityGroups=[sg-xxx],assignPublicIp=ENABLED}" \
    --region $REGION

echo "ECS service created successfully!"
echo "View logs in CloudWatch: /ecs/plex-extractor"