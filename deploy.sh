#!/bin/bash
set -e

# Configuración
STACK_NAME="scraper-fiscalia"
REGION="us-east-1"  # Cambiar según la región deseada
ECR_REPO_NAME="scraper-fiscalia"
IMAGE_TAG="latest"
AWS_PROFILE="16989303_BECH_PermiteCreacionRolesIAM"  # Perfil SSO configurado

# Colores para mensajes
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}=== Iniciando despliegue del Scraper Fiscalía ===${NC}"

# 1. Construir la imagen Docker
echo -e "${YELLOW}Construyendo imagen Docker...${NC}"
docker build -t ${ECR_REPO_NAME}:${IMAGE_TAG} -f .lambdacontainer/Dockerfile .

# 2. Crear repositorio ECR si no existe
echo -e "${YELLOW}Verificando repositorio ECR...${NC}"
aws ecr describe-repositories --repository-names ${ECR_REPO_NAME} --region ${REGION} --profile ${AWS_PROFILE} || \
    aws ecr create-repository --repository-name ${ECR_REPO_NAME} --region ${REGION} --profile ${AWS_PROFILE}

# 3. Autenticar Docker con ECR
echo -e "${YELLOW}Autenticando Docker con ECR...${NC}"
aws ecr get-login-password --region ${REGION} --profile ${AWS_PROFILE} | docker login --username AWS --password-stdin $(aws sts get-caller-identity --query Account --output text --profile ${AWS_PROFILE}).dkr.ecr.${REGION}.amazonaws.com

# 4. Etiquetar y subir la imagen a ECR
echo -e "${YELLOW}Subiendo imagen a ECR...${NC}"
ECR_URI=$(aws ecr describe-repositories --repository-names ${ECR_REPO_NAME} --region ${REGION} --profile ${AWS_PROFILE} --query "repositories[0].repositoryUri" --output text)
docker tag ${ECR_REPO_NAME}:${IMAGE_TAG} ${ECR_URI}:${IMAGE_TAG}
docker push ${ECR_URI}:${IMAGE_TAG}

# 5. Desplegar la plantilla CloudFormation
echo -e "${YELLOW}Desplegando infraestructura con CloudFormation...${NC}"
aws cloudformation deploy \
    --template-file template.yaml \
    --stack-name ${STACK_NAME} \
    --capabilities CAPABILITY_IAM \
    --profile ${AWS_PROFILE} \
    --parameter-overrides \
        ECRRepositoryUri=${ECR_URI}:${IMAGE_TAG} \
        ScheduleExpression="cron(0 1 * * ? *)" \
        S3BucketName="v1-st-bech-inegocio-analitica-sandbox-zone"

echo -e "${GREEN}=== Despliegue completado con éxito ===${NC}"
echo -e "${GREEN}La función Lambda se ejecutará diariamente a la 1:00 AM UTC${NC}"
echo -e "${GREEN}Puedes modificar la programación en la consola de EventBridge o actualizando el stack${NC}"