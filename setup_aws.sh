#!/bin/bash
set -e

# Colores para mensajes
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${YELLOW}=== Configuración de AWS CLI ===${NC}"

# Verificar si AWS CLI está instalado
if ! command -v aws &> /dev/null; then
    echo -e "${RED}AWS CLI no está instalado. Instalando...${NC}"
    curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
    unzip awscliv2.zip
    sudo ./aws/install
    rm -rf aws awscliv2.zip
fi

# Verificar la versión de AWS CLI
aws_version=$(aws --version)
echo -e "${GREEN}AWS CLI instalado: ${aws_version}${NC}"

# Verificar si hay credenciales configuradas
if aws sts get-caller-identity &> /dev/null; then
    echo -e "${GREEN}Ya tienes una sesión de AWS activa.${NC}"
    aws sts get-caller-identity
else
    echo -e "${YELLOW}No se detectó una sesión de AWS activa. Configurando...${NC}"
    
    # Solicitar credenciales
    echo -e "${YELLOW}Ingresa tus credenciales de AWS:${NC}"
    read -p "AWS Access Key ID: " aws_access_key
    read -sp "AWS Secret Access Key: " aws_secret_key
    echo ""
    read -p "Región predeterminada [us-east-1]: " aws_region
    aws_region=${aws_region:-us-east-1}
    
    # Configurar AWS CLI
    mkdir -p ~/.aws
    
    cat > ~/.aws/credentials << EOF
[default]
aws_access_key_id = ${aws_access_key}
aws_secret_access_key = ${aws_secret_key}
EOF

    cat > ~/.aws/config << EOF
[default]
region = ${aws_region}
output = json
EOF

    echo -e "${GREEN}Credenciales configuradas correctamente.${NC}"
    
    # Verificar la configuración
    if aws sts get-caller-identity &> /dev/null; then
        echo -e "${GREEN}Sesión de AWS verificada correctamente:${NC}"
        aws sts get-caller-identity
    else
        echo -e "${RED}No se pudo verificar la sesión de AWS. Revisa tus credenciales.${NC}"
        exit 1
    fi
fi

echo -e "${GREEN}=== Configuración de AWS CLI completada ===${NC}"