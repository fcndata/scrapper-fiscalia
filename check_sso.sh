#!/bin/bash

# Script para verificar el estado de la sesión SSO y renovarla si es necesario

# Perfil SSO a verificar
PROFILE="16989303_BECH_PermiteCreacionRolesIAM"

# Verificar si la sesión está activa
echo "Verificando sesión SSO..."
if aws sts get-caller-identity --profile $PROFILE &> /dev/null; then
    echo "✅ Sesión SSO activa"
    aws sts get-caller-identity --profile $PROFILE
else
    echo "❌ Sesión SSO expirada o no iniciada"
    echo "Iniciando nueva sesión SSO..."
    aws sso login --profile $PROFILE
    
    # Verificar si la renovación fue exitosa
    if aws sts get-caller-identity --profile $PROFILE &> /dev/null; then
        echo "✅ Sesión SSO renovada exitosamente"
        aws sts get-caller-identity --profile $PROFILE
    else
        echo "❌ Error al renovar la sesión SSO"
        exit 1
    fi
fi