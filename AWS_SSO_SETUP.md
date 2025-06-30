# Configuración de AWS SSO para el Scraper Fiscalía

Este documento describe cómo configurar y usar AWS SSO para autenticar con AWS al desplegar y administrar la infraestructura del Scraper Fiscalía.

## Configuración Inicial de AWS SSO

1. Configura AWS SSO con el siguiente comando:
   ```bash
   aws configure sso
   ```

2. Proporciona la siguiente información:
   - SSO session name: (nombre de tu elección, ej: ftoneguz)
   - SSO start URL: https://awsbech.awsapps.com/start/#
   - SSO region: us-east-1
   - SSO registration scopes: sso:account:access

3. Se abrirá tu navegador para iniciar sesión en AWS SSO. Completa el proceso de autenticación.

4. Selecciona la cuenta `16989303` cuando se te presente la lista de cuentas.

5. Selecciona el rol `BECH_PermiteCreacionRolesIAM`.

6. Configura la región predeterminada como `us-east-1`.

## Verificación de la Sesión SSO

Para verificar que tu sesión SSO está activa, ejecuta:

```bash
./check_sso.sh
```

Este script verificará si tu sesión está activa y, si ha expirado, iniciará una nueva sesión automáticamente.

## Renovación de la Sesión SSO

Las sesiones de AWS SSO expiran después de un tiempo (generalmente 8-12 horas). Para renovar manualmente tu sesión:

```bash
aws sso login --profile 16989303_BECH_PermiteCreacionRolesIAM
```

## Despliegue con AWS SSO

El script `deploy.sh` está configurado para usar tu perfil SSO. Para desplegar la infraestructura:

1. Asegúrate de que tu sesión SSO esté activa usando `./check_sso.sh`
2. Ejecuta el script de despliegue: `./deploy.sh`

## Solución de Problemas

Si encuentras errores relacionados con la autenticación:

1. Verifica que tu sesión SSO esté activa: `aws sts get-caller-identity --profile 16989303_BECH_PermiteCreacionRolesIAM`
2. Si la sesión ha expirado, inicia una nueva: `aws sso login --profile 16989303_BECH_PermiteCreacionRolesIAM`
3. Verifica que el perfil SSO tenga los permisos necesarios para crear y administrar los recursos de AWS requeridos

## Permisos Necesarios

El rol `BECH_PermiteCreacionRolesIAM` debe tener permisos para:

- Crear y administrar roles IAM
- Crear y administrar funciones Lambda
- Crear y administrar reglas de EventBridge
- Acceder a ECR para subir imágenes
- Acceder a S3 para almacenar datos

Si encuentras errores de permisos, contacta al administrador de AWS para solicitar los permisos adicionales necesarios.