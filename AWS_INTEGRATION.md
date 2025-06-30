# Integración del Scraper Fiscalía con AWS

Este documento describe cómo se integra el Scraper Fiscalía con los servicios de AWS para automatizar su ejecución.

## Arquitectura de la Integración

La integración utiliza los siguientes servicios de AWS:

1. **Amazon ECR**: Almacena la imagen Docker del scraper
2. **AWS Lambda**: Ejecuta el scraper en un entorno serverless
3. **Amazon EventBridge**: Programa la ejecución automática del scraper
4. **Amazon S3**: Almacena los datos extraídos
5. **AWS IAM**: Gestiona los permisos y roles necesarios
6. **Amazon CloudWatch**: Monitorea la ejecución y almacena logs

## Flujo de Ejecución

1. EventBridge activa la función Lambda según la programación configurada
2. Lambda descarga la imagen Docker desde ECR y la ejecuta
3. El scraper extrae los datos y los guarda temporalmente
4. Los datos se convierten a formato Parquet y se suben a S3
5. Los logs se envían a CloudWatch

## Requisitos Previos

- AWS CLI configurado con credenciales adecuadas
- Docker instalado localmente
- Permisos para crear recursos en AWS (IAM, Lambda, EventBridge, S3)

## Despliegue

Para desplegar la infraestructura completa, ejecuta el script `deploy.sh`:

```bash
./deploy.sh
```

Este script realiza las siguientes acciones:

1. Construye la imagen Docker usando el Dockerfile en `.lambdacontainer/`
2. Crea un repositorio ECR si no existe
3. Sube la imagen Docker a ECR
4. Despliega la plantilla CloudFormation que define toda la infraestructura

## Configuración

La configuración principal se realiza a través de la plantilla CloudFormation (`template.yaml`). Los parámetros más importantes son:

- **ECRRepositoryUri**: URI del repositorio ECR que contiene la imagen del scraper
- **ScheduleExpression**: Expresión cron para la ejecución programada (por defecto: diariamente a la 1:00 AM UTC)
- **S3BucketName**: Nombre del bucket S3 para almacenar los datos

## Monitoreo

Para monitorear la ejecución del scraper:

1. Revisa los logs en CloudWatch Logs (grupo `/aws/lambda/scraper-fiscalia-ScraperFunction-XXXX`)
2. Configura alarmas en CloudWatch para notificaciones de errores
3. Verifica los archivos generados en S3

## Solución de Problemas

Si el scraper falla durante la ejecución:

1. Revisa los logs en CloudWatch para identificar el error
2. Verifica que la función Lambda tenga suficiente memoria y tiempo de ejecución
3. Asegúrate de que los permisos IAM sean correctos
4. Prueba la imagen Docker localmente antes de desplegarla

## Personalización

Para modificar la programación de ejecución:

1. Actualiza el parámetro `ScheduleExpression` en la plantilla CloudFormation
2. Vuelve a desplegar el stack con `aws cloudformation update-stack`

O bien, modifica la regla directamente en la consola de EventBridge.