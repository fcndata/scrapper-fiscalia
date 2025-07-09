# SCRAPER FISCALÍA

## Propósito
Este proyecto implementa un sistema completo de extracción, transformación y enriquecimiento de datos de empresas y sociedades desde fuentes oficiales chilenas. El sistema extrae datos del Registro de Empresas y Sociedades y del Diario Oficial, los enriquece con información de bases de datos corporativas (empresas y funcionarios), y genera datasets consolidados para análisis empresarial.

## Arquitectura

### Componentes Principales
- **BrowserSession**: Gestiona la sesión del navegador Chrome/Selenium para scraping web.
- **BaseScraper**: Clase base abstracta que define el flujo de scraping.
- **SociedadScraper**: Implementación para extraer datos del Registro de Empresas y Sociedades.
- **DiarioScraper**: Implementación para extraer datos del Diario Oficial.
- **S3Manager**: Gestiona operaciones de almacenamiento en AWS S3 con estructura jerárquica por fecha.
- **AthenaManager**: Gestiona consultas a bases de datos corporativas via AWS Athena.
- **CompanyMetadata**: Modelo de datos Pydantic para validación y serialización.
- **Config**: Singleton para gestionar la configuración del sistema.

### Estructura de Directorios
```
scrapper-fiscalia/
├── .amazonq/rules/           # Reglas para Amazon Q
├── .devcontainer/            # Configuración del entorno de desarrollo
├── .lambdacontainer/         # Configuración para despliegue en AWS Lambda
├── config/                   # Configuración del sistema
├── data/                     # Datos extraídos (ejemplo)
├── logs/                     # Sistema de logging
├── src/                      # Código fuente principal
│   ├── scraper.py            # Implementación de scrapers
│   ├── utils.py              # Funciones auxiliares y queries SQL
│   ├── s3.py                 # Gestión de almacenamiento en S3
│   ├── models.py             # Modelos de datos
│   ├── athena.py             # Gestión de consultas Athena
│   ├── lambda.py             # Handler para AWS Lambda (extracción)
│   └── lambda_transform.py   # Handler para AWS Lambda (transformación)
└── trigger.py                # Punto de entrada principal
```

## Flujo de Datos

### 1. Extracción (Lambda de Scraping)
- **Entrada**: Configuración de URLs y parámetros de scraping
- **Proceso**:
  - Inicialización de scrapers con Selenium/Chrome
  - Navegación a fuentes oficiales (Registro de Empresas, Diario Oficial)
  - Extracción de datos usando BeautifulSoup
  - Validación y serialización a objetos CompanyMetadata
  - Guardado en formato JSONL temporal
- **Salida**: Archivos JSONL subidos a S3 en ruta `raw/`

### 2. Transformación y Enriquecimiento (Lambda de Transformación)
- **Entrada**: Archivos JSONL desde S3 raw
- **Proceso**:
  - Descarga y consolidación de archivos JSONL
  - Consulta a bases de datos corporativas via Athena:
    - Datos de empresas (segmento, plataforma, ejecutivo)
    - Datos de funcionarios (contacto, dependencia)
  - Unión de datos usando RUT como clave principal
  - Eliminación de duplicados y validación de integridad
- **Salida**: DataFrame consolidado subido a S3 en formato Parquet

### 3. Estructura de Datos Final
El dataset final incluye:
- **Datos originales**: RUT, razón social, URL, actuación, CVE, fechas
- **Datos empresariales**: Segmento, plataforma, código ejecutivo
- **Datos de funcionarios**: RUT, nombre, puesto, correo, dependencia

## Dependencias Clave

### Scraping y Procesamiento
- **Selenium**: Navegación web automatizada y extracción de datos dinámicos
- **BeautifulSoup**: Parsing de HTML y extracción de datos
- **Pandas**: Procesamiento de datos y manipulación de DataFrames
- **Pydantic**: Validación de datos y serialización

### AWS y Base de Datos
- **Boto3**: Interacción con servicios AWS (S3, Athena)
- **PyAthena**: Cliente específico para consultas Athena

### Configuración y Utilidades
- **PyYAML**: Manejo de configuración
- **Pathlib**: Manipulación de rutas de archivos

## Configuración

### Archivo config/config.yaml
```yaml
scraper:
  headless: New
  user_agent: 'Mozilla/5.0 ...'
  timeout_seconds: 230

urls: 
  sociedades: https://www.registrodeempresasysociedades.cl/...
  diario_oficial: https://www.diariooficial.interior.gob.cl/...
  
output:
  sociedades: /tmp/empresa_scraper.jsonl 
  diario_oficial: /tmp/diario_scraper.jsonl

aws:
  s3_bucket: 'bucket-name'
  s3_name: 'scraper/fiscalia'
  region: 'us-east-1'
  athena_maestro_empresa: 'database.table'
```

## Uso

### Ejecución Local
```bash
# Ejecutar el scraper completo
python trigger.py

# Ejecutar Lambda de extracción (simulación)
python src/lambda.py

# Ejecutar Lambda de transformación (simulación)
python src/lambda_transform.py
```

### Despliegue en AWS Lambda

#### Lambda de Extracción
1. Construir imagen con `.lambdacontainer/Dockerfile`
2. Subir imagen a ECR
3. Configurar función Lambda con imagen
4. Asignar rol IAM con permisos S3

#### Lambda de Transformación
1. Configurar función Lambda con runtime Python
2. Asignar rol IAM con permisos S3 y Athena
3. Configurar variables de entorno para conexión Athena

### Estructura de Datos en S3
```
s3://bucket/scraper/fiscalia/YYYYMMDD/
├── raw/
│   ├── empresa_scraper.jsonl
│   └── diario_scraper.jsonl
└── processed/
    └── empresas_enriquecidas_YYYYMMDD_HHMMSS.parquet
```

## Características Avanzadas

### Gestión de Duplicados
- Consultas SQL optimizadas con `ROW_NUMBER()` para obtener registros más recientes
- Eliminación de duplicados en el proceso de merge
- Validación de integridad de datos

### Manejo de Errores
- Logging detallado en cada etapa del proceso
- Manejo específico de excepciones AWS y de red
- Continuación del proceso ante fallos parciales

### Optimizaciones
- Procesamiento en memoria sin archivos temporales
- Consultas SQL parametrizadas y optimizadas
- Conversión directa de DataFrame a Parquet en S3

## Estado Actual

### Funcionalidades Implementadas
- ✅ Extracción automatizada de dos fuentes oficiales
- ✅ Enriquecimiento con datos corporativos
- ✅ Sistema de logging completo
- ✅ Integración completa con AWS (S3, Athena)
- ✅ Validación y limpieza de datos
- ✅ Formato Parquet para consultas eficientes
- ✅ Preparado para despliegue en AWS Lambda

### Métricas de Rendimiento
- Procesamiento de ~1100 registros por ejecución
- Tiempo de ejecución: ~2-3 minutos por Lambda
- Tasa de éxito de enriquecimiento: >95%

## Limitaciones Conocidas

- Dependencia de disponibilidad de fuentes oficiales
- Limitaciones de rate limiting en consultas Athena
- Requiere conexión estable a internet para scraping
- Formato de páginas web puede cambiar requiriendo ajustes