# SCRAPER FISCALÍA

## Propósito
Este proyecto implementa un sistema de scraping automatizado para extraer y procesar datos de documentos públicos relacionados con empresas y sociedades desde fuentes oficiales como el Registro de Empresas y Sociedades y el Diario Oficial de Chile. Los datos extraídos son procesados y almacenados para su posterior análisis.

## Arquitectura

### Componentes Principales
- **BrowserSession**: Gestiona la sesión del navegador Chrome/Selenium.
- **BaseScraper**: Clase base abstracta que define el flujo de scraping.
- **SociedadScraper**: Implementación para extraer datos del Registro de Empresas y Sociedades.
- **DiarioScraper**: Implementación para extraer datos del Diario Oficial.
- **S3Manager**: Gestiona la carga y descarga de archivos desde/hacia AWS S3.
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
│   ├── utils.py              # Funciones auxiliares
│   ├── s3.py                 # Gestión de almacenamiento en S3
│   ├── models.py             # Modelos de datos
│   └── lambda.py             # Handler para AWS Lambda
└── trigger.py                # Punto de entrada principal
```

## Flujo de Datos

1. **Extracción**: 
   - El proceso inicia en `trigger.py` que instancia los scrapers.
   - Cada scraper navega a la URL configurada y extrae datos de la página.
   - Los datos extraídos se transforman en objetos `CompanyMetadata`.

2. **Procesamiento**:
   - Los datos se validan para asegurar integridad.
   - Se serializan a formato JSONL y se guardan temporalmente.

3. **Almacenamiento**:
   - Los archivos JSONL se convierten a formato Parquet.
   - Se suben a AWS S3 en una estructura organizada por fecha.
   - Se mantienen versiones raw y processed de los datos.

## Dependencias Clave

- **Selenium**: Para navegación web automatizada y extracción de datos dinámicos.
- **BeautifulSoup**: Para parsing de HTML y extracción de datos.
- **Pydantic**: Para validación de datos y serialización.
- **Boto3**: Para interacción con servicios AWS (S3).
- **Pandas**: Para procesamiento de datos y conversión a formato Parquet.
- **PyYAML**: Para manejo de configuración.

## Estado Actual

- Implementación básica completa para dos fuentes de datos:
  - Registro de Empresas y Sociedades
  - Diario Oficial
- Sistema de logging configurado
- Integración con AWS S3 para almacenamiento
- Preparado para despliegue en AWS Lambda

## Configuración

El sistema se configura a través del archivo `config/config.yaml` que incluye:

- Parámetros del scraper (headless, user-agent, timeout)
- URLs de las fuentes de datos
- Rutas de salida para archivos
- Configuración de AWS (bucket, región)

## Uso

### Ejecución Local

```python
# Ejecutar el scraper completo
python trigger.py

# Ejecutar como función Lambda (simulación)
python src/lambda.py
```

### Despliegue en AWS Lambda

El proyecto incluye configuración para despliegue en AWS Lambda mediante contenedores:

1. Construir la imagen con `.lambdacontainer/Dockerfile`
2. Subir la imagen a ECR
3. Configurar la función Lambda para usar la imagen
4. Asignar el rol IAM adecuado (ver `bedrock_agent_role_custom.json`)

## Problemas Conocidos y Limitaciones

- Manejo de errores en conexiones intermitentes podría mejorarse
- Validación de datos incompleta en algunos escenarios
- No implementa reintentos automáticos en caso de fallos
- La conversión de formatos podría optimizarse para grandes volúmenes de datos