# SCRAPER FISCALÍA

## Propósito
Sistema completo de extracción, transformación y enriquecimiento de datos empresariales desde fuentes oficiales chilenas. Extrae datos del Registro de Empresas y Sociedades y del Diario Oficial, los enriquece con información corporativa y genera datasets consolidados en formato Parquet para análisis empresarial.

## Arquitectura
- **Extracción**: Scrapers (Selenium + BeautifulSoup) → JSONL → S3 raw/
- **Transformación**: S3 raw/ → Athena queries → DataFrame merge → S3 processed/ (Parquet)
- **Enriquecimiento**: Datos oficiales + datos empresariales + datos funcionarios
- **Almacenamiento**: AWS S3 con estructura jerárquica por fecha (YYYYMMDD/raw|processed/)
- **Consultas**: AWS Athena para bases de datos corporativas

## Flujo de Datos
1. **Lambda Extracción**: Scraping → Validación → JSONL → S3 raw/
2. **Lambda Transformación**: S3 raw/ → Consolidación → Athena queries → Merge → S3 processed/ (Parquet)
3. **Estructura final**: Datos oficiales + segmento + plataforma + ejecutivo + funcionario

## Dependencias Clave
- **Scraping**: Selenium, BeautifulSoup, Chrome/Chromedriver
- **Datos**: Pandas, Pydantic, PyAthena
- **AWS**: Boto3 (S3, Athena)
- **Formato**: Parquet para consultas eficientes

## Estado Actual
Sistema completo funcionando con dos Lambdas independientes. Procesa ~1100 registros por ejecución con >95% de éxito en enriquecimiento. Optimizado para consultas SQL con ROW_NUMBER() para evitar duplicados. Manejo robusto de errores y logging detallado.

## Problemas Conocidos
- Dependencia de disponibilidad de fuentes oficiales
- Rate limiting en consultas Athena
- Cambios potenciales en estructura de páginas web