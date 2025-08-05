# SCRAPER FISCALÍA

## Propósito
Sistema completo de extracción, transformación y enriquecimiento de datos empresariales desde fuentes oficiales chilenas. Extrae datos del Registro de Empresas y Sociedades y del Diario Oficial, los enriquece con información corporativa, genera datasets consolidados en formato Parquet y envía reportes automáticos por email con estadísticas semanales.

## Arquitectura
- **Extracción**: Scrapers (Selenium + BeautifulSoup) → JSONL → S3 raw/
- **Transformación**: S3 raw/ → Athena queries → DataFrame merge → S3 delivery/ (Parquet)
- **Enriquecimiento**: Datos oficiales + datos empresariales + datos funcionarios
- **Almacenamiento**: AWS S3 con estructura jerárquica por fecha (pa_date=YYYY-MM-DD/)
- **Consultas**: AWS Athena para bases de datos corporativas
- **Reportes**: Sistema de notificaciones automáticas vía AWS SES con estadísticas semanales

## Flujo de Datos
1. **Lambda Extracción**: Scraping → Validación → JSONL → S3 raw/
2. **Lambda Transformación**: S3 raw/ → Consolidación → Athena queries → Merge → S3 delivery/ (Parquet)
3. **Sistema de Reportes**: Generación de estadísticas semanales → Envío automático por email
4. **Estructura final**: Datos oficiales + segmento + plataforma + ejecutivo + funcionario

## Componentes Principales
- **WeeklyStatsManager**: Genera estadísticas semanales desde archivos Parquet en S3
- **SESManager**: Gestiona envío de reportes automáticos por email con archivos adjuntos
- **Scrapers**: Extracción de datos desde fuentes oficiales
- **S3Manager**: Gestión de almacenamiento con estructura particionada
- **AthenaManager**: Consultas a bases de datos corporativas

## Dependencias Clave
- **Scraping**: Selenium, BeautifulSoup, Chrome/Chromedriver
- **Datos**: Pandas, Pydantic, PyAthena
- **AWS**: Boto3 (S3, Athena, SES)
- **Email**: MIME para formateo HTML con adjuntos Excel
- **Formato**: Parquet para consultas eficientes

## Estado Actual
Sistema completo funcionando con dos Lambdas independientes más sistema de reportes automáticos. Procesa ~1100 registros por ejecución con >95% de éxito en enriquecimiento. Genera reportes semanales con estadísticas por fuente (Diario Oficial y Registro de Empresas). Optimizado para consultas SQL con ROW_NUMBER() para evitar duplicados. Manejo robusto de errores y logging detallado.

## Funcionalidades de Reportes
- **Estadísticas Semanales**: Conteo diario por fuente (lunes a domingo)
- **Formato HTML**: Emails con formato profesional y negritas
- **Archivos Adjuntos**: Excel con datos procesados
- **Lógica Temporal**: Semana actual o anterior completa si es lunes
- **Manejo de Días Futuros**: Muestra guiones para fechas sin datos

## Problemas Conocidos
- Dependencia de disponibilidad de fuentes oficiales
- Rate limiting en consultas Athena
- Cambios potenciales en estructura de páginas web
- Dependencia de configuración SES para envío de emails