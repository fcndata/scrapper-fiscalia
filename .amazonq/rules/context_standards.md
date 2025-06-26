# Estándares para Pin Context en Amazon Q

## Estructura del Contexto Fijado
Cuando fijes contexto en Amazon Q, sigue esta estructura para mantener la información organizada:

```
# [NOMBRE DEL PROYECTO/MÓDULO]

## Propósito
Breve descripción del propósito del proyecto o módulo.

## Arquitectura
Descripción concisa de la arquitectura del sistema.

## Flujo de Datos
Explicación de cómo fluyen los datos a través del sistema.

## Dependencias Clave
Lista de dependencias externas críticas.

## Estado Actual
Descripción del estado actual del desarrollo.

## Problemas Conocidos
Lista de problemas conocidos o limitaciones.
```

## Reglas para Fijar Contexto

1. **Sé conciso**: Limita cada sección a 3-5 puntos clave.

2. **Usa formato Markdown**: Utiliza encabezados, listas y código para mejorar la legibilidad.

3. **Incluye solo información esencial**: Fija solo la información que necesitas que Amazon Q recuerde constantemente.

4. **Actualiza regularmente**: Actualiza el contexto fijado cuando haya cambios significativos en el proyecto.

5. **Estructura jerárquica**: Organiza la información de lo general a lo específico.

## Ejemplos de Contexto Bien Estructurado

### Ejemplo para un Scraper:

```
# SCRAPER FISCALÍA

## Propósito
Extraer y procesar datos de documentos públicos de la Fiscalía.

## Arquitectura
- Módulo scraper.py: Extracción de datos
- Módulo utils.py: Funciones auxiliares
- Módulo s3.py: Almacenamiento de datos

## Flujo de Datos
1. Extracción de URLs desde la página principal
2. Descarga de documentos PDF
3. Extracción de metadatos
4. Almacenamiento en formato JSONL/Parquet

## Dependencias Clave
- BeautifulSoup para parsing HTML
- Pandas para procesamiento de datos
- AWS S3 para almacenamiento

## Estado Actual
Implementación básica completa, optimizando rendimiento.

## Problemas Conocidos
- Manejo de errores en conexiones intermitentes
- Validación de datos incompleta
```