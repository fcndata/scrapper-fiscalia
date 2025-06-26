# Reglas de Codificación Python para Amazon Q

## PEP 8 y estilo de código
Asegúrate de que todo el código siga las convenciones PEP 8:
- Usa 4 espacios para indentación, nunca tabulaciones
- Limita las líneas a 88 caracteres máximo
- Usa snake_case para variables, funciones y métodos
- Usa PascalCase para clases
- Organiza los imports en bloques: estándar, terceros, propios
- Usa espacios alrededor de operadores
- Evita líneas en blanco innecesarias

## Documentación
Toda función, clase y método debe tener docstrings en formato Google:
- Descripción breve en primera línea
- Descripción detallada después de línea en blanco (opcional)
- Args: para parámetros
- Returns: para valores de retorno
- Raises: para excepciones que puede lanzar
- Examples: para ejemplos de uso cuando sea necesario

Los docstrings deben explicar el "qué" hace la función, no el "cómo".

## Estructura y Organización
- Cada módulo debe tener una responsabilidad única y bien definida
- Limita el tamaño de las funciones a 30 líneas máximo
- Evita duplicación de código (principio DRY)
- Usa clases para agrupar funcionalidad relacionada
- Separa la lógica de negocio de la interfaz de usuario
- Organiza el código en capas: presentación, lógica de negocio, acceso a datos
- Usa inyección de dependencias para facilitar pruebas

## Prácticas de seguridad
- Valida siempre las entradas de usuario antes de procesarlas
- Usa parámetros parametrizados para consultas SQL
- No almacenes credenciales en el código fuente
- Usa variables de entorno o servicios de secretos para credenciales
- Implementa control de acceso adecuado
- Registra intentos de acceso fallidos
- Sanitiza datos antes de mostrarlos al usuario
- Maneja errores sin exponer información sensible

## Manejo de errores
- Usa un logger singleton configurado para todo el proyecto
- Captura excepciones específicas, no Exception genérica
- Proporciona mensajes de error descriptivos
- Usa niveles de log apropiados (DEBUG, INFO, WARNING, ERROR, CRITICAL)
- Incluye contexto en los mensajes de error (ID de transacción, datos relevantes)
- No suprimas excepciones sin registrarlas
- Implementa retry patterns para operaciones que pueden fallar temporalmente
- Usa try/except solo alrededor del código que puede fallar, no bloques grandes