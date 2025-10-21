# SECCIÓN 1: CONCEPTOS FUNDAMENTALES DE DBT

## Pregunta 1: ¿Cuál es la función principal del archivo `dbt_project.yml`?

**Respuesta: b) Configurar la estructura del proyecto dbt, incluyendo rutas de modelos, macros, seeds, variables y hooks.**

El `dbt_project.yml` es el archivo de configuración maestro del proyecto. Define la estructura organizacional, rutas de componentes, materializaciones por defecto, variables globales y hooks de ejecución.

---

## Pregunta 2: En dbt, ¿qué significa el término "materialización" (materialization)?

**Respuesta: a) El proceso de convertir el código SQL de dbt en objetos de base de datos como tablas o vistas.**

La materialización define cómo dbt persiste el código SQL en la base de datos. Los tipos principales son:
- **VIEW**: No almacena datos, solo la consulta
- **TABLE**: Almacena datos físicamente
- **INCREMENTAL**: Solo procesa registros nuevos/modificados
- **EPHEMERAL**: No se materializa, existe solo como CTE

---

## Pregunta 3: ¿Cuál materialización es más adecuada para datos que cambian frecuentemente?

**Respuesta: b) view**

Una vista refleja siempre el estado actual de las tablas subyacentes, proporcionando datos frescos en tiempo real sin costo de almacenamiento. Es ideal para datos que cambian constantemente donde se requiere la versión más actualizada.

---

## Pregunta 4: ¿Qué problema busca resolver la materialización incremental?

**Respuesta: a) Reducir el tiempo de ejecución de modelos con grandes volúmenes de datos, procesando solo los cambios desde la última ejecución.**

La materialización incremental optimiza el rendimiento al procesar únicamente registros nuevos o modificados en lugar de reconstruir toda la tabla en cada ejecución. Reduce significativamente el tiempo y costo de procesamiento para datasets grandes.

---

## Pregunta 5: ¿Para qué se utilizan los packages en dbt?

**Respuesta: a) Para encapsular y reutilizar la lógica de negocio, macros y modelos comunes, fomentando las mejores prácticas y la modularidad.**

Los packages son colecciones reutilizables de código dbt que permiten compartir macros, tests y modelos entre proyectos. Ejemplos populares incluyen dbt_utils para utilidades generales y dbt_expectations para tests estadísticos avanzados.

---

## Pregunta 6: ¿Cuál comando se utiliza para ejecutar las pruebas en dbt?

**Respuesta: c) `dbt test`**

El comando `dbt test` ejecuta todas las pruebas definidas en el proyecto. Alternativamente, `dbt build` ejecuta modelos y tests de forma integrada, pero `dbt test` es el comando específico para validación.

---

## Pregunta 7: ¿Cuál es el propósito de los "seeds" en dbt?

**Respuesta: a) Cargar datos estáticos o de referencia directamente en la base de datos a través de archivos CSV, ideal para datos pequeños y de configuración.**

Los seeds permiten versionar y cargar datos de referencia pequeños (como mapeos de códigos, tasas de conversión o listas de categorías) directamente desde archivos CSV en el repositorio. Son ideales para datos que cambian poco y requieren menos de 1000 filas.

---

## Pregunta 8: Si tienes una macro llamada `generate_uuid()`, ¿cómo la llamarías en un modelo SQL?

**Respuesta: a) `{{ generate_uuid() }}`**

Las macros se invocan usando la sintaxis de expresiones de Jinja: `{{ }}`. Esta sintaxis genera output que se inserta en el SQL compilado.

---

## Pregunta 9: ¿Cuál es la principal ventaja de usar la directiva `ref()` en lugar de referenciar directamente las tablas en SQL?

**Respuesta: a) Permite a dbt construir un grafo de dependencias y gestionar el orden de ejecución, además de manejar automáticamente el esquema y las relaciones entre entornos.**

La función `ref()` es fundamental en dbt porque:
- Construye el DAG (grafo de dependencias) automáticamente
- Gestiona el orden de ejecución correcto
- Permite ejecución selectiva de modelos
- Funciona en diferentes ambientes (dev/prod) sin cambios
- Facilita refactoring y visualización de dependencias

---

## Pregunta 10: ¿Cuál de las siguientes afirmaciones sobre las "exposures" es verdadera?

**Respuesta: b) Las exposures representan una capa de aplicación, dashboard (BI), o notebook que consume los datos transformados por dbt, documentando el uso downstream.**

Las exposures documentan cómo se utilizan los datos fuera de dbt, incluyendo dashboards, aplicaciones y modelos de ML. Permiten rastrear el impacto de cambios en modelos upstream y documentar la cadena completa de valor de los datos.

---
