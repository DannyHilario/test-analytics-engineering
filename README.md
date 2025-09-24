---

## Sección 1: Preguntas de Opción Múltiple (30 puntos - 3 puntos cada una)
Selecciona la opción más adecuada para cada pregunta.

**1. ¿Cuál es la función principal del archivo `dbt_project.yml`?**
    a) Definir la configuración de las credenciales de la base de datos.
    b) Configurar la estructura del proyecto dbt, incluyendo rutas de modelos, macros, seeds, variables y hooks.
    c) Almacenar los resultados de las pruebas de los modelos.
    d) Ejecutar los modelos dbt en un entorno de producción.

**2. En dbt, ¿qué significa el término "materialización" (materialization)?**
    a) El proceso de convertir el código SQL de dbt en objetos de base de datos como tablas o vistas.
    b) La forma en que dbt gestiona las dependencias entre modelos.
    c) La estrategia de versionado de los modelos dbt.
    d) La manera en que dbt interactúa con el sistema de control de versiones.

**3. ¿Cuál de las siguientes materializaciones es la más adecuada para un modelo que contiene datos que cambian frecuentemente y necesitas acceder a la versión más actualizada?**
    a) `table`
    b) `view` (para datos que cambian frecuentemente y quieres la versión en tiempo real sin costo de almacenamiento, asumiendo que la vista es performante sobre la base).
    c) `incremental`
    d) `ephemeral`

**4. ¿Qué problema busca resolver la materialización incremental?**
    a) Reducir el tiempo de ejecución de modelos con grandes volúmenes de datos, procesando solo los cambios desde la última ejecución.
    b) Permitir que los modelos dbt se ejecuten en diferentes bases de datos.
    c) Facilitar la documentación automática de los modelos.
    d) Optimizar el rendimiento de las consultas SQL generadas por dbt.

**5. ¿Para qué se utilizan los packages en dbt?**
    a) Para encapsular y reutilizar la lógica de negocio, macros y modelos comunes, fomentando las mejores prácticas y la modularidad.
    b) Para definir las variables de entorno de un proyecto dbt.
    c) Para gestionar las credenciales de la base de datos de forma segura.
    d) Para especificar el orden de ejecución de los modelos.

**6. ¿Cuál de los siguientes comandos se utiliza para ejecutar las pruebas definidas en un proyecto dbt?**
    a) `dbt run`
    b) `dbt build`
    c) `dbt test` (o `dbt build` que incluye tests, pero `dbt test` es específico para pruebas).
    d) `dbt docs generate`

**7. ¿Cuál es el propósito de los "seeds" en dbt?**
    a) Cargar datos estáticos o de referencia directamente en la base de datos a través de archivos CSV, ideal para datos pequeños y de configuración.
    b) Definir la estructura de la base de datos.
    c) Ejecutar scripts personalizados antes o después de los modelos dbt.
    d) Almacenar las configuraciones de los perfiles de conexión.

**8. Si tienes una macro llamada `generate_uuid()` en tu proyecto, ¿cómo la llamarías dentro de un modelo SQL?**
    a) `{{ generate_uuid() }}`
    b) `{% generate_uuid() %}`
    c) `[[ generate_uuid() ]]`
    d) `(( generate_uuid() ))`

**9. ¿Cuál es la principal ventaja de usar la directiva `ref()` en lugar de referenciar directamente las tablas en SQL?**
    a) Permite a dbt construir un grafo de dependencias y gestionar el orden de ejecución, además de manejar automáticamente el esquema y las relaciones entre entornos.
    b) Mejora la legibilidad del código SQL.
    c) Evita errores de sintaxis en las consultas SQL.
    d) Permite la ejecución de modelos en diferentes esquemas.

**10. ¿Cuál de las siguientes afirmaciones sobre las "exposures" es verdadera?**
    a) Las exposures se utilizan para definir modelos upstream que dependen de otros modelos.
    b) Las exposures representan una capa de aplicación, dashboard (BI), o notebook que consume los datos transformados por dbt, documentando el uso downstream.
    c) Las exposures son un tipo de materialización para tablas pequeñas.
    d) Las exposures son una forma de documentar las pruebas de los modelos.

---

## Sección 2: Preguntas de Respuesta Corta y Ejemplos (40 puntos - 10 puntos cada una)
Proporciona respuestas concisas y, cuando se solicite, ejemplos de código.

**1. Explica la diferencia entre una materialización `view` y una materialización `table`. ¿Cuándo usarías una sobre la otra?**

**2. Describe cómo implementarías un modelo incremental en dbt. Incluye un ejemplo de cómo dbt gestiona las inserciones/actualizaciones con la cláusula `is_incremental()`.**

**3. ¿Qué son las "macros" en dbt y cómo pueden mejorar la mantenibilidad y reusabilidad del código en un proyecto dbt? Proporciona un ejemplo de una macro sencilla que podría ser útil en varios modelos.**

**4. Imagina que tienes un modelo `stg_orders` y quieres asegurarte de que la columna `order_id` es única y no nula, y que la columna `order_date` siempre tiene una fecha válida (no es una fecha futura). ¿Cómo definirías estas pruebas en tu archivo `schema.yml`?**

---

## Sección 3: Jinja en dbt (20 puntos - 10 puntos cada una)
Demuestra tu comprensión de Jinja en dbt.

**1. Explica la diferencia entre los delimitadores `{{ ... }}` y `{% ... %}` en Jinja dentro de dbt. Proporciona un ejemplo donde usarías cada uno.**

**2. Tienes una tabla `stg_events` con una columna `event_type`. Quieres crear una tabla agregada `fct_event_summary` que cuente los eventos por tipo y por día, pero solo para un conjunto específico de `event_type` que podría variar. Usa una variable de proyecto y Jinja para lograr esto de forma dinámica.**

---

## Sección 4: Pruebas Estadísticas Personalizadas con Macros (20 puntos - 10 puntos cada una)
Diseña y aplica pruebas estadísticas personalizadas usando macros.

**1. Describe el proceso general para crear una prueba personalizada en dbt usando una macro. ¿Qué estructura básica debe tener la macro de prueba y cómo se invocaría en `schema.yml`?**

**2. Crea una macro de prueba personalizada en dbt llamada `test_column_values_below_std_dev_threshold` que falle si algún valor en una columna numérica está más de N desviaciones estándar por encima del promedio. Los argumentos de la macro deben ser `model`, `column_name` y `std_dev_threshold` (el valor por defecto será 3).**

---

## Sección 5: Diseño y Solución de Problemas Avanzados (30 puntos - 15 puntos cada una)
Aplica tus conocimientos de dbt para resolver los siguientes escenarios.

**Diseño de un Flujo de Datos con Fuentes Dispares y Modelos Exponibles:**

Tienes los siguientes datos crudos de diferentes fuentes:
*   Sistema de CRM (PostgreSQL): `customers` (`id_cliente`, `nombre`, `email`, `segmento`, `fecha_registro`)
*   Sistema de Ventas (Snowflake): `orders` (`id_pedido`, `id_cliente_crm`, `fecha_pedido`, `total_pedido`, `estado`, `id_vendedor`)
*   Sistema de Inventario (Google BigQuery): `products` (`id_producto`, `nombre_producto`, `categoria`, `precio_unitario`, `stock_actual`)
*   Tabla de Referencia (CSV seed): `currency_conversion_rates` (`fecha`, `moneda_origen`, `moneda_destino`, `tasa`)

Diseña un flujo de modelos dbt para crear una tabla final `fct_daily_sales_performance` que contenga las ventas diarias agregadas por vendedor y segmento de cliente, en una moneda común (USD).

La tabla final debe incluir:
*   `fecha_pedido`
*   `id_vendedor`
*   `nombre_vendedor` (asume que puedes obtener esto de un `stg_employees` que aún no existe, pero lo mencionarás)
*   `segmento_cliente`
*   `total_ventas_usd_dia`
*   `numero_pedidos_dia`

Además, define una exposure para un dashboard de BI llamado "Sales Performance Dashboard" que consumirá `fct_daily_sales_performance`.

Indica los nombres de los modelos (`stg_`, `int_`, `fct_`), sus materializaciones, las fuentes (`source`) y las relaciones generales entre ellos.

Divide tu respuesta en 5 puntos principales:
1.  **Fuentes**
2.  **Seeds**
3.  **Flujo de Modelos** (Nombres de modelos, materializaciones y un breve resumen de lo que hace cada uno)
4.  **Exposure**
5.  **Linaje** (descripción del grafo de dependencias o un diagrama textual simple)

---

**¡Buena suerte con el examen teorico!**

---

# Ejercicio Práctico para Analytics Engineering

Debe realizar un fork de este repositorio para desarrollar y entregar su trabajo.

Si está interesado en aplicar al test, puede enviar un correo a jgarcial@deacero.com.

## Ejercicio DBT

Este ejercicio utiliza datos de una campaña de marketing por correo electrónico disponibles en el [UCI Machine Learning Repository](https://archive.ics.uci.edu/dataset/222/bank+marketing). Los datos contienen información sobre diversas campañas de marketing directo de una institución bancaria.

**Objetivo del Ejercicio:**

El objetivo es evaluar el dominio de la herramienta DBT (Data Build Tool) del candidato, su capacidad para incorporar pruebas unitarias, mantener la calidad de los datos y desplegar modelos de datos en BigQuery. El objetivo de negocio es crear un Data Mart que permita al equipo de marketing analizar la efectividad de sus campañas, enfocándose en KPIs como la tasa de conversión, el número de contactos exitosos y la segmentación de clientes.

### Instrucciones del Ejercicio:

1. **Configuración del Proyecto DBT:**
    - Crea un nuevo proyecto de DBT.
    - Conéctalo a BigQuery. Asegúrate de configurar correctamente las credenciales y el dataset de destino.
    
2. **Obtención de Datos:**
    - Descarga los datos del [Bank Marketing dataset](https://archive.ics.uci.edu/dataset/222/bank+marketing).
    - Carga los datos en una tabla en BigQuery llamada `raw_bank_marketing`.

3. **Modelado:**
    - Crea un modelo `staging_bank_marketing.sql` para transformar los datos iniciales utilizando CTEs (Common Table Expressions). Este modelo debe realizar las siguientes tareas:
        - Limpiar y normalizar los datos.
        - Filtrar registros irrelevantes.
        - Crear nuevas columnas necesarias para el análisis.
        
    - Crea un modelo `kpi_bank_marketing.sql` para agregar los KPIs de marketing utilizando CTEs. Este modelo debe calcular:
        - Tasa de conversión: porcentaje de contactos exitosos sobre el total de contactos.
        - Número de contactos exitosos: total de conversiones logradas.
        - Segmentación de clientes: clasificación de clientes basada en criterios relevantes como edad, ocupación, etc.
       
4. **Pruebas Unitarias:**
    - Agrega pruebas unitarias en el archivo `schema.yml` para asegurar la integridad de los datos. Incluye pruebas para:
        - Validar tipos de datos.
        - Comprobar valores nulos.
        - Verificar rangos y unicidad de campos clave.
    
5. **Despliegue y Calidad:**
    - Configura un pipeline CI/CD para desplegar los modelos DBT usando herramientas como GitHub Actions o GitLab CI. Asegúrate de incluir pasos para:
        - Validación de código.
        - Ejecución de pruebas unitarias.
        - Despliegue en BigQuery.
    - Configura alertas para pruebas fallidas y realiza auditorías periódicas de calidad de datos. Considera el uso de herramientas como dbt tests, Great Expectations, o similares para automatizar estas auditorías.

### Entrega del Ejercicio

- Suba su proyecto a un repositorio de GitHub.
- Asegúrese de que el repositorio incluya:
    - Todo el código fuente del proyecto DBT.
    - Documentación que explique el proceso seguido, las decisiones tomadas y cómo ejecutar el proyecto.
    - Instrucciones claras sobre cómo configurar y ejecutar el pipeline CI/CD.

### Criterios de Evaluación

- **Exactitud y eficacia del modelo:** ¿Los modelos transforman y agregan los datos de manera correcta y eficiente?
- **Calidad del código:** ¿El código es claro, bien documentado y sigue buenas prácticas?
- **Implementación de pruebas unitarias:** ¿Las pruebas unitarias son exhaustivas y cubren los casos relevantes?
- **Despliegue y automatización:** ¿El pipeline CI/CD está correctamente configurado y automatiza el proceso de despliegue y pruebas?

¡Suerte a todos! 

---
