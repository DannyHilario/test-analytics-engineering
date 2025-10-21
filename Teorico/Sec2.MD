# SECCIÓN 2: PREGUNTAS DE RESPUESTA CORTA Y EJEMPLOS

## Pregunta 1: Explica la diferencia entre una materialización `view` y una materialización `table`. ¿Cuándo usarías una sobre la otra?

### Diferencias Principales:

**VIEW (Vista):**
- No almacena datos físicamente, solo la consulta SQL
- Siempre refleja los datos más recientes
- Rápida de construir, lenta de consultar
- No consume almacenamiento adicional

**TABLE (Tabla):**
- Almacena datos físicamente en disco
- Snapshot de datos en el momento de construcción
- Lenta de construir, rápida de consultar
- Consume almacenamiento

### Cuándo Usar Cada Una:

**VIEW:**
- Capa de staging (transformaciones 1:1)
- Datos que cambian frecuentemente
- Transformaciones simples sin agregaciones pesadas
- Cuando el almacenamiento es una preocupación

**TABLE:**
- Capa de marts (modelos de negocio)
- Agregaciones complejas o múltiples JOINs
- Modelos consultados frecuentemente por dashboards
- Cuando la velocidad de consulta es crítica

**Ejemplo práctico:**
```sql
-- Staging: VIEW
{{ config(materialized='view') }}
SELECT * FROM {{ source('raw', 'customers') }}
WHERE is_deleted = false

-- Marts: TABLE
{{ config(materialized='table') }}
SELECT
    customer_id,
    COUNT(order_id) as total_orders,
    SUM(order_amount) as lifetime_value
FROM {{ ref('stg_orders') }}
GROUP BY 1
```

---

## Pregunta 2: Describe cómo implementarías un modelo incremental en dbt. Incluye un ejemplo de cómo dbt gestiona las inserciones/actualizaciones con la cláusula `is_incremental()`.

### Implementación de Modelo Incremental:

Un modelo incremental procesa solo datos nuevos o modificados en lugar de reconstruir toda la tabla.

**Componentes clave:**
1. `materialized='incremental'` en configuración
2. `unique_key` para identificar registros únicos
3. `is_incremental()` para lógica condicional
4. `{{ this }}` para referenciar la tabla actual

### Ejemplo Completo:

```sql
{{
    config(
        materialized='incremental',
        unique_key='event_id',
        incremental_strategy='merge'
    )
}}

SELECT
    event_id,
    user_id,
    event_timestamp,
    event_type
FROM {{ source('raw', 'events') }}

{% if is_incremental() %}
    -- Solo registros nuevos después de la última carga
    WHERE event_timestamp > (SELECT MAX(event_timestamp) FROM {{ this }})
{% endif %}
```

### Funcionamiento:

**Primera ejecución:**
- `is_incremental()` = FALSE
- Carga todos los datos históricos

**Ejecuciones subsecuentes:**
- `is_incremental()` = TRUE
- Solo carga eventos nuevos desde el último MAX(event_timestamp)
- La estrategia `merge` actualiza registros existentes e inserta nuevos

**Estrategias disponibles:**
- `append`: Solo inserta
- `merge`: Actualiza o inserta (upsert)
- `delete+insert`: Elimina y vuelve a insertar particiones completas

---

## Pregunta 3: ¿Qué son las "macros" en dbt y cómo pueden mejorar la mantenibilidad y reusabilidad del código? Proporciona un ejemplo de una macro sencilla.

### Definición:

Las macros son funciones reutilizables en Jinja que generan código SQL. Son equivalentes a funciones en programación tradicional.

### Beneficios:

1. **DRY (Don't Repeat Yourself)**: Elimina duplicación de código
2. **Mantenibilidad**: Cambios en un solo lugar
3. **Consistencia**: Misma lógica en todo el proyecto
4. **Multi-plataforma**: Adapta SQL según el data warehouse
5. **Reusabilidad**: Compartible vía packages

### Ejemplo - Macro para Convertir Centavos a Dólares:

```sql
-- macros/cents_to_dollars.sql
{% macro cents_to_dollars(column_name, precision=2) %}
    ROUND({{ column_name }} / 100.0, {{ precision }})
{% endmacro %}
```

**Uso en modelos:**
```sql
SELECT
    order_id,
    {{ cents_to_dollars('amount_cents') }} as amount_dollars,
    {{ cents_to_dollars('tax_cents', 4) }} as tax_dollars
FROM {{ ref('stg_orders') }}
```

**SQL compilado:**
```sql
SELECT
    order_id,
    ROUND(amount_cents / 100.0, 2) as amount_dollars,
    ROUND(tax_cents / 100.0, 4) as tax_dollars
FROM analytics.stg_orders
```

### Impacto en Mantenibilidad:

Si necesitas cambiar la lógica de conversión, solo modificas la macro una vez en lugar de actualizar cada modelo que realiza esta transformación.

---

## Pregunta 4: ¿Cómo definirías tests para asegurar que `order_id` es único y no nulo, y que `order_date` no es una fecha futura?

### Solución en schema.yml:

```yaml
version: 2

models:
  - name: stg_orders
    description: "Órdenes limpias y normalizadas"
    columns:
      - name: order_id
        description: "Identificador único de la orden"
        tests:
          - unique
          - not_null

      - name: order_date
        description: "Fecha de la orden"
        tests:
          - not_null
          - dbt_utils.expression_is_true:
              expression: "<= current_date()"
              name: order_date_not_in_future
```

### Explicación de Tests:

**1. `unique`**: Verifica que no haya valores duplicados
```sql
-- SQL generado
SELECT order_id, COUNT(*) as n_records
FROM stg_orders
GROUP BY order_id
HAVING COUNT(*) > 1
```

**2. `not_null`**: Verifica ausencia de valores nulos
```sql
-- SQL generado
SELECT * FROM stg_orders
WHERE order_id IS NULL
```

**3. `expression_is_true`**: Test personalizado con expresión booleana
```sql
-- SQL generado
SELECT * FROM stg_orders
WHERE NOT (order_date <= current_date())
```

### Ejecución:

```bash
dbt test --select stg_orders
```

Los tests fallan si retornan registros (registros problemáticos). Pasan si retornan 0 filas.

---
