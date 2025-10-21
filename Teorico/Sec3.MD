# SECCIÓN 3: JINJA EN DBT

## Pregunta 1: Explica la diferencia entre los delimitadores `{{ ... }}` y `{% ... %}` en Jinja. Proporciona un ejemplo donde usarías cada uno.

### Diferencias Fundamentales:

**`{{ }}` - EXPRESIONES (Generan output)**
- Evalúan y retornan un valor
- El resultado se inserta en el SQL compilado
- Usado para llamar funciones y variables

**`{% %}` - DECLARACIONES (Control de flujo)**
- Controlan la lógica del template
- No generan output directo
- Usado para condicionales, loops y definiciones

**`{# #}` - COMENTARIOS**
- No se compilan en el SQL final
- Solo para documentación del desarrollador

### Ejemplos de Uso:

**`{{ }}` para expresiones:**
```sql
SELECT
    order_id,
    {{ cents_to_dollars('amount') }} as amount_dollars,  -- Llama macro
    '{{ target.schema }}' as schema_name                  -- Inserta variable
FROM {{ ref('stg_orders') }}                               -- Referencia modelo
WHERE order_date >= '{{ var("start_date") }}'              -- Variable del proyecto
```

**`{% %}` para control de flujo:**
```sql
SELECT
    order_id,
    order_date,
    {% if target.name == 'prod' %}
        CURRENT_TIMESTAMP() as loaded_at
    {% else %}
        NULL as loaded_at
    {% endif %}
FROM {{ ref('stg_orders') }}
```

**Loops con `{% %}`:**
```sql
{% set payment_methods = ['credit_card', 'debit_card', 'paypal'] %}

SELECT
    order_date,
    {% for method in payment_methods %}
        SUM(CASE WHEN payment_method = '{{ method }}' THEN amount ELSE 0 END) as {{ method }}_total
        {% if not loop.last %},{% endif %}
    {% endfor %}
FROM {{ ref('stg_orders') }}
GROUP BY 1
```

### Regla General:

- Si necesitas que algo **aparezca en el SQL**: usa `{{ }}`
- Si necesitas **controlar el flujo**: usa `{% %}`
- Si necesitas **documentar**: usa `{# #}`

---

## Pregunta 2: Crea una tabla agregada `fct_event_summary` que cuente eventos por tipo y día, filtrando dinámicamente por tipos específicos usando variables de proyecto.

### Paso 1: Definir Variable en dbt_project.yml

```yaml
# dbt_project.yml
vars:
  tracked_event_types:
    - 'page_view'
    - 'button_click'
    - 'form_submit'
    - 'purchase'
    - 'add_to_cart'
```

### Paso 2: Crear el Modelo

```sql
-- models/marts/analytics/fct_event_summary.sql

{{
    config(
        materialized='table',
        partition_by={'field': 'event_date', 'data_type': 'date'}
    )
}}

{% set event_types = var('tracked_event_types') %}

SELECT
    DATE(event_timestamp) as event_date,
    event_type,

    COUNT(*) as event_count,
    COUNT(DISTINCT user_id) as unique_users,
    COUNT(DISTINCT session_id) as unique_sessions

FROM {{ ref('stg_events') }}

WHERE event_type IN (
    {% for event_type in event_types %}
        '{{ event_type }}'{% if not loop.last %},{% endif %}
    {% endfor %}
)

GROUP BY 1, 2
ORDER BY 1 DESC, 3 DESC
```

### SQL Compilado:

```sql
SELECT
    DATE(event_timestamp) as event_date,
    event_type,

    COUNT(*) as event_count,
    COUNT(DISTINCT user_id) as unique_users,
    COUNT(DISTINCT session_id) as unique_sessions

FROM analytics.stg_events

WHERE event_type IN (
    'page_view',
    'button_click',
    'form_submit',
    'purchase',
    'add_to_cart'
)

GROUP BY 1, 2
ORDER BY 1 DESC, 3 DESC
```

### Variante: Pivotear Columnas Dinámicamente

```sql
{% set event_types = var('tracked_event_types') %}

SELECT
    event_date,

    {% for event_type in event_types %}
        COUNT(CASE WHEN event_type = '{{ event_type }}' THEN 1 END) as {{ event_type }}_count,
        COUNT(DISTINCT CASE WHEN event_type = '{{ event_type }}' THEN user_id END) as {{ event_type }}_users
        {% if not loop.last %},{% endif %}
    {% endfor %}

FROM {{ ref('stg_events') }}
GROUP BY 1
```

### Override de Variables por Ambiente:

```bash
# Ejecutar con diferentes event types
dbt run --vars '{"tracked_event_types": ["page_view", "purchase"]}'
```

### Beneficios:

1. **Configuración centralizada**: Cambio en un solo lugar
2. **Reutilizable**: Misma variable en múltiples modelos
3. **Flexible**: Override por ambiente o ejecución
4. **Mantenible**: Agregar/remover tipos sin tocar SQL

---
