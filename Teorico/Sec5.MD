# SECCIÓN 5: DISEÑO Y SOLUCIÓN DE PROBLEMAS AVANZADOS

## Escenario: Flujo de Datos con Fuentes Dispares y Modelos Exponibles

### Fuentes de Datos:

1. **CRM (PostgreSQL)**: `customers` (id_cliente, nombre, email, segmento, fecha_registro)
2. **Ventas (Snowflake)**: `orders` (id_pedido, id_cliente_crm, fecha_pedido, total_pedido, estado, id_vendedor)
3. **Inventario (BigQuery)**: `products` (id_producto, nombre_producto, categoria, precio_unitario, stock_actual)
4. **Seed (CSV)**: `currency_conversion_rates` (fecha, moneda_origen, moneda_destino, tasa)

### Objetivo:

Crear `fct_daily_sales_performance` con ventas diarias agregadas por vendedor y segmento en USD.

---

## Arquitectura de Solución en 3 Capas:

```
SOURCES → STAGING → INTERMEDIATE → MARTS → EXPOSURES
  (4)       (4)          (2)          (1)       (2)
```

---

## 1. FUENTES - Definición en sources.yml

```yaml
version: 2

sources:
  - name: crm_postgres
    database: crm_production
    schema: public
    tables:
      - name: customers
        columns:
          - name: id_cliente
            tests: [unique, not_null]
        freshness:
          warn_after: {count: 24, period: hour}

  - name: sales_snowflake
    database: sales_db
    schema: raw
    tables:
      - name: orders
        freshness:
          warn_after: {count: 1, period: hour}

  - name: inventory_bigquery
    database: inventory_prod
    schema: raw_data
    tables:
      - name: products
```

---

## 2. SEEDS - Tasas de Conversión

```csv
fecha,moneda_origen,moneda_destino,tasa
2024-01-01,MXN,USD,0.0590
2024-01-01,EUR,USD,1.1000
```

**Configuración:**
```yaml
# dbt_project.yml
seeds:
  my_project:
    currency_conversion_rates:
      +schema: reference_data
```

---

## 3. Flujo de Modelos (4 modelos - VIEW)

### stg_crm_customers.sql
```sql
{{ config(materialized='view', schema='staging') }}

SELECT
    id_cliente as customer_id,
    TRIM(nombre) as customer_name,
    LOWER(email) as customer_email,
    UPPER(segmento) as customer_segment,
    fecha_registro as registration_date
FROM {{ source('crm_postgres', 'customers') }}
```

### stg_sales_orders.sql
```sql
{{ config(materialized='view', schema='staging') }}

SELECT
    id_pedido as order_id,
    id_cliente_crm as customer_id,
    id_vendedor as sales_rep_id,
    CAST(fecha_pedido AS DATE) as order_date,
    total_pedido as order_amount_local,
    'MXN' as currency_code,
    LOWER(estado) as order_status
FROM {{ source('sales_snowflake', 'orders') }}
WHERE id_pedido IS NOT NULL
```

### stg_hr_employees.sql (futuro)
```sql
{{ config(materialized='view', schema='staging') }}

SELECT
    employee_id as sales_rep_id,
    TRIM(employee_name) as sales_rep_name,
    department
FROM {{ source('hr_system', 'employees') }}
```

---

##  CAPA INTERMEDIA (2 modelos - EPHEMERAL)

### int_orders_with_currency.sql
```sql
{{ config(materialized='ephemeral') }}

SELECT
    o.*,
    c.tasa as conversion_rate,
    o.order_amount_local * c.tasa as order_amount_usd
FROM {{ ref('stg_sales_orders') }} o
LEFT JOIN {{ ref('currency_conversion_rates') }} c
    ON o.order_date = c.fecha
    AND o.currency_code = c.moneda_origen
    AND c.moneda_destino = 'USD'
WHERE o.order_status = 'completed'
```

### int_orders_enriched.sql
```sql
{{ config(materialized='ephemeral') }}

SELECT
    o.order_id,
    o.order_date,
    c.customer_id,
    c.customer_segment,
    e.sales_rep_id,
    e.sales_rep_name,
    o.order_amount_usd
FROM {{ ref('int_orders_with_currency') }} o
INNER JOIN {{ ref('stg_crm_customers') }} c
    ON o.customer_id = c.customer_id
LEFT JOIN {{ ref('stg_hr_employees') }} e
    ON o.sales_rep_id = e.sales_rep_id
WHERE o.order_amount_usd IS NOT NULL
```

---

##  CAPA DE NEGOCIO (MARTS) (1 modelo - INCREMENTAL)

### fct_daily_sales_performance.sql
```sql
{{
    config(
        materialized='incremental',
        unique_key=['fecha_pedido', 'id_vendedor', 'segmento_cliente'],
        incremental_strategy='merge',
        partition_by={'field': 'fecha_pedido', 'data_type': 'date'},
        cluster_by=['id_vendedor', 'segmento_cliente'],
        schema='analytics'
    )
}}

WITH enriched_orders AS (
    SELECT * FROM {{ ref('int_orders_enriched') }}

    {% if is_incremental() %}
        WHERE order_date >= DATE_SUB(
            (SELECT MAX(fecha_pedido) FROM {{ this }}),
            INTERVAL 3 DAY
        )
    {% endif %}
)

SELECT
    order_date as fecha_pedido,
    sales_rep_id as id_vendedor,
    sales_rep_name as nombre_vendedor,
    customer_segment as segmento_cliente,

    SUM(order_amount_usd) as total_ventas_usd_dia,
    COUNT(DISTINCT order_id) as numero_pedidos_dia,
    COUNT(DISTINCT customer_id) as numero_clientes_unicos,
    AVG(order_amount_usd) as ticket_promedio_usd,

    CURRENT_TIMESTAMP() as dbt_updated_at

FROM enriched_orders

GROUP BY 1, 2, 3, 4
```

**Características clave:**
- **Incremental**: Solo procesa datos nuevos
- **Lookback window**: 3 días para late-arriving data
- **Partitioning**: Por fecha (optimiza queries)
- **Clustering**: Por vendedor y segmento (mejora filtros)

---

## 4. EXPOSURES - Documentación de Uso

```yaml
# models/marts/sales/exposures.yml

version: 2

exposures:
  - name: sales_performance_dashboard
    label: "Sales Performance Dashboard"
    type: dashboard
    maturity: high
    url: https://lookerstudio.google.com/reporting/sales-dashboard

    description: |
      Dashboard ejecutivo de performance de ventas diarias.

      Métricas:
      - Total de ventas (USD)
      - Número de órdenes
      - Ticket promedio
      - Top vendedores
      - Performance por segmento

      Audiencia: VP Ventas, Gerentes, Revenue Operations

    depends_on:
      - ref('fct_daily_sales_performance')

    owner:
      name: Daniel G. Hilario
      email: danny.hilario.mgmail.com

    tags: [sales, executive, daily]
```

---

## 5. LINAJE COMPLETO (DAG)

```
Sources:
├─ crm_postgres.customers → stg_crm_customers ─┐
├─ sales_snowflake.orders → stg_sales_orders ──┼→ int_orders_with_currency
├─ hr_system.employees → stg_hr_employees ─────┘         ↓
└─ [seed] currency_conversion_rates ────────────→ int_orders_enriched
                                                           ↓
                                                  fct_daily_sales_performance
                                                           ↓
                                                  [Exposure] Sales Dashboard
```

---

## Comandos de Ejecución:

### Setup inicial:
```bash
dbt deps
dbt seed
dbt build
```

### Producción (diario):
```bash
dbt seed --select currency_conversion_rates
dbt build --select +fct_daily_sales_performance
dbt source freshness
```

### Selectivo:
```bash
# Solo staging
dbt build --select staging.*

# Flujo completo hasta fct
dbt build --select +fct_daily_sales_performance

# Full refresh incremental
dbt build --select fct_daily_sales_performance --full-refresh
```

---

## Estructura de Carpetas:

```
models/
├── staging/
│   ├── _sources.yml
│   ├── crm/
│   │   ├── stg_crm_customers.sql
│   │   └── schema.yml
│   ├── sales/
│   │   ├── stg_sales_orders.sql
│   │   └── schema.yml
│   └── hr/
│       └── stg_hr_employees.sql
│
├── intermediate/
│   └── sales/
│       ├── int_orders_with_currency.sql
│       └── int_orders_enriched.sql
│
└── marts/
    └── sales/
        ├── fct_daily_sales_performance.sql
        ├── schema.yml
        └── exposures.yml
```

---

## Configuración en dbt_project.yml:

```yaml
name: 'sales_analytics'
version: '1.0.0'

models:
  sales_analytics:
    staging:
      +materialized: view
      +schema: staging

    intermediate:
      +materialized: ephemeral

    marts:
      +materialized: table
      +schema: analytics
      sales:
        fct_daily_sales_performance:
          +materialized: incremental
```

---

## Beneficios del Diseño:

1. **Modularidad**: Cambios aislados por capa
2. **Performance**: Staging=VIEW, Intermediate=EPHEMERAL, Marts=INCREMENTAL
3. **Calidad**: Tests en cada capa + freshness monitoring
4. **Documentación**: Sources, modelos y exposures documentados
5. **Reutilización**: Modelos intermedios compartibles
6. **Escalabilidad**: Fácil agregar nuevas fuentes y marts

---

## Tests de Calidad:

```yaml
models:
  - name: fct_daily_sales_performance
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - fecha_pedido
            - id_vendedor
            - segmento_cliente

    columns:
      - name: total_ventas_usd_dia
        tests:
          - not_null
          - dbt_utils.expression_is_true:
              expression: ">= 0"
```

---
