# SECCIÓN 4: PRUEBAS ESTADÍSTICAS PERSONALIZADAS CON MACROS

## Pregunta 1: Describe el proceso general para crear una prueba personalizada en dbt usando una macro. ¿Qué estructura básica debe tener?

### Proceso en 5 Pasos:

**1. Crear la macro de test en `macros/`**
```sql
-- macros/test_<nombre>.sql
{% test nombre_test(model, column_name, parametros) %}
    SELECT * FROM {{ model }}
    WHERE <condición_que_identifica_registros_malos>
{% endtest %}
```

**2. La query debe retornar registros problemáticos**
- 0 filas = test PASA
- N filas = test FALLA (muestra esos N registros con problemas)

**3. Documentar parámetros y uso**

**4. Invocar en schema.yml**
```yaml
columns:
  - name: columna
    tests:
      - nombre_test:
          parametro: valor
```

**5. Ejecutar con `dbt test`**

### Ejemplo Completo - Test de Valores No Negativos:

```sql
-- macros/test_not_negative.sql

{% test not_negative(model, column_name) %}

    SELECT
        {{ column_name }} as negative_value,
        *
    FROM {{ model }}
    WHERE {{ column_name }} < 0

{% endtest %}
```

### Invocación en schema.yml:

```yaml
version: 2

models:
  - name: fct_orders
    columns:
      - name: order_amount
        description: "Monto de la orden"
        tests:
          - not_null
          - not_negative
```

### Componentes de una Macro de Test:

1. **Nombre**: Descriptivo de qué valida
2. **Parámetros**: `model` (automático), `column_name` (automático), otros opcionales
3. **Query SELECT**: Retorna solo registros que fallan la validación
4. **Filtro WHERE**: Define la condición de fallo

---

## Pregunta 2: Crea una macro `test_column_values_below_std_dev_threshold` que detecte outliers más de N desviaciones estándar por encima del promedio.

### Solución Completa:

```sql
-- macros/test_column_values_below_std_dev_threshold.sql

{% test column_values_below_std_dev_threshold(model, column_name, std_dev_threshold=3) %}

    WITH statistics AS (
        SELECT
            AVG({{ column_name }}) as mean_value,
            STDDEV({{ column_name }}) as std_dev_value
        FROM {{ model }}
        WHERE {{ column_name }} IS NOT NULL
    ),

    threshold_calculation AS (
        SELECT
            mean_value + ({{ std_dev_threshold }} * std_dev_value) as upper_threshold
        FROM statistics
    ),

    outliers AS (
        SELECT
            {{ column_name }} as outlier_value,
            *,
            (SELECT mean_value FROM statistics) as dataset_mean,
            (SELECT std_dev_value FROM statistics) as dataset_std_dev,
            (SELECT upper_threshold FROM threshold_calculation) as allowed_threshold,

            -- Cuántas desviaciones estándar del promedio
            ({{ column_name }} - (SELECT mean_value FROM statistics)) /
            NULLIF((SELECT std_dev_value FROM statistics), 0) as std_devs_from_mean

        FROM {{ model }}

        WHERE
            {{ column_name }} IS NOT NULL
            AND {{ column_name }} > (SELECT upper_threshold FROM threshold_calculation)
    )

    SELECT * FROM outliers

{% endtest %}
```

### Lógica del Test:

**Fórmula:**
```
Umbral superior = μ + (N × σ)

Donde:
- μ (mu) = Promedio de la columna
- σ (sigma) = Desviación estándar
- N = std_dev_threshold (default: 3)

Falla si: valor > μ + (N × σ)
```

**Ejemplo numérico:**
- Promedio = 100
- Std Dev = 20
- Threshold = 3
- Límite superior = 100 + (3 × 20) = 160
- Cualquier valor > 160 es un outlier

### Uso en schema.yml:

```yaml
models:
  - name: fct_orders
    columns:
      - name: order_amount
        description: "Monto de la orden"
        tests:
          - not_null
          - column_values_below_std_dev_threshold  # Usa default: 3

      - name: shipping_cost
        tests:
          - column_values_below_std_dev_threshold:
              std_dev_threshold: 2  # Más estricto
              config:
                severity: warn
                store_failures: true
```

### Contexto Estadístico - Regla 68-95-99.7:

En distribución normal:
- 68.27% de datos dentro de ±1σ
- 95.45% de datos dentro de ±2σ
- 99.73% de datos dentro de ±3σ

**Por qué threshold=3 es común:**
- Captura 99.73% de valores normales
- Solo 0.27% son outliers estadísticos
- Balance entre detectar errores y evitar falsos positivos

### Ejecución:

```bash
# Ejecutar test específico
dbt test --select fct_orders,test_name:column_values_below_std_dev_threshold

# Ver registros fallidos (si store_failures: true)
SELECT * FROM test_failures.column_values_below_std_dev_threshold_fct_orders_order_amount
```

### Aplicación Práctica:

**Casos de uso:**
- Detectar errores de entrada de datos (montos incorrectos)
- Identificar transacciones fraudulentas
- Validar métricas de negocio fuera de rango esperado
- Encontrar tiempos de procesamiento anormales

**Limitaciones:**
- Requiere distribución aproximadamente normal
- No funciona bien con datos bimodales o muy asimétricos
- Sensible a outliers existentes (afectan cálculo de μ y σ)

---
