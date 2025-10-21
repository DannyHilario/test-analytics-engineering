/*
    Modelo de KPIs para análisis de efectividad de campaña de marketing bancario

    Objetivo: Calcular métricas clave para evaluar el desempeño de la campaña
    y proporcionar insights accionables para el equipo de marketing

    KPIs incluidos:
    1. Tasa de conversión general y por segmento
    2. Efectividad de campaña (conversión vs número de contactos)
    3. Impacto del historial previo en conversión
    4. Segmentación de clientes por potencial

    Materialización: table (para mejor performance en dashboards)
*/

with staging as (
    select * from {{ ref('staging_bank_marketing') }}
),

-- ====================================
-- KPI 1: CONVERSIÓN GENERAL
-- ====================================
conversion_general as (
    select
        'General' as segment,
        'Todos' as segment_value,
        count(*) as total_contacts,
        countif(subscribed) as conversions,
        round(countif(subscribed) * 100.0 / count(*), 2) as conversion_rate_pct
    from staging
),

-- ====================================
-- KPI 2: CONVERSIÓN POR GRUPO DE EDAD
-- ====================================
conversion_por_edad as (
    select
        'Grupo de Edad' as segment,
        age_group as segment_value,
        count(*) as total_contacts,
        countif(subscribed) as conversions,
        round(countif(subscribed) * 100.0 / count(*), 2) as conversion_rate_pct
    from staging
    group by age_group
),

-- ====================================
-- KPI 3: CONVERSIÓN POR OCUPACIÓN
-- ====================================
conversion_por_ocupacion as (
    select
        'Ocupación' as segment,
        coalesce(job_type, 'Desconocido') as segment_value,
        count(*) as total_contacts,
        countif(subscribed) as conversions,
        round(countif(subscribed) * 100.0 / count(*), 2) as conversion_rate_pct
    from staging
    group by job_type
),

-- ====================================
-- KPI 4: CONVERSIÓN POR NIVEL EDUCATIVO
-- ====================================
conversion_por_educacion as (
    select
        'Nivel Educativo' as segment,
        coalesce(education_level, 'Desconocido') as segment_value,
        count(*) as total_contacts,
        countif(subscribed) as conversions,
        round(countif(subscribed) * 100.0 / count(*), 2) as conversion_rate_pct
    from staging
    group by education_level
),

-- ====================================
-- KPI 5: CONVERSIÓN POR ESTADO CIVIL
-- ====================================
conversion_por_estado_civil as (
    select
        'Estado Civil' as segment,
        coalesce(marital_status, 'Desconocido') as segment_value,
        count(*) as total_contacts,
        countif(subscribed) as conversions,
        round(countif(subscribed) * 100.0 / count(*), 2) as conversion_rate_pct
    from staging
    group by marital_status
),

-- ====================================
-- KPI 6: CONVERSIÓN POR ACUMULADO EN CUENTA
-- ====================================
conversion_por_saldo as (
    select
        'Acumulado en Cuenta' as segment,
        balance_category as segment_value,
        count(*) as total_contacts,
        countif(subscribed) as conversions,
        round(countif(subscribed) * 100.0 / count(*), 2) as conversion_rate_pct
    from staging
    group by balance_category
),

-- ====================================
-- KPI 7: EFECTIVIDAD DE CAMPAÑA (INTENSIDAD DE CONTACTOS)
-- ====================================
conversion_por_intensidad_campana as (
    select
        'Intensidad de Campaña' as segment,
        campaign_intensity as segment_value,
        count(*) as total_contacts,
        countif(subscribed) as conversions,
        round(countif(subscribed) * 100.0 / count(*), 2) as conversion_rate_pct
    from staging
    group by campaign_intensity
),

-- ====================================
-- KPI 8: CONVERSIÓN POR CANAL
-- ====================================
conversion_por_canal as (
    select
        'Canal' as segment,
        coalesce(contact_type, 'Desconocido') as segment_value,
        count(*) as total_contacts,
        countif(subscribed) as conversions,
        round(countif(subscribed) * 100.0 / count(*), 2) as conversion_rate_pct
    from staging
    group by contact_type
),

-- ====================================
-- KPI 9: CONVERSIÓN POR RESULTADO DE CAMPAÑA ANTERIOR
-- ====================================
conversion_por_campana_anterior as (
    select
        'Resultado Campaña Anterior' as segment,
        coalesce(previous_outcome, 'Sin Campaña Previa') as segment_value,
        count(*) as total_contacts,
        countif(subscribed) as conversions,
        round(countif(subscribed) * 100.0 / count(*), 2) as conversion_rate_pct
    from staging
    group by previous_outcome
),

-- ====================================
-- KPI 10: CONVERSIÓN POR MES
-- ====================================
conversion_por_mes as (
    select
        'Mes de Contacto' as segment,
        contact_month as segment_value,
        count(*) as total_contacts,
        countif(subscribed) as conversions,
        round(countif(subscribed) * 100.0 / count(*), 2) as conversion_rate_pct
    from staging
    group by contact_month
),

-- ====================================
-- UNIÓN DE TODOS LOS KPIs
-- ====================================
todos_los_kpis as (
    select segment, segment_value, total_contacts, conversions, conversion_rate_pct
    from conversion_general

    union all
    select segment, segment_value, total_contacts, conversions, conversion_rate_pct
    from conversion_por_edad

    union all
    select segment, segment_value, total_contacts, conversions, conversion_rate_pct
    from conversion_por_ocupacion

    union all
    select segment, segment_value, total_contacts, conversions, conversion_rate_pct
    from conversion_por_educacion

    union all
    select segment, segment_value, total_contacts, conversions, conversion_rate_pct
    from conversion_por_estado_civil

    union all
    select segment, segment_value, total_contacts, conversions, conversion_rate_pct
    from conversion_por_saldo

    union all
    select segment, segment_value, total_contacts, conversions, conversion_rate_pct
    from conversion_por_intensidad_campana

    union all
    select segment, segment_value, total_contacts, conversions, conversion_rate_pct
    from conversion_por_canal

    union all
    select segment, segment_value, total_contacts, conversions, conversion_rate_pct
    from conversion_por_campana_anterior

    union all
    select segment, segment_value, total_contacts, conversions, conversion_rate_pct
    from conversion_por_mes
)

-- Resultado final con métricas calculadas
select
    segment,
    segment_value,
    total_contacts,
    conversions,
    conversion_rate_pct,

    -- Calcular índice de efectividad relativa (vs tasa general)
    round(
        conversion_rate_pct / nullif(
            (select conversion_rate_pct from conversion_general), 0
        ), 2
    ) as relative_effectiveness_index,

    -- Timestamp de generación del reporte
    current_timestamp() as report_generated_at

from todos_los_kpis
order by segment, conversion_rate_pct desc
