/*
    Modelo de staging para el dataset de Bank Marketing

    Objetivo: Limpiar y normalizar los datos raw de la campaña de marketing bancario

    Transformaciones aplicadas:
    1. Conversión de valores 'unknown' a NULL para mejor manejo de datos faltantes
    2. Conversión de campos yes/no a tipo booleano para análisis
    3. Creación de campos derivados para segmentación (age_group, balance_category, campaign_intensity)
    4. Conversión de duración de llamada a minutos para mejor interpretabilidad
    5. Manejo del valor especial -1 en pdays (indica que nunca fue contactado previamente)
*/

with source as (
    -- Referencia a la tabla raw cargada desde el seed
    select * from {{ ref('raw_bank_marketing') }}
),

transformed as (
    select
        age,

        -- Convertir 'unknown' a NULL para mejor manejo de valores faltantes
        case when job = 'unknown' then null else job end as job_type,
        case when marital = 'unknown' then null else marital end as marital_status,
        case when education = 'unknown' then null else education end as education_level,

        -- Saldo de cuenta (puede ser negativo indicando sobregiro)
        balance as account_balance_eur,

        -- Convertir valores yes/no a tipo booleano para facilitar análisis.
        -- Se cambia nombre de campo: default -> has_default
        -- Nota: default es palabra reservada, se usa backticks
        case
            when `default` = 'yes' then true
            when `default` = 'no' then false
            else null  -- 'unknown' se convierte a null
        end as has_default,

        -- Se cambia nombre de campo: housing -> has_housing_loan
        case
            when housing = 'yes' then true
            when housing = 'no' then false
            else null
        end as has_housing_loan,

        -- Se cambia nombre de campo: loan -> has_personal_loan
        case
            when loan = 'yes' then true
            when loan = 'no' then false
            else null
        end as has_personal_loan,
    
        case when contact = 'unknown' then null else contact end as contact_type, -- Se cambia nombre de campo: contact -> contact_type
        `day` as contact_day,           -- Se cambia nombre de campo: day -> contact_day (day también es palabra reservada)
        `month` as contact_month,       -- Se cambia nombre de campo: month -> contact_month (month también es palabra reservada)

        -- Convertir duración de segundos a minutos para mejor interpretación
        -- Redondeado a 2 decimales
        round(duration / 60.0, 2) as call_duration_minutes,

        -- Mantener campo original en segundos
        -- Se cambia nombre de campo: duration -> call_duration_seconds
        duration as call_duration_seconds,

        -- Número de contactos en esta campaña
        -- Se cambia nombre de campo: campaign -> call_duration_num_contacts_campaign
        campaign as num_contacts_campaign,

        -- Manejar valor especial -1 en pdays
        -- -1 significa que el cliente nunca fue contactado en campañas previas
        -- Lo convertimos a NULL para indicar "no aplica"
        -- Se cambia nombre de campo: pdays -> days_since_last_contact
        case when pdays = -1 then null else pdays end as days_since_last_contact,

        -- Número de contactos en campañas anteriores
        previous as num_contacts_previous,

        -- Resultado de campaña anterior ('unknown' se convierte a null)
        case when poutcome = 'unknown' then null else poutcome end as previous_outcome,

        -- ====================================
        -- VARIABLE OBJETIVO (TARGET)
        -- ====================================
        -- Convertir yes/no a booleano
        -- TRUE = cliente suscribió depósito a plazo
        -- FALSE = cliente no suscribió
        case
            when y = 'yes' then true
            when y = 'no' then false
            else null
        end as subscribed,

        -- ====================================
        -- CAMPOS DERIVADOS PARA SEGMENTACIÓN
        -- ====================================

        -- Crear grupos etarios para análisis de conversión por edad
        case
            when age < 30 then '18-29'
            when age < 40 then '30-39'
            when age < 50 then '40-49'
            when age < 60 then '50-59'
            else '60+'
        end as age_group,

        -- Categorizar saldo de cuenta para identificar segmentos de clientes
        -- Negativo indica sobregiro (mayor riesgo)
        -- Valores altos indican mayor capacidad de ahorro
        case
            when balance < 0 then 'Negative'
            when balance = 0 then 'Zero'
            when balance <= 1000 then 'Low (1-1K)'
            when balance <= 5000 then 'Medium (1K-5K)'
            when balance <= 10000 then 'High (5K-10K)'
            else 'Very High (10K+)'
        end as balance_category,

        -- Clasificar intensidad de contactos
        -- Ayuda a identificar punto de rendimiento decreciente
        -- (demasiados contactos pueden molestar al cliente)
        case
            when campaign = 1 then 'Single Contact'
            when campaign <= 3 then 'Low (2-3)'
            when campaign <= 5 then 'Medium (4-5)'
            else 'High (6+)'
        end as campaign_intensity

    from source

    -- ====================================
    -- FILTRADO DE REGISTROS IRRELEVANTES
    -- ====================================
    -- Filtrar contactos con duración menor a 60 segundos
    --
    -- Justificación de negocio:
    -- - Conversaciones < 60 seg tienen tasa de conversión casi nula (0.15-0.25%)
    -- - Estos contactos representan rechazos inmediatos, números equivocados,
    --   o clientes que cuelgan antes de escuchar la propuesta completa
    -- - No aportan valor para análisis de efectividad de campaña
    -- - Una propuesta de depósito a plazo requiere explicación mínima (~1 minuto)
    --
    -- Impacto del filtro:
    -- - Elimina: 4,766 registros (10.5% del total)
    -- - Conversiones perdidas: 9 de 5,289 (0.17% de conversiones totales)
    -- - Mejora calidad del dataset para KPIs sin pérdida significativa de información
    where duration >= 60
)

-- Retornar todos los registros transformados y filtrados
select * from transformed