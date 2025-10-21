# INSTRUCCIONES - Ejercicio Práctico dbt: Bank Marketing

## Índice

1. [Dataset y Transformaciones](#dataset-bank-marketing-45211-registros)
2. [Setup en Ubuntu](#setup-en-ubuntu)
3. [CI/CD con GitHub Actions](#paso-9-cicd-con-github-actions)
4. [Resumen del Proyecto](#resumen-del-proyecto)

---

## DATASET: Bank Marketing (45,211 registros)

**Fuente:** https://archive.ics.uci.edu/dataset/222/bank+marketing
**Ubicación:** `Dataset/bank-full.csv`

### Diccionario de Datos (17 columnas):

**Variables de Cliente:**
- `age`: Edad del cliente (numérico)
- `job`: Tipo de trabajo (categórico: "admin.", "blue-collar", "entrepreneur", "housemaid", "management", "retired", "self-employed", "services", "student", "technician", "unemployed", "unknown")
- `marital`: Estado civil (categórico: "divorced", "married", "single", "unknown")
- `education`: Nivel educativo (categórico: "basic.4y", "basic.6y", "basic.9y", "high.school", "illiterate", "professional.course", "university.degree", "unknown")
- `balance`: Saldo promedio anual en euros (numérico)

**Variables de Préstamos:**
- `default`: ¿Tiene crédito en default? (categórico: "yes", "no", "unknown")
- `housing`: ¿Tiene préstamo de vivienda? (categórico: "yes", "no", "unknown")
- `loan`: ¿Tiene préstamo personal? (categórico: "yes", "no", "unknown")

**Variables de Campaña:**
- `contact`: Tipo de comunicación (categórico: "cellular", "telephone")
- `day`: Último día de contacto del mes (numérico: 1-31)
- `month`: Último mes de contacto del año (categórico: "jan", "feb", "mar", ..., "dec")
- `duration`: Duración del último contacto en segundos (numérico)
- `campaign`: Número de contactos realizados durante esta campaña (numérico)
- `pdays`: Días desde el último contacto de campaña anterior (numérico: -1 = no fue contactado previamente)
- `previous`: Número de contactos antes de esta campaña (numérico)
- `poutcome`: Resultado de la campaña anterior (categórico: "failure", "nonexistent", "success", "unknown")

**Variable Objetivo:**
- `y`: ¿El cliente suscribió un depósito a plazo? (categórico: "yes", "no")

### Transformaciones Necesarias:

**1. Conversión de Tipos:**
- `default`, `housing`, `loan`, `y`: yes/no → boolean (TRUE/FALSE)
- `day`, `month`: Combinar en fecha si es necesario
- `duration`: Convertir a minutos para mejor interpretación

**2. Tratamiento de Valores Especiales:**
- `unknown`: Mapear a NULL en todos los campos categóricos
- `pdays = -1`: Mapear a NULL (indica sin contacto previo)
- Valores negativos en `balance`: Mantener (indica sobregiro)

**3. Filtrado de Registros Irrelevantes:**
- `duration < 60 segundos`: Filtrar contactos con conversación menor a 1 minuto

**Justificación del Filtro:**

Análisis de datos reveló que conversaciones menores a 60 segundos tienen tasa de conversión prácticamente nula:

| Duración | Contactos | Conversiones | Tasa Conversión |
|----------|-----------|--------------|-----------------|
| 0-30 seg | 2,007 | 5 | 0.25% |
| 31-60 seg | 2,756 | 4 | 0.15% |
| 61-120 seg | 9,278 | 202 | 2.18% |
| 121-300 seg | 18,893 | 1,620 | 8.57% |
| 600+ seg | 3,790 | 1,833 | 48.36% |

**Lógica de Negocio:**
- Una propuesta de depósito a plazo requiere al menos 1 minuto de explicación
- Conversaciones < 60 seg representan rechazos inmediatos o contactos fallidos
- Filtro elimina 10.5% de registros pero solo 0.17% de conversiones
- Mejora calidad del análisis sin pérdida significativa de información

**4. KPIs a Calcular:**
- **Tasa de Conversión General**: % de clientes que suscribieron (`y = yes`)
- **Tasa de Conversión por Segmento**: Por job, education, marital, age_group
- **Efectividad de Campaña**: Conversión vs número de contactos (campaign)
- **Duración Óptima**: Correlación entre duration y conversión
- **Efecto de Historial**: Impacto de poutcome en conversión actual
- **Segmentación de Clientes**: Grupos de alto/medio/bajo potencial

---

## SETUP EN UBUNTU

### Paso 1: Sistema y Python 3.11

```bash
# Actualizar sistema
sudo apt update && sudo apt upgrade -y

# Instalar herramientas básicas
sudo apt install -y git curl wget vim build-essential software-properties-common

# Instalar Python 3.11
sudo add-apt-repository ppa:deadsnakes/ppa -y
sudo apt update
sudo apt install -y python3.11 python3.11-venv python3.11-dev
curl -sS https://bootstrap.pypa.io/get-pip.py | python3.11

# Verificar
python3.11 --version  # 3.11.x
```

---

### Paso 2: Configurar Git

```bash
# Configurar identidad
git config --global user.name "Tu Nombre"
git config --global user.email "tu-email@gmail.com"
git config --global credential.helper store

# Verificar
git config --global --list
```

**Autenticación GitHub:**
1. Ve a GitHub → Settings → Developer settings → Personal access tokens → Tokens (classic)
2. Generate new token → Selecciona scope `repo` → Copiar token (ghp_...)
3. Úsalo como password cuando Git te lo pida (NO tu contraseña de GitHub)

---

### Paso 3: Clonar Repositorio

```bash
mkdir -p ~/projects
cd ~/projects
git clone https://github.com/TU_USUARIO/test-entrevista.git
cd test-entrevista
```

---

### Paso 4: Entorno Virtual y dbt

```bash
# Crear entorno virtual
python3.11 -m venv .venv
source .venv/bin/activate

# Instalar dbt-bigquery
pip install --upgrade pip
pip install dbt-bigquery==1.7.4

# Crear requirements.txt
pip freeze > requirements.txt

# Verificar
dbt --version  # Core: 1.7.4, bigquery: 1.7.4
```

---

### Paso 5: Google Cloud SDK y Autenticación

```bash
# Instalar Google Cloud SDK via snap
sudo snap install google-cloud-cli --classic

# Verificar
gcloud --version

# Autenticación con Service Account
gcloud auth activate-service-account --key-file=$HOME/.gcp/dbt-sa-key.json

# Configurar proyecto
gcloud config set project YOUR_PROJECT_ID

# Verificar configuración
gcloud config list
gcloud auth list
```

**Nota:** El archivo JSON del service account debe estar en `~/.gcp/dbt-sa-key.json` con permisos 600.

---

### Paso 6: Crear Proyecto dbt

```bash
cd ~/projects/test-entrevista
source .venv/bin/activate

# Inicializar dbt
dbt init bank_marketing_project

# Responder:
# [1] bigquery
# [1] service_account
# project: YOUR_PROJECT_ID
# dataset: analytics
# threads: 4
# timeout: 300
# [1] US
# keyfile: /home/YOUR_USER/.gcp/dbt-sa-key.json
```

---

### Paso 7: Verificar Conexión

```bash
cd ~/projects/test-entrevista/bank_marketing_project
dbt debug
```

**Debe decir:** `All checks passed!`

---

## Estructura Final

```
~/projects/test-entrevista/
├── .venv/                      # Entorno virtual (no se commitea)
├── .gitignore                  # Ignora .venv, credentials, etc.
├── Dataset/
│   └── bank-full.csv
├── bank_marketing_project/     # Proyecto dbt
│   ├── models/
│   ├── macros/
│   ├── seeds/
│   └── dbt_project.yml
├── requirements.txt
└── Instrucciones.MD
```

---

## Comandos de Uso Diario

```bash
# Activar entorno
cd ~/projects/test-entrevista
source .venv/bin/activate

# Sincronizar con GitHub
git pull

# Trabajar con dbt
cd bank_marketing_project
dbt debug
dbt run
dbt test
```

---

## Paso 8: Cargar Datos y Crear Modelos

```bash
cd ~/projects/test-entrevista/bank_marketing_project
source ../.venv/bin/activate

# Cargar datos raw a BigQuery como seed
dbt seed

# Instalar dependencias de dbt (dbt-utils)
dbt deps

# Ejecutar modelo de staging
dbt run --select staging_bank_marketing

# Ejecutar tests de calidad de datos
dbt test --select staging_bank_marketing
```

**Estructura de modelos:**
- `seeds/raw_bank_marketing.csv`: Datos raw (45,211 registros)
- `models/staging/staging_bank_marketing.sql`: Limpieza y normalización (40,445 registros después del filtro)
- `models/marts/kpi_bank_marketing.sql`: KPIs de conversión por 10 segmentos

---

## Paso 9: CI/CD con GitHub Actions

El proyecto incluye un pipeline automatizado que se ejecuta en cada push a main:

**Archivo:** `.github/workflows/dbt_ci_cd.yml`

**Pasos del pipeline:**
1. Validación de código dbt
2. Ejecución de pruebas unitarias (26 tests total)
3. Despliegue automático a BigQuery
4. Generación de documentación

**Configuración de Secrets (GitHub):**

Necesitas configurar 2 secrets en GitHub (Settings → Secrets and variables → Actions):

1. `DBT_SERVICE_ACCOUNT_KEY`: Contenido del archivo JSON del service account
2. `DBT_PROJECT_ID`: `long-province-475619-v2`

Ver instrucciones detalladas en: `.github/SETUP_SECRETS.md`

**Alertas automáticas:**
- GitHub envía email si el pipeline falla
- Los tests fallidos detienen el despliegue

---

## Resumen del Proyecto

### Arquitectura Implementada

```
┌─────────────────────────────────────────────────────────────────┐
│                      GITHUB REPOSITORY                          │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐         │
│  │   Dataset/   │  │    Models/   │  │  .github/    │         │
│  │ raw_bank_    │  │  - staging   │  │  workflows/  │         │
│  │ marketing    │  │  - marts     │  │  dbt_ci_cd   │         │
│  └──────────────┘  └──────────────┘  └──────────────┘         │
└─────────────────────────────────────────────────────────────────┘
                              ↓
                    ┌─────────────────────┐
                    │  GITHUB ACTIONS     │
                    │  (CI/CD Pipeline)   │
                    │  - Validate         │
                    │  - Test (26 tests)  │
                    │  - Deploy           │
                    └─────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                      GOOGLE BIGQUERY                            │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  analytics (dataset base)                                │  │
│  │    └── raw_bank_marketing (45,211 registros)            │  │
│  ├──────────────────────────────────────────────────────────┤  │
│  │  analytics_staging                                       │  │
│  │    └── staging_bank_marketing (40,445 registros)        │  │
│  ├──────────────────────────────────────────────────────────┤  │
│  │  analytics_marts                                         │  │
│  │    └── kpi_bank_marketing (10 segmentos × N valores)    │  │
│  └──────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

### Datos Procesados

**Pipeline de Datos:**
- **Raw:** 45,211 contactos telefónicos originales
- **Filtro aplicado:** duration >= 60 segundos (conversaciones significativas)
- **Staging:** 40,445 contactos válidos (89.5% del dataset)
- **Conversiones:** 5,280 clientes suscribieron (13.05% tasa de conversión)
- **Registros eliminados:** 4,766 (10.5%) - contactos sin conversación real

### Modelos dbt Implementados

**1. Seeds:**
- `raw_bank_marketing.csv` → Tabla en `analytics.raw_bank_marketing`

**2. Staging Layer:**
- `staging_bank_marketing.sql` → Vista en `analytics_staging.staging_bank_marketing`
- Transformaciones: Limpieza, normalización, creación de campos derivados
- Tests: 15 tests de calidad de datos

**3. Marts Layer:**
- `kpi_bank_marketing.sql` → Tabla en `analytics_marts.kpi_bank_marketing`
- KPIs: 10 segmentos de análisis de conversión
- Tests: 11 tests de validación de métricas

### KPIs Calculados

1. Conversión General (baseline)
2. Conversión por Grupo de Edad (18-29, 30-39, 40-49, 50-59, 60+)
3. Conversión por Ocupación (management, blue-collar, technician, etc.)
4. Conversión por Nivel Educativo
5. Conversión por Estado Civil
6. Conversión por Acumulado en Cuenta (categorías de saldo)
7. Conversión por Intensidad de Campaña (número de contactos)
8. Conversión por Canal (cellular vs telephone)
9. Conversión por Resultado de Campaña Anterior
10. Conversión por Mes (estacionalidad)

**Métrica adicional:** Índice de Efectividad Relativa (comparado con tasa general)

### Tests de Calidad de Datos

**Total: 26 tests automáticos**

**Staging (15 tests):**
- `not_null`: age, account_balance, contact_type, contact_day, contact_month, call_duration_seconds, num_contacts_campaign, num_contacts_previous, subscribed, age_group, balance_category, campaign_intensity
- `accepted_values`: contact_type (cellular/telephone), age_group (5 rangos)
- `accepted_range`: age (18-100)

**Marts (11 tests):**
- `not_null`: segment, segment_value, total_contacts, conversions, conversion_rate_pct, report_generated_at
- `accepted_values`: segment (10 segmentos válidos)
- `accepted_range`: total_contacts (>=1), conversions (>=0), conversion_rate_pct (0-100), relative_effectiveness_index (>=0)

### CI/CD Pipeline

**Herramienta:** GitHub Actions

**Trigger:** Push o Pull Request a branch `main`

**Pasos automatizados:**
1. Setup Python 3.11 y dbt-bigquery
2. Configuración de credenciales desde GitHub Secrets
3. `dbt deps` - Instalación de dependencias (dbt-utils)
4. `dbt debug` - Verificación de conexión a BigQuery
5. `dbt seed --full-refresh` - Carga de datos raw
6. `dbt run` - Ejecución de modelos (staging + marts)
7. `dbt test` - Ejecución de 26 tests de calidad
8. `dbt docs generate` - Generación de documentación
9. Cleanup de credenciales

**Beneficios:**
- Despliegue automático en cada cambio
- Validación de calidad antes de merge
- Alertas por email en caso de fallos
- Historial completo de ejecuciones
- Garantía de código funcional en main

### Decisiones Técnicas Clave

**1. Filtrado de Datos (duration >= 60 seg):**
- Justificación: Análisis exploratorio mostró que conversaciones < 60 seg tienen tasa de conversión casi nula (0.15-0.25%)
- Impacto: Elimina 10.5% de registros pero solo 0.17% de conversiones
- Beneficio: Dataset más limpio para análisis sin pérdida significativa de información

**2. Service Account vs OAuth:**
- Elegido: Service Account
- Justificación: Automatizable para CI/CD, no requiere interacción humana
- Seguridad: Credentials almacenados como GitHub Secrets

**3. Materialización:**
- Staging: `view` (transformaciones ligeras, datos frescos)
- Marts: `table` (agregaciones pesadas, mejor performance para dashboards)

**4. Schemas Separados:**
- `analytics`: Datos raw
- `analytics_staging`: Datos limpios
- `analytics_marts`: KPIs y métricas
- Beneficio: Separación clara de responsabilidades, mejor organización

### Tecnologías Utilizadas

- **Python:** 3.11
- **dbt:** 1.7.4 (dbt-core + dbt-bigquery)
- **BigQuery:** Google Cloud data warehouse
- **GitHub Actions:** CI/CD
- **Ubuntu:** 22.04 LTS (VM de desarrollo)
- **DBeaver:** Cliente SQL para análisis
- **Git:** Control de versiones

### Estructura del Repositorio

```
test-entrevista/
├── .github/
│   ├── workflows/
│   │   └── dbt_ci_cd.yml          # Pipeline CI/CD
│   └── SETUP_SECRETS.md            # Guía de configuración
├── Dataset/
│   └── raw_bank_marketing.csv      # Datos raw (45,211 registros)
├── Teorico/
│   ├── Sec1.MD - Sec5.MD           # Examen teórico dbt
│   └── Apuntes.MD                  # Conceptos fundamentales dbt
├── bank_marketing_project/         # Proyecto dbt
│   ├── models/
│   │   ├── staging/
│   │   │   ├── staging_bank_marketing.sql
│   │   │   └── schema.yml
│   │   └── marts/
│   │       ├── kpi_bank_marketing.sql
│   │       └── schema.yml
│   ├── seeds/
│   │   └── raw_bank_marketing.csv
│   ├── macros/
│   ├── tests/
│   ├── dbt_project.yml
│   └── packages.yml                # Dependencias (dbt-utils)
├── .gitignore
├── requirements.txt                # Python dependencies
├── Instrucciones.MD                # Este archivo
├── InstruccionesDetalle.MD         # Documentación técnica detallada
└── README.MD                       # Enunciado del ejercicio

Archivos no commiteados (en .gitignore):
├── .venv/                          # Entorno virtual Python
└── ~/.dbt/profiles.yml             # Configuración de conexión
└── ~/.gcp/dbt-sa-key.json          # Service account key
```

### Comandos Útiles

```bash
# Activar entorno
cd ~/projects/test-entrevista
source .venv/bin/activate

# Desarrollo local
cd bank_marketing_project
dbt deps                # Instalar dependencias
dbt debug               # Verificar conexión
dbt seed                # Cargar datos raw
dbt run                 # Ejecutar modelos
dbt test                # Ejecutar tests
dbt docs generate       # Generar documentación
dbt docs serve          # Ver documentación en navegador

# Ejecutar modelos específicos
dbt run --select staging_bank_marketing
dbt run --select kpi_bank_marketing
dbt test --select staging_bank_marketing

# Full refresh (recrear todo desde cero)
dbt seed --full-refresh
dbt run --full-refresh
```

### Próximos Pasos (Mejoras Futuras)

1. **Ambientes Múltiples:** Configurar dev, staging, producción
2. **Scheduled Runs:** Ejecución diaria automática del pipeline
3. **dbt Docs Hosting:** Publicar documentación en GitHub Pages
4. **Métricas Avanzadas:** Análisis de cohortes, CLV (Customer Lifetime Value)
5. **Incremental Models:** Optimizar para datasets más grandes
6. **dbt Cloud:** Migrar a dbt Cloud para scheduling y observability
7. **Alerting:** Integración con Slack para notificaciones en tiempo real
8. **Data Quality Monitoring:** Integración con Great Expectations

---

**Proyecto completado:** 2024-10-19
**Autor:** Daniel Hilario
**Versión:** 1.0
**Estado:** Producción - CI/CD Activo
