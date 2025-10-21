#!/bin/bash

# Script de instalación automatizada para test-analytics-engineering
# Autor: Daniel Hilario
# Fecha: 2025-10-20

set -e  # Detener en caso de error

# Colores para output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}  Instalación de test-analytics-engineering${NC}"
echo -e "${GREEN}========================================${NC}\n"

# Verificar que estamos en el directorio correcto
if [ ! -f "README.MD" ]; then
    echo -e "${RED}Error: Debes ejecutar este script desde el directorio raíz del proyecto${NC}"
    exit 1
fi

# Verificar variables de entorno requeridas
if [ -z "$DBT_PROJECT_ID" ]; then
    echo -e "${RED}Error: Variable DBT_PROJECT_ID no está configurada${NC}"
    echo -e "${YELLOW}Ejecuta: export DBT_PROJECT_ID='your-project-id'${NC}"
    exit 1
fi

if [ -z "$DBT_KEYFILE_PATH" ]; then
    echo -e "${YELLOW}Warning: DBT_KEYFILE_PATH no configurada, usando default: ~/.gcp/dbt-sa-key.json${NC}"
    export DBT_KEYFILE_PATH="$HOME/.gcp/dbt-sa-key.json"
fi

# Verificar que existe el keyfile
if [ ! -f "$DBT_KEYFILE_PATH" ]; then
    echo -e "${RED}Error: Archivo de credenciales no encontrado: $DBT_KEYFILE_PATH${NC}"
    echo -e "${YELLOW}Copia tu archivo dbt-sa-key.json a la ubicación especificada${NC}"
    exit 1
fi

# 1. Verificar Python 3.11
echo -e "${GREEN}[1/8]${NC} Verificando Python 3.11..."
if ! command -v python3.11 &> /dev/null; then
    echo -e "${YELLOW}Python 3.11 no encontrado. Instalando...${NC}"
    sudo add-apt-repository ppa:deadsnakes/ppa -y
    sudo apt update
    sudo apt install -y python3.11 python3.11-venv python3.11-dev
    curl -sS https://bootstrap.pypa.io/get-pip.py | python3.11
else
    echo -e "${GREEN}✓ Python 3.11 instalado${NC}"
fi

# 2. Verificar Google Cloud SDK
echo -e "\n${GREEN}[2/8]${NC} Verificando Google Cloud SDK..."
if ! command -v gcloud &> /dev/null; then
    echo -e "${YELLOW}Google Cloud SDK no encontrado. Instalando...${NC}"
    sudo snap install google-cloud-cli --classic
else
    echo -e "${GREEN}✓ Google Cloud SDK instalado${NC}"
fi

# 3. Autenticar con Google Cloud
echo -e "\n${GREEN}[3/8]${NC} Configurando autenticación Google Cloud..."
gcloud auth activate-service-account --key-file="$DBT_KEYFILE_PATH"
gcloud config set project "$DBT_PROJECT_ID"
echo -e "${GREEN}✓ Autenticación configurada${NC}"

# 4. Crear entorno virtual
echo -e "\n${GREEN}[4/8]${NC} Creando entorno virtual..."
if [ -d ".venv" ]; then
    echo -e "${YELLOW}Entorno virtual ya existe, omitiendo...${NC}"
else
    python3.11 -m venv .venv
    echo -e "${GREEN}✓ Entorno virtual creado${NC}"
fi

# 5. Activar entorno virtual e instalar dependencias
echo -e "\n${GREEN}[5/8]${NC} Instalando dependencias Python..."
source .venv/bin/activate
pip install --upgrade pip --quiet
pip install -r requirements.txt --quiet
echo -e "${GREEN}✓ Dependencias instaladas${NC}"

# 6. Configurar profiles.yml
echo -e "\n${GREEN}[6/8]${NC} Configurando dbt profiles.yml..."
mkdir -p ~/.dbt

# Crear profiles.yml con las variables de entorno
cat > ~/.dbt/profiles.yml << EOF
bank_marketing_project:
  outputs:
    dev:
      dataset: analytics
      job_execution_timeout_seconds: 300
      job_retries: 1
      keyfile: $DBT_KEYFILE_PATH
      location: US
      method: service-account
      priority: interactive
      project: $DBT_PROJECT_ID
      threads: 4
      type: bigquery
  target: dev
EOF

chmod 600 ~/.dbt/profiles.yml
echo -e "${GREEN}✓ profiles.yml configurado${NC}"

# 7. Verificar conexión dbt
echo -e "\n${GREEN}[7/8]${NC} Verificando conexión dbt..."
cd bank_marketing_project
dbt deps
if dbt debug | grep -q "All checks passed"; then
    echo -e "${GREEN}✓ Conexión dbt exitosa${NC}"
else
    echo -e "${RED}Error: dbt debug falló${NC}"
    dbt debug
    exit 1
fi

# 8. Cargar datos iniciales (opcional)
echo -e "\n${GREEN}[8/8]${NC} ¿Deseas cargar los datos y ejecutar los modelos ahora? (s/n)"
read -r response
if [[ "$response" =~ ^[Ss]$ ]]; then
    echo -e "${YELLOW}Cargando datos...${NC}"
    dbt seed --full-refresh

    echo -e "${YELLOW}Ejecutando modelos...${NC}"
    dbt run

    echo -e "${YELLOW}Ejecutando tests...${NC}"
    dbt test

    echo -e "${GREEN}✓ Datos cargados y modelos ejecutados${NC}"
else
    echo -e "${YELLOW}Puedes cargar los datos más tarde con:${NC}"
    echo -e "  cd bank_marketing_project"
    echo -e "  dbt seed"
    echo -e "  dbt run"
    echo -e "  dbt test"
fi

# Resumen final
echo -e "\n${GREEN}========================================${NC}"
echo -e "${GREEN}  ✓ Instalación completada exitosamente${NC}"
echo -e "${GREEN}========================================${NC}\n"

echo -e "Para trabajar con el proyecto:"
echo -e "  ${YELLOW}cd $(pwd)${NC}"
echo -e "  ${YELLOW}source .venv/bin/activate${NC}"
echo -e "  ${YELLOW}cd bank_marketing_project${NC}"
echo -e "  ${YELLOW}dbt run${NC}\n"

echo -e "Variables de entorno configuradas:"
echo -e "  ${YELLOW}DBT_PROJECT_ID=${NC}$DBT_PROJECT_ID"
echo -e "  ${YELLOW}DBT_KEYFILE_PATH=${NC}$DBT_KEYFILE_PATH\n"
