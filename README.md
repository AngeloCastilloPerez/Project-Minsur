# Minsur ETL – Migración Jenkins → Azure

Repositorio oficial del proyecto de migración del pipeline ETL de finanzas de Minsur desde la infraestructura on-premise (Jenkins) hacia Microsoft Azure.

---

## Descripción

El proyecto migra los procesos de extracción, transformación y carga (ETL) de los dominios financieros de Minsur hacia una arquitectura moderna en la nube basada en Azure Data Factory, Azure Databricks y Azure Data Lake Storage Gen2.

Los dominios cubiertos son:

| Dominio | Descripción |
|---------|-------------|
| **G&A** | Gastos Administrativos – extracción desde SAP vía SOAP y SharePoint |
| **CxC** | Cuentas por Cobrar – réplica desde Azure SQL / SAP |
| **Balance General** | Balance General – archivos Excel en SharePoint |
| **Mercado** | Datos de mercado – archivos Excel en SharePoint |

---

## Demo

![Demo del pipeline ETL](docs/assets/demo.gif)

---

## Arquitectura

```
┌─────────────────────────────────────────────────────────────────────┐
│                         FUENTES DE DATOS                            │
│   SAP (SOAP)   │  SharePoint Excel  │  Azure SQL (réplica SAP)      │
└───────┬─────────────────┬──────────────────────┬────────────────────┘
        │                 │                      │
        ▼                 ▼                      ▼
┌──────────────────────────────────────────────────────────────────┐
│              ORQUESTACIÓN – Azure Data Factory (ADF)             │
│  pl_GA_Master  │  pl_CxC_Master  │  pl_Balance_Master │ pl_Mercado_Master │
└──────────────────────────────────────────────────────────────────┘
        │                 │
        ▼                 ▼
┌────────────────────────────────────────────────────────────┐
│           ADLS Gen2 – Arquitectura Medallón                │
│  finanzas-landing  →  finanzas-staging  →  finanzas-curated│
└────────────────────────────────────────────────────────────┘
        │
        ▼
┌────────────────────────────────────────────────────────────┐
│         TRANSFORMACIÓN – Azure Databricks / PySpark        │
│   GA: Homologaciones → Real → Plans → Exportable           │
│   CxC: Real → Exportable                                   │
│   Balance: Real → Exportable                               │
│   Mercado: Real → Exportable                               │
└────────────────────────────────────────────────────────────┘
        │
        ▼
┌────────────────────────────────────────────────────────────┐
│         DESTINO – Azure SQL (BD_FINANZAS)                  │
│  FACT_GA_REAL / FACT_GA_PLAN / FACT_GA_EXPORTABLE          │
│  FACT_CXC_REAL / FACT_CXC_EXPORTABLE                       │
│  FACT_BALANCE_REAL / FACT_BALANCE_EXPORTABLE               │
│  FACT_MERCADO_REAL / FACT_MERCADO_EXPORTABLE               │
└────────────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────┐
│   CONSUMO – Power BI Dashboards │
└─────────────────────────────────┘
```

### Stack Tecnológico

- **Orquestación**: Azure Data Factory (ADF)
- **Transformación**: Azure Databricks con PySpark
- **Almacenamiento**: ADLS Gen2 (arquitectura medallón: landing → staging → curated)
- **Extracción SAP**: Azure Functions (SOAP/zeep)
- **Seguridad**: Azure Key Vault
- **Monitoreo**: Azure Monitor + Log Analytics
- **Destino**: SQL Server / Azure SQL (BD_FINANZAS)
- **Visualización**: Power BI

---

## Estructura del Repositorio

```
Project-Minsur/
├── adf/                          # Pipelines de Azure Data Factory (ARM/JSON)
│   ├── pl_GA_Master.json
│   ├── pl_CxC_Master.json
│   ├── pl_Balance_Master.json
│   └── pl_Mercado_Master.json
├── databricks/                   # Notebooks PySpark por dominio
│   ├── GA/
│   │   ├── nb_GA_01_Homologaciones.py
│   │   ├── nb_GA_02_Real.py
│   │   ├── nb_GA_03_Plans.py
│   │   └── nb_GA_04_Exportable.py
│   ├── CxC/
│   │   ├── nb_CxC_01_Real.py
│   │   └── nb_CxC_02_Exportable.py
│   ├── Balance/
│   │   ├── nb_Balance_01_Real.py
│   │   └── nb_Balance_02_Exportable.py
│   └── Mercado/
│       ├── nb_Mercado_01_Real.py
│       └── nb_Mercado_02_Exportable.py
├── functions/                    # Azure Functions
│   ├── host.json
│   └── sap_gastos/
│       ├── __init__.py
│       ├── function.json
│       └── requirements.txt
├── sql/
│   ├── ddl/                      # Scripts DDL por dominio
│   │   ├── ga_tables.sql
│   │   ├── cxc_tables.sql
│   │   ├── balance_tables.sql
│   │   └── mercado_tables.sql
│   └── validations/              # Queries de validación
│       ├── val_GA.sql
│       ├── val_CxC.sql
│       ├── val_Balance.sql
│       └── val_Mercado.sql
├── infra/                        # Configuraciones de infraestructura
│   ├── keyvault_secrets.json
│   ├── adls_lifecycle_policy.json
│   └── databricks_cluster_config.json
├── docs/                         # Documentación técnica
│   ├── arquitectura_AS-IS.md
│   ├── arquitectura_TO-BE.md
│   ├── flujo_GA.md
│   ├── flujo_CxC.md
│   └── glosario.md
├── requirements.txt
├── .gitignore
└── README.md
```

---

## Arquitectura Medallón (ADLS Gen2)

| Capa | Contenedor | Descripción |
|------|-----------|-------------|
| **Landing** | `finanzas-landing` | Datos crudos tal como llegan de las fuentes (CSV, Excel, JSON) |
| **Staging** | `finanzas-staging` | Datos limpios, homologados, en formato Parquet |
| **Curated** | `finanzas-curated` | Datos finales listos para consumo, particionados por año/mes |

Logs de ejecución: `finanzas-logs`

---

## Setup

### Pre-requisitos

- Azure CLI instalado y autenticado (`az login`)
- Python 3.11+
- Acceso a Azure Key Vault con los secretos configurados (ver `infra/keyvault_secrets.json`)
- Databricks CLI configurado
- ADF con linked services hacia ADLS, Databricks, Key Vault y Azure Functions

### Instalación local

```bash
# Clonar repositorio
git clone https://github.com/minsur/Project-Minsur.git
cd Project-Minsur

# Crear entorno virtual
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate

# Instalar dependencias
pip install -r requirements.txt
```

### Secretos en Key Vault

Configurar los secretos listados en `infra/keyvault_secrets.json` en el Key Vault del entorno correspondiente antes de ejecutar cualquier pipeline.

### Despliegue de ADF Pipelines

```bash
# Publicar pipelines via ARM template desde la rama adf/publish
az deployment group create \
  --resource-group rg-minsur-etl \
  --template-file adf/pl_GA_Master.json
```

### Subir notebooks a Databricks

```bash
databricks workspace import_dir ./databricks /Users/minsur-etl/databricks --overwrite
```

---

## Ejecución

Cada pipeline de ADF acepta los parámetros:

| Parámetro | Descripción |
|-----------|-------------|
| `environment` | Entorno destino: `dev`, `stg`, `prd` |
| `execution_id` | ID único de ejecución (GUID) para trazabilidad |
| `storage_account` | Nombre de la cuenta de almacenamiento ADLS Gen2 |

---

## Documentación adicional

- [Arquitectura AS-IS](docs/arquitectura_AS-IS.md)
- [Arquitectura TO-BE](docs/arquitectura_TO-BE.md)
- [Flujo G&A](docs/flujo_GA.md)
- [Flujo CxC](docs/flujo_CxC.md)
- [Glosario](docs/glosario.md)

---

## Contribución

1. Crear rama desde `develop`: `git checkout -b feature/nombre-feature`
2. Realizar cambios y commits siguiendo la convención: `feat:`, `fix:`, `docs:`
3. Abrir Pull Request hacia `develop`
4. El PR debe pasar las validaciones SQL antes de merge

---

## Contacto

Equipo de Datos – Minsur  

Este proyecto consiste en la migración completa de los pipelines ETL financieros de Minsur S.A. (División Minera) desde una infraestructura on-premise basada en **Jenkins** hacia una arquitectura cloud moderna sobre Microsoft Azure.  Se diseñaron e implementaron 4 arquitecturas de datos independientes
