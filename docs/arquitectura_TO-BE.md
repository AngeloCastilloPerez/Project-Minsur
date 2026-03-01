# Arquitectura TO-BE: Pipeline ETL en Azure

## Descripción General

La arquitectura destino (TO-BE) migra todos los procesos ETL de finanzas de Minsur a Microsoft Azure, utilizando servicios PaaS nativos que proveen escalabilidad automática, monitoreo integrado, seguridad por diseño y separación de ambientes (dev/stg/prd).

---

## Servicios de Azure Involucrados

| Servicio | Rol | SKU Recomendado |
|---------|-----|----------------|
| **Azure Data Factory** | Orquestación de pipelines ETL | Standard |
| **Azure Databricks** | Transformación con PySpark | Premium (Unity Catalog) |
| **ADLS Gen2** | Almacenamiento medallón (landing/staging/curated) | StorageV2 LRS |
| **Azure Functions** | Extracción SAP SOAP | Consumption Plan |
| **Azure Key Vault** | Gestión de secretos y credenciales | Standard |
| **Azure SQL Database** | Destino final (BD_FINANZAS) | General Purpose 4 vCores |
| **Azure Monitor** | Monitoreo, alertas y dashboards operativos | Log Analytics Workspace |
| **Application Insights** | Telemetría de Azure Functions | Vinculado a Functions |
| **Power BI Premium** | Visualización con Direct Lake | P1 |
| **Azure DevOps** | CI/CD de pipelines y notebooks | Basic |

---

## Flujo de Datos (TO-BE)

```
┌─────────────────────────────────────────────────────────────────┐
│                     FUENTES DE DATOS                            │
│                                                                 │
│  SAP (SOAP RFC)        SharePoint Online       Azure SQL        │
│  ZmfCoSAlr87013613     Excel en Biblioteca     (Réplica SAP)    │
└────────┬───────────────────────┬──────────────────┬────────────┘
         │                       │                  │
         ▼                       ▼                  │
┌─────────────────┐   ┌──────────────────────┐     │
│  Azure Function  │   │    ADF Copy Activity  │◄───┘
│  sap_gastos      │   │  (SharePoint → ADLS)  │
│  (HTTP trigger)  │   └──────────┬───────────┘
└────────┬────────┘              │
         │                       │
         ▼                       ▼
┌─────────────────────────────────────────────────────────────────┐
│              ADLS Gen2 – Capa Landing (finanzas-landing)        │
│   /ga/real/     /ga/plan/    /cxc/    /balance/    /mercado/    │
└────────────────────────────────┬────────────────────────────────┘
                                  │
                    ADF invoca notebooks Databricks
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────┐
│          Azure Databricks – Transformación PySpark              │
│                                                                 │
│  GA: Homologaciones → Real → Plans → Exportable                │
│  CxC: Real → Exportable                                        │
│  Balance: Real → Exportable                                    │
│  Mercado: Real → Exportable                                    │
└────────────┬────────────────────────────────────┬──────────────┘
             │                                    │
             ▼                                    ▼
┌──────────────────────────────┐    ┌─────────────────────────────┐
│  ADLS Gen2 – Staging          │    │  ADLS Gen2 – Curated        │
│  (finanzas-staging)           │    │  (finanzas-curated)         │
│  Parquet, homologado          │    │  Parquet, particionado      │
│  por año/mes                  │    │  año/mes, listo consumo     │
└──────────────────────────────┘    └──────────────┬──────────────┘
                                                    │
                                                    ▼
                                    ┌─────────────────────────────┐
                                    │   Azure SQL – BD_FINANZAS   │
                                    │   FACT_GA_REAL              │
                                    │   FACT_GA_PLAN              │
                                    │   FACT_GA_EXPORTABLE        │
                                    │   FACT_CXC_REAL             │
                                    │   FACT_CXC_EXPORTABLE       │
                                    │   FACT_BALANCE_REAL         │
                                    │   FACT_MERCADO_REAL         │
                                    │   ... (etc.)                │
                                    └──────────────┬──────────────┘
                                                    │
                                                    ▼
                                    ┌─────────────────────────────┐
                                    │     Power BI Premium        │
                                    │  Dashboards Financieros     │
                                    │  (Direct Lake / Import)     │
                                    └─────────────────────────────┘
```

---

## Ambientes

| Ambiente | Prefijo de recursos | Propósito |
|---------|-------------------|----------|
| **dev** | `minsur-dev-*` | Desarrollo y pruebas unitarias |
| **stg** | `minsur-stg-*` | Validación y pruebas de integración |
| **prd** | `minsur-prd-*` | Producción |

Cada ambiente tiene su propio:
- Key Vault (`kv-minsur-dev`, `kv-minsur-stg`, `kv-minsur-prd`)
- ADLS Gen2 Storage Account
- Azure SQL Database
- Databricks Workspace
- ADF Instance

---

## Estrategia de Seguridad

- **Managed Identity** para comunicación entre servicios Azure (ADF → Databricks, Functions → ADLS)
- **Key Vault References** en Azure Functions App Settings (sin credenciales en código)
- **Databricks Secret Scope** backed por Key Vault para notebooks
- **Private Endpoints** para Azure SQL y ADLS en producción
- **Network Security Groups** en las subredes de Databricks
- **Audit Logging** habilitado en Azure SQL y Key Vault

---

## Mejoras Respecto al AS-IS

| Dimensión | AS-IS | TO-BE |
|----------|-------|-------|
| Escalabilidad | Servidor fijo 4CPU/16GB | Autoscale 2-8 nodos Databricks |
| SLA de procesamiento | 8 horas | < 2 horas |
| Retry automático | Manual | ADF retry policy configurable |
| Monitoreo | Logs en archivos locales | Azure Monitor + Application Insights |
| Seguridad de credenciales | `.env` en servidor | Azure Key Vault |
| CI/CD | Manual (FTP) | Azure DevOps Pipelines |
| Linaje de datos | Sin trazabilidad | execution_id en todas las tablas |
| Ambientes | Solo producción | dev / stg / prd |
| Disponibilidad objetivo | 97% | 99.9% (SLA Azure) |

---

## Beneficios de la Migración

1. **Reducción de tiempo de procesamiento**: de 8h a <2h gracias a paralelismo en Databricks
2. **Eliminación de servidores on-premise**: reducción de costos de infraestructura y mantenimiento
3. **Seguridad mejorada**: credenciales en Key Vault, nunca en código o archivos planos
4. **Observabilidad completa**: alertas proactivas, dashboards operativos en Azure Monitor
5. **Escalabilidad elástica**: recursos ajustados automáticamente según carga
6. **Gobierno de datos**: arquitectura medallón con linaje completo por execution_id
7. **Despliegue automatizado**: CI/CD con Azure DevOps, sin intervención manual
