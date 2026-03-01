# Flujo de Datos: G&A (Gastos Administrativos)

## Descripción General

El dominio G&A (Gastos Administrativos) procesa datos de gastos operativos extraídos de SAP y archivos de presupuesto desde SharePoint, los transforma mediante la arquitectura medallón en ADLS Gen2, y los carga en el modelo estrella de BD_FINANZAS para consumo en Power BI.

---

## Fuentes de Datos

| Fuente | Tipo | Descripción |
|-------|------|-------------|
| SAP SOAP RFC `ZmfCoSAlr87013613` | Web Service SOAP | Datos reales de gastos por centro de costo y cuenta GL |
| SharePoint Online – Biblioteca "Presupuestos" | Excel (.xlsx) | Plan/presupuesto anual desglosado por mes, CC y GL |
| SharePoint Online – Biblioteca "Homologaciones" | CSV (.csv) | Tablas de mapeo: CC SAP → CC Local, GL SAP → GL Local |

---

## Flujo End-to-End

```
[1. EXTRACCIÓN]
    Azure Function sap_gastos (HTTP POST trigger)
    ├── Recibe: fiscal_year, period, company_code, execution_id
    ├── Llama: SAP SOAP ZmfCoSAlr87013613
    └── Escribe: JSON → finanzas-landing/ga/real/execution_id={id}/

    ADF CopyActivity (SharePoint → ADLS)
    ├── Lee: Excel Plan GA de SharePoint
    └── Escribe: .xlsx → finanzas-landing/ga/plan/execution_id={id}/

    ADF CopyActivity (SharePoint → ADLS)
    ├── Lee: CSV homologaciones de SharePoint
    └── Escribe: .csv → finanzas-landing/ga/reference/

          │
          ▼
[2. HOMOLOGACIÓN – nb_GA_01_Homologaciones.py]
    Lee: landing/ga/real/ (JSON SAP) + landing/ga/reference/ (CSVs)
    Procesa:
    ├── Estandariza columnas (BUKRS, KOSTL, SAKNR → EMPRESA, CC, GL)
    ├── Join con homologacion_centros_costo.csv (CC SAP → CC Local)
    ├── Join con homologacion_cuentas_gl.csv (GL SAP → GL Local)
    ├── Flags: FLAG_HOMOLOGADO_CC, FLAG_HOMOLOGADO_GL
    └── Escribe: Parquet → finanzas-staging/ga/homologaciones/

          │
          ▼ (paralelo)
[3A. REAL – nb_GA_02_Real.py]          [3B. PLAN – nb_GA_03_Plans.py]
    Lee: staging/ga/homologaciones/         Lee: landing/ga/plan/ (Excel)
    Filtra: FLAG_HOMOLOGADO=1               Aplica homologaciones del staging
    Transforma:                              Estandariza columnas de plan
    ├── Redondeo montos                     ├── MONTO_ML_PLAN, MONTO_USD_PLAN
    ├── Filtro empresas válidas              └── Escribe:
    └── Escribe:                                 staging/ga/plan/
         curated/ga/real/ (Parquet)              curated/ga/plan/ (Parquet)
         FACT_GA_REAL (pyodbc ACID)              FACT_GA_PLAN (pyodbc ACID)

          │
          ▼
[4. EXPORTABLE – nb_GA_04_Exportable.py]
    Lee: curated/ga/real/ + curated/ga/plan/
    Procesa:
    ├── Agrupa por CC + GL + período
    ├── Full join Real vs Plan
    ├── Calcula varianza: VAR_ML, VAR_USD, VAR_PCT_ML
    └── Escribe:
         curated/ga/exportable/ (Parquet)
         FACT_GA_EXPORTABLE (pyodbc ACID)
```

---

## Orquestación ADF (pl_GA_Master)

```
GetMetadataSharePoint
        │
        ▼ (Succeeded)
CallAzureFunctionSAPSOAP
        │
        ▼ (Succeeded)
RunDatabricksHomologaciones
        │
   ┌────┴────┐
   ▼         ▼
RunDatabricks  RunDatabricks
Real           Plans
   │         │
   └────┬────┘
        ▼ (both Succeeded)
RunDatabricksExportable
```

---

## Diccionario de Datos

### Campos clave en FACT_GA_REAL / FACT_GA_PLAN / FACT_GA_EXPORTABLE

| Campo | Tipo | Descripción | Origen SAP |
|-------|------|-------------|-----------|
| `EMPRESA` | VARCHAR(4) | Código de empresa SAP (e.g., `1000` = Minsur S.A.) | `BUKRS` |
| `CENTRO_COSTO_SAP` | VARCHAR(10) | Centro de costo SAP (5 dígitos, izquierda llenado con 0) | `KOSTL` |
| `CC_LOCAL` | VARCHAR(20) | Código de centro de costo en nomenclatura local Minsur | Homologación |
| `CC_GERENCIA` | VARCHAR(100) | Gerencia responsable del centro de costo | Homologación |
| `CC_AREA` | VARCHAR(100) | Área dentro de la gerencia | Homologación |
| `CUENTA_GL_SAP` | VARCHAR(10) | Cuenta de mayor general SAP | `SAKNR` |
| `GL_LOCAL` | VARCHAR(20) | Código de cuenta GL en nomenclatura local | Homologación |
| `GL_TIPO_GASTO` | VARCHAR(50) | Tipo de gasto: PERSONAL, SERVICIOS, MATERIALES, DEPRECIACION | Homologación |
| `GL_CLASIFICACION` | VARCHAR(50) | Clasificación: FIJO, VARIABLE, SEMIVARIABLE | Homologación |
| `ANIO_FISCAL` | SMALLINT | Año fiscal SAP (e.g., `2024`) | `GJAHR` |
| `PERIODO` | TINYINT | Período contable 1-12 (meses) | `MONAT` |
| `PERIODO_LABEL` | VARCHAR(7) | Etiqueta de período: `2024-03` | Calculado |
| `MONTO_ML` | DECIMAL(18,2) | Monto en moneda local (Soles PEN) | `WKGBTR` |
| `MONTO_USD` | DECIMAL(18,2) | Monto en dólares USD | `WTGBTR` |
| `MONTO_ML_PLAN` | DECIMAL(18,2) | Monto planificado en moneda local | Excel SharePoint |
| `VAR_ML` | DECIMAL(18,2) | Varianza Real - Plan en moneda local | Calculado |
| `VAR_PCT_ML` | DECIMAL(10,2) | Varianza porcentual (Real-Plan)/Plan * 100 | Calculado |
| `EXECUTION_ID` | VARCHAR(100) | ID único de la ejecución del pipeline (GUID o timestamp) | ADF |
| `FECHA_PROCESO` | DATETIME2 | Timestamp UTC de procesamiento | Sistema |

---

## Parámetros del Pipeline

| Parámetro | Descripción | Ejemplo |
|----------|-------------|---------|
| `fiscal_year` | Año fiscal a procesar | `2024` |
| `period` | Período (mes) a procesar | `3` |
| `company_code` | Código de empresa SAP | `1000` |
| `environment` | Entorno: dev/stg/prd | `prd` |
| `execution_id` | GUID único de ejecución | `20240301120000` |
| `storage_account` | Nombre ADLS Gen2 | `stminsurprd` |

---

## Consideraciones de Negocio

- Los datos G&A se procesan mensualmente, típicamente el **1er día hábil** de cada mes para el período anterior
- La homologación de CC y GL puede cambiar; la tabla `LINK_TABLE_GA_HOMOLOGACION` registra la vigencia por fecha
- Los centros de costo sin homologación se registran con `FLAG_HOMOLOGADO_CC = 0` y se excluyen de FACT_GA_REAL pero se registran en logs para revisión
- El plan se carga desde un Excel preparado por el equipo de Presupuestos de Finanzas y subido a SharePoint antes del cierre mensual
