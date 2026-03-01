# Flujo de Datos: CxC (Cuentas por Cobrar)

## Descripción General

El dominio CxC procesa datos de cuentas por cobrar extraídos de la réplica Azure SQL de SAP, aplica transformaciones de envejecimiento (aging), calcula estados de documentos y genera datasets para análisis en Power BI.

---

## Fuentes de Datos

| Fuente | Tipo | Descripción |
|-------|------|-------------|
| Azure SQL – tabla `EXT_CXC_REAL` (réplica SAP) | SQL Server | Partidas abiertas y compensadas de deudores (tipo cuenta `D`) |
| SAP FI-AR | Sistema origen | Tablas `BSID` (abiertas) y `BSAD` (compensadas) replicadas vía Azure Data Factory |

---

## Flujo End-to-End

```
[1. EXTRACCIÓN – ADF CopyActivity]
    Lee: dbo.EXT_CXC_REAL WHERE FECHA_ACTUALIZACION >= CAST(GETDATE()-1 AS DATE)
    Escribe: Parquet → finanzas-landing/cxc/real/execution_id={id}/

          │
          ▼
[2. TRANSFORMACIÓN REAL – nb_CxC_01_Real.py]
    Lee: landing/cxc/real/
    Procesa:
    ├── Estandariza columnas (BUKRS, KUNNR, BELNR → EMPRESA, CLIENTE, DOCUMENTO)
    ├── Convierte fechas de yyyyMMdd a DATE
    ├── Calcula DIAS_VENCIDO (hoy - FECHA_VENCIMIENTO si no compensado)
    ├── Clasifica ESTADO: ABIERTO / COMPENSADO
    ├── Asigna BUCKET_VENCIMIENTO: VIGENTE / 1-30 / 31-60 / 61-90 / +90
    ├── Filtra TIPO_CUENTA = 'D' (solo deudores)
    ├── Escribe: staging/cxc/real/ (Parquet)
    └── Escribe: curated/cxc/real/ (Parquet) + FACT_CXC_REAL (pyodbc ACID)

          │
          ▼
[3. EXPORTABLE – nb_CxC_02_Exportable.py]
    Lee: curated/cxc/real/
    Procesa:
    ├── Agrupa por cliente, bucket, estado, período
    ├── Calcula: QTY_DOCUMENTOS, TOTAL_ML, TOTAL_USD,
    │           PROMEDIO_DIAS_VENCIDO, MAX_DIAS_VENCIDO
    └── Escribe: curated/cxc/exportable/ + FACT_CXC_EXPORTABLE (pyodbc ACID)
```

---

## Orquestación ADF (pl_CxC_Master)

```
CopyFromAzureSQL_SAPReplica
        │
        ▼ (Succeeded)
RunDatabricks_CxC_Real
        │
        ▼ (Succeeded)
RunDatabricks_CxC_Exportable
```

---

## Diccionario de Datos

### Campos clave en FACT_CXC_REAL

| Campo | Tipo | Descripción | Origen SAP |
|-------|------|-------------|-----------|
| `EMPRESA` | VARCHAR(4) | Código de empresa SAP | `BUKRS` |
| `CLIENTE_SAP` | VARCHAR(10) | Número de cliente SAP (padded con ceros a 10 dígitos) | `KUNNR` |
| `NRO_DOCUMENTO` | VARCHAR(10) | Número de documento contable | `BELNR` |
| `FECHA_DOCUMENTO` | DATE | Fecha del documento | `BLDAT` |
| `FECHA_VENCIMIENTO` | DATE | Fecha de vencimiento del pago | `FAEDT` |
| `MONTO_ML` | DECIMAL(18,2) | Monto en moneda local (PEN) | `DMBTR` |
| `MONTO_USD` | DECIMAL(18,2) | Monto en moneda del documento (USD/PEN/EUR) | `WRBTR` |
| `MONEDA` | VARCHAR(5) | Clave de moneda del documento | `WAERS` |
| `DOC_COMPENSACION` | VARCHAR(10) | Número de documento de compensación (pago) | `AUGBL` |
| `FECHA_COMPENSACION` | DATE | Fecha en que se compensó (pagó) el documento | `AUGDT` |
| `DIAS_VENCIDO` | INT | Días transcurridos desde vencimiento (0 si compensado) | Calculado |
| `ESTADO` | VARCHAR(15) | `ABIERTO` (sin compensar) o `COMPENSADO` (pagado) | Calculado |
| `BUCKET_VENCIMIENTO` | VARCHAR(20) | Rango de envejecimiento del documento | Calculado |
| `ANIO_FISCAL` | SMALLINT | Año de la fecha del documento | Calculado |
| `PERIODO` | TINYINT | Mes de la fecha del documento | Calculado |

### Buckets de Vencimiento

| Bucket | Condición |
|--------|----------|
| `COMPENSADO` | `DOC_COMPENSACION IS NOT NULL` |
| `VIGENTE` | `DIAS_VENCIDO <= 0` (aún no vencido) |
| `1-30 DIAS` | `DIAS_VENCIDO BETWEEN 1 AND 30` |
| `31-60 DIAS` | `DIAS_VENCIDO BETWEEN 31 AND 60` |
| `61-90 DIAS` | `DIAS_VENCIDO BETWEEN 61 AND 90` |
| `+90 DIAS` | `DIAS_VENCIDO > 90` |

---

## Consideraciones de Negocio

- El proceso CxC se ejecuta **diariamente** para mantener el saldo de cuentas por cobrar actualizado
- Solo se procesan partidas de tipo cuenta `D` (Deudores); las de tipo `K` (Acreedores) pertenecen al dominio de CxP
- Los documentos con `MONTO_ML < 0` son notas de crédito o ajustes; se incluyen en el dataset pero se marcan para revisión en las validaciones
- La tabla `DIM_CXC_CLIENTE` debe mantenerse sincronizada con el maestro de clientes SAP (tabla `KNA1`)
