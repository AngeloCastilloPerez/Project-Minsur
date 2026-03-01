# Arquitectura AS-IS: Pipeline ETL On-Premise (Jenkins)

## Descripción General

La arquitectura actual (AS-IS) del proceso ETL de finanzas de Minsur se basa en una infraestructura on-premise gestionada mediante **Jenkins** como orquestador de pipelines, con scripts Python y conexiones directas a SAP y servidores de archivos compartidos.

---

## Componentes Actuales

| Componente | Tecnología | Ubicación |
|-----------|-----------|----------|
| Orquestador | Jenkins | Servidor on-premise Lima |
| Extracción SAP | Python + zeep (SOAP) | Scripts en servidor Jenkins |
| Extracción SharePoint | Python + requests/MSAL | Scripts en servidor Jenkins |
| Almacenamiento intermedio | Sistema de archivos local / NAS | On-premise |
| Transformación | Python (pandas) | Scripts en servidor Jenkins |
| Destino | SQL Server 2019 on-premise (BD_FINANZAS) | Data center Lima |
| Visualización | Power BI (Direct Query / Import) | Nube (conecta a SQL on-premise) |

---

## Flujo del Pipeline Jenkins

```
[Trigger: Cron Jenkins]
        │
        ▼
[1. Extracción SAP SOAP]
    Python script invoca RFC ZmfCoSAlr87013613
    Resultado: CSV en directorio /etl/landing/ga/
        │
        ▼
[2. Extracción SharePoint]
    Python script descarga Excel de biblioteca SharePoint
    Resultado: .xlsx en /etl/landing/balance/, /etl/landing/mercado/
        │
        ▼
[3. Homologación y Transformación]
    Python + pandas
    Aplica mapeos de centros de costo y cuentas GL
    Resultado: CSV/DataFrame transformado
        │
        ▼
[4. Carga a SQL Server]
    Python pyodbc INSERT masivo
    Destino: BD_FINANZAS on-premise
        │
        ▼
[5. Notificación]
    Email vía SMTP local
    Log en archivo /etl/logs/
```

---

## Detalle por Dominio

### G&A (Gastos Administrativos)
- **Fuente**: SAP SOAP (RFC `ZmfCoSAlr87013613`)
- **Frecuencia**: Mensual (1er día hábil de cada mes)
- **Servidor**: jenkins-etl-01.minsur.local
- **Script**: `/opt/etl/scripts/ga/extract_ga.py`, `transform_ga.py`, `load_ga.py`
- **Destino**: `BD_FINANZAS.dbo.FACT_GA_REAL`, `FACT_GA_PLAN`

### CxC (Cuentas por Cobrar)
- **Fuente**: SQL Server réplica SAP (base de datos `SAP_REPLICA`)
- **Frecuencia**: Diaria
- **Script**: `/opt/etl/scripts/cxc/extract_cxc.py`, `load_cxc.py`
- **Destino**: `BD_FINANZAS.dbo.FACT_CXC_REAL`

### Balance General
- **Fuente**: Excel en SharePoint (subida manual por el área de finanzas)
- **Frecuencia**: Mensual
- **Script**: `/opt/etl/scripts/balance/extract_balance.py`
- **Destino**: `BD_FINANZAS.dbo.FACT_BALANCE_REAL`

### Mercado
- **Fuente**: Excel en SharePoint
- **Frecuencia**: Mensual
- **Script**: `/opt/etl/scripts/mercado/extract_mercado.py`
- **Destino**: `BD_FINANZAS.dbo.FACT_MERCADO_REAL`

---

## Problemas y Limitaciones (Pain Points)

| # | Problema | Impacto |
|---|---------|---------|
| 1 | **Sin escalabilidad**: servidor Jenkins de capacidad fija (4 CPU, 16 GB RAM). En períodos de cierre contable el proceso tarda 4+ horas | Alto |
| 2 | **Sin retry automático**: si un paso falla, requiere intervención manual para re-ejecutar desde el punto de falla | Alto |
| 3 | **Sin monitoreo centralizado**: logs dispersos en archivos locales, sin alertas proactivas | Alto |
| 4 | **Dependencia de horario de oficina**: el servidor Jenkins está en zona horaria Lima, sin soporte 24/7 | Medio |
| 5 | **Sin linaje de datos**: no hay trazabilidad de qué datos fueron procesados en cada ejecución | Medio |
| 6 | **Seguridad de credenciales**: SAP passwords almacenados en archivos `.env` en el servidor | Alto |
| 7 | **Sin ambiente de desarrollo/QA**: los scripts se prueban directamente en producción | Alto |
| 8 | **Mantenimiento de Python en servidor**: versiones de librerías no gestionadas (pip install manual) | Medio |
| 9 | **Power BI limitado**: Direct Query a SQL Server on-premise con latencia alta; sin posibilidad de ADLS nativo | Medio |
| 10 | **Sin CI/CD**: los scripts se despliegan copiando archivos por FTP/SCP manualmente | Alto |

---

## Resumen de Capacidades Actuales

- **Volumen procesado**: ~500K registros/mes en G&A, ~200K en CxC
- **SLA actual**: 8 horas desde cierre contable hasta datos disponibles en Power BI
- **Disponibilidad**: 97% (3 incidentes/año por caída de servidor Jenkins o SAP)
- **Equipo de soporte**: 2 ingenieros de datos dedicados a mantenimiento reactivo
