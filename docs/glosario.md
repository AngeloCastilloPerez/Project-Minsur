# Glosario – Proyecto Minsur ETL

Referencia de términos de negocio, técnicos y abreviaturas utilizados en el proyecto de migración ETL de Minsur hacia Azure.

---

## Términos de Negocio

| Término | Definición |
|---------|-----------|
| **G&A** | Gastos Administrativos. Dominio financiero que agrupa todos los gastos operativos por centro de costo y cuenta contable. Incluye personal, servicios, materiales y depreciación. |
| **CxC** | Cuentas por Cobrar. Dominio financiero que registra los documentos de deuda de clientes hacia Minsur: facturas emitidas pendientes de cobro. |
| **CxP** | Cuentas por Pagar. (Fuera del alcance de este proyecto) Documentos de deuda de Minsur hacia proveedores. |
| **Balance General** | Estado de situación financiera de Minsur. Muestra los activos, pasivos y patrimonio a una fecha determinada. |
| **Mercado** | Dominio de datos de ventas y precios de metales: volumen, precio y ingresos por producto/metal/período. |
| **Cierre Contable** | Proceso mensual en el que el equipo de finanzas valida y cierra el período contable en SAP. Ocurre típicamente en los primeros 3 días hábiles del mes siguiente. |
| **Centro de Costo** | Unidad organizativa en SAP que agrupa los gastos de una gerencia o área específica. Ejemplo: `CC-1001` = Gerencia de Operaciones Mina. |
| **Cuenta GL** | Cuenta de Mayor General (General Ledger). Clasificación contable del gasto. Ejemplo: `GL-401001` = Remuneraciones. |
| **Plan / Presupuesto** | Datos presupuestados para el año fiscal, desagregados por mes, centro de costo y cuenta GL. Se carga desde Excel en SharePoint. |
| **Real** | Datos de ejecución real (actual) extraídos de SAP, correspondientes a los gastos ya contabilizados. |
| **Exportable** | Dataset consolidado que combina datos reales y de plan, con cálculo de varianzas, listo para consumo en Power BI. |
| **Varianza** | Diferencia entre el valor real y el planificado. `VAR = REAL - PLAN`. Varianza positiva en gastos = sobrecosto. |
| **Homologación** | Proceso de mapeo entre los códigos internos de SAP (KOSTL, SAKNR) y la nomenclatura de reportes locales de Minsur. |
| **Período Fiscal** | Mes contable en SAP (1-12). Coincide con el mes calendario en Minsur (no hay períodos especiales activos). |
| **Empresa SAP** | Código de 4 dígitos que identifica la entidad legal en SAP. `1000` = Minsur S.A., `1100` = Compañía Minera Kuri Kullu S.A. |
| **Aging / Envejecimiento** | Clasificación de documentos CxC según el número de días transcurridos desde su vencimiento. |
| **RFC** | Remote Function Call. Mecanismo de SAP para exponer funciones de negocio que pueden ser llamadas externamente (vía SOAP/REST). |
| **Partida Abierta** | Documento CxC que aún no ha sido compensado (pagado). |
| **Partida Compensada** | Documento CxC que ya fue cancelado mediante un documento de compensación (pago o nota de crédito). |
| **BSID / BSAD** | Tablas de SAP FI: `BSID` contiene partidas abiertas de deudores; `BSAD` contiene partidas compensadas. |

---

## Términos Técnicos

| Término | Definición |
|---------|-----------|
| **Medallón (Medallion Architecture)** | Arquitectura de datos en capas: landing (raw), staging (cleaned) y curated (business-ready). Cada capa agrega calidad y estructura. |
| **Landing** | Primera capa del medallón. Datos crudos tal como llegan de la fuente, sin transformación. Contenedor ADLS: `finanzas-landing`. |
| **Staging** | Segunda capa. Datos limpios, estandarizados y homologados, en formato Parquet. Contenedor: `finanzas-staging`. |
| **Curated** | Tercera capa. Datos finales listos para consumo, particionados por año/mes. Contenedor: `finanzas-curated`. |
| **ADLS Gen2** | Azure Data Lake Storage Generation 2. Servicio de almacenamiento de objetos de Microsoft Azure con soporte nativo para sistemas de archivos jerárquicos. |
| **ADF** | Azure Data Factory. Servicio de orquestación ETL/ELT de Microsoft Azure. |
| **Databricks** | Plataforma de análisis de datos basada en Apache Spark, disponible como servicio gestionado en Azure. |
| **PySpark** | API de Python para Apache Spark. Se usa en los notebooks Databricks para procesamiento distribuido. |
| **Key Vault** | Azure Key Vault. Servicio de Azure para almacenar y gestionar secretos, claves de cifrado y certificados de forma segura. |
| **Secret Scope** | Mecanismo de Databricks para referenciar secretos almacenados en Azure Key Vault desde notebooks. Uso: `dbutils.secrets.get(scope="kv-minsur", key="...")`. |
| **Azure Function** | Servicio de cómputo serverless de Azure. En este proyecto se usa para encapsular la llamada SOAP a SAP. |
| **SOAP** | Simple Object Access Protocol. Protocolo de comunicación basado en XML para servicios web. SAP expone sus RFCs como servicios SOAP. |
| **zeep** | Librería Python para consumir servicios web SOAP. Usada en la Azure Function `sap_gastos`. |
| **pyodbc** | Librería Python para conectarse a bases de datos ODBC (SQL Server, Azure SQL). Usada en notebooks para escritura ACID. |
| **ACID** | Atomicity, Consistency, Isolation, Durability. Propiedades de las transacciones de base de datos. En los notebooks se implementa con `BEGIN TRANSACTION` / `COMMIT` / `ROLLBACK`. |
| **Parquet** | Formato de almacenamiento columnar open-source. Más eficiente que CSV para lectura analítica. Usado en staging y curated. |
| **execution_id** | Identificador único (GUID o timestamp) asignado a cada ejecución del pipeline. Permite trazar todos los datos de una misma ejecución. |
| **abfss** | Azure Blob File System Secure. Esquema de URI para acceder a ADLS Gen2 desde Spark. Formato: `abfss://container@account.dfs.core.windows.net/path`. |
| **Managed Identity** | Identidad gestionada de Azure. Permite que los servicios Azure se autentiquen entre sí sin necesidad de credenciales explícitas. |
| **ARM Template** | Azure Resource Manager Template. Archivos JSON que describen la infraestructura Azure de forma declarativa. |
| **Direct Lake** | Modo de Power BI Premium que permite leer directamente desde Delta Lake en ADLS sin importar los datos. |
| **dbutils** | Objeto de utilidades de Databricks. `dbutils.widgets` para parámetros, `dbutils.secrets` para secretos, `dbutils.notebook` para control de notebooks. |

---

## Abreviaturas

| Abreviatura | Significado |
|-------------|------------|
| ADF | Azure Data Factory |
| ADLS | Azure Data Lake Storage |
| CC | Centro de Costo |
| CxC | Cuentas por Cobrar |
| CxP | Cuentas por Pagar |
| ETL | Extract, Transform, Load |
| ELT | Extract, Load, Transform |
| FI | Financial Accounting (módulo SAP) |
| G&A | Gastos y Administración (en reportes locales) |
| GL | General Ledger (Mayor General) |
| KV | Key Vault |
| ML | Moneda Local (PEN – Soles peruanos) |
| PBI | Power BI |
| PEN | Peso o Sol peruano (código ISO moneda local) |
| prd | Producción (ambiente) |
| RFC | Remote Function Call (SAP) |
| SAP | Systems, Applications, and Products in Data Processing |
| SLA | Service Level Agreement |
| SOAP | Simple Object Access Protocol |
| SQL | Structured Query Language |
| stg | Staging (ambiente de pruebas de integración) |
| UAT | User Acceptance Testing |
| USD | United States Dollar |
| WSDL | Web Services Description Language |
