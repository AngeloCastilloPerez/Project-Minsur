import logging
import json
import os
import uuid
from datetime import datetime, timezone

import azure.functions as func
from zeep import Client, Settings
from zeep.transports import Transport
from zeep.wsse.username import UsernameToken
from requests import Session
from requests.auth import HTTPBasicAuth
from azure.storage.filedatalake import DataLakeServiceClient
from azure.identity import DefaultAzureCredential

logger = logging.getLogger("sap_gastos")
logger.setLevel(logging.INFO)


def main(req: func.HttpRequest) -> func.HttpResponse:
    """
    Azure Function – HTTP POST trigger.
    Calls SAP SOAP web service ZmfTestService to extract G&A cost data
    and saves the result as a JSON file in ADLS Gen2 test-landing/ga/real/.
    """
    logger.info("sap_gastos function triggered.")

    # -------------------------------------------------------------------------
    # 1. Parse request body
    # -------------------------------------------------------------------------
    try:
        body = req.get_json()
    except ValueError:
        return func.HttpResponse(
            json.dumps({"error": "Request body must be valid JSON."}),
            status_code=400,
            mimetype="application/json"
        )

    fiscal_year     = body.get("fiscal_year")
    period          = body.get("period")
    company_code    = body.get("company_code", "TEST1")
    environment     = body.get("environment", "dev")
    execution_id    = body.get("execution_id") or str(uuid.uuid4())
    storage_account = body.get("storage_account")

    if not fiscal_year or not period or not storage_account:
        return func.HttpResponse(
            json.dumps({"error": "fiscal_year, period, and storage_account are required."}),
            status_code=400,
            mimetype="application/json"
        )

    # -------------------------------------------------------------------------
    # 2. Retrieve credentials from environment variables (backed by Key Vault refs)
    # -------------------------------------------------------------------------
    sap_url      = os.environ.get("SAP_URL")
    sap_username = os.environ.get("SAP_USERNAME")
    sap_password = os.environ.get("SAP_PASSWORD")

    if not sap_url or not sap_username or not sap_password:
        logger.error("SAP credentials not configured in environment variables.")
        return func.HttpResponse(
            json.dumps({"error": "SAP credentials are not configured."}),
            status_code=500,
            mimetype="application/json"
        )

    # -------------------------------------------------------------------------
    # 3. Call SAP SOAP web service ZmfTestService
    # -------------------------------------------------------------------------
    try:
        logger.info(f"Calling SAP SOAP service | fiscal_year={fiscal_year} | period={period} | company={company_code}")

        session = Session()
        session.auth = HTTPBasicAuth(sap_username, sap_password)
        transport = Transport(session=session, timeout=120)

        settings = Settings(strict=False, xml_huge_tree=True)
        client = Client(
            wsdl=sap_url,
            wsse=UsernameToken(sap_username, sap_password),
            transport=transport,
            settings=settings
        )

        # Call the SAP BAPI/RFC exposed as SOAP
        response = client.service.ZmfTestService(
            I_BUKRS=company_code,
            I_GJAHR=str(fiscal_year),
            I_MONAT=str(period).zfill(2),
            I_VERSN="0",    # Actual version
            I_WRTTP="04"    # 04 = actual values
        )

        # Serialize response to list of dicts
        records = []
        if hasattr(response, "ET_DATA") and response.ET_DATA:
            for item in response.ET_DATA.item:
                records.append({
                    "BUKRS":  getattr(item, "BUKRS",  None),
                    "KOSTL":  getattr(item, "KOSTL",  None),
                    "SAKNR":  getattr(item, "SAKNR",  None),
                    "GJAHR":  getattr(item, "GJAHR",  None),
                    "MONAT":  getattr(item, "MONAT",  None),
                    "WKGBTR": str(getattr(item, "WKGBTR", 0)),
                    "WTGBTR": str(getattr(item, "WTGBTR", 0)),
                    "VERSN":  getattr(item, "VERSN",  None),
                    "WRTTP":  getattr(item, "WRTTP",  None),
                })

        logger.info(f"SAP response: {len(records)} records received.")

    except Exception as e:
        logger.error(f"SAP SOAP call failed: {e}")
        return func.HttpResponse(
            json.dumps({"error": f"SAP SOAP call failed: {str(e)}"}),
            status_code=502,
            mimetype="application/json"
        )

    # -------------------------------------------------------------------------
    # 4. Save JSON output to ADLS Gen2 test-landing/ga/real/
    # -------------------------------------------------------------------------
    try:
        output = {
            "metadata": {
                "fiscal_year":  fiscal_year,
                "period":       period,
                "company_code": company_code,
                "execution_id": execution_id,
                "environment":  environment,
                "record_count": len(records),
                "extracted_at": datetime.now(timezone.utc).isoformat()
            },
            "data": records
        }

        file_content = json.dumps(output, ensure_ascii=False, indent=2).encode("utf-8")
        file_name    = f"sap_ga_real_{fiscal_year}_{str(period).zfill(2)}_{execution_id}.json"
        container    = "test-landing"
        directory    = f"ga/real/execution_id={execution_id}"

        credential     = DefaultAzureCredential()
        service_client = DataLakeServiceClient(
            account_url=f"https://{storage_account}.dfs.core.windows.net",
            credential=credential
        )

        fs_client  = service_client.get_file_system_client(file_system=container)
        dir_client = fs_client.get_directory_client(directory)
        dir_client.create_directory()

        file_client = dir_client.create_file(file_name)
        file_client.append_data(data=file_content, offset=0, length=len(file_content))
        file_client.flush_data(len(file_content))

        adls_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/{directory}/{file_name}"
        logger.info(f"File saved to ADLS: {adls_path}")

    except Exception as e:
        logger.error(f"ADLS write failed: {e}")
        return func.HttpResponse(
            json.dumps({"error": f"ADLS write failed: {str(e)}"}),
            status_code=500,
            mimetype="application/json"
        )

    # -------------------------------------------------------------------------
    # 5. Return success response
    # -------------------------------------------------------------------------
    return func.HttpResponse(
        json.dumps({
            "status":       "SUCCESS",
            "execution_id": execution_id,
            "records":      len(records),
            "adls_path":    adls_path
        }),
        status_code=200,
        mimetype="application/json"
    )
