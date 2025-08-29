from google.cloud import bigquery
import os
from datetime import datetime, timedelta
from google.auth.transport.requests import Request
from google.oauth2 import id_token
import json
import time
import requests  # Added for non-IAP case


# Helper functions remain the same
def send_request(url, param_values):
    wop_regulation = os.getenv('WOP_REGULATION', "commercial")
    if wop_regulation == "commercial" or wop_regulation == "conus":
        import op_iap.op_iap as iap
        return iap.make_iap_request(url, params=param_values)
    else:
        return requests.get(url, params=param_values)

def get_vault_secret(secret_name, **kwargs):
    dag_run = kwargs["dag_run"]
    AC_URL = os.getenv("AC_URL")
    param_values = {
        "run_id": dag_run.run_id,
        "dag_id": dag_run.dag_id,
        "id": dag_run.id,
        "secret_name": secret_name,
    }
    url = f"{AC_URL}/vault-secret"
    resp = send_request(url, param_values)
    return json.loads(resp.text)[1]  # Directly return secret value

ENV_MAP = {
    "-prod": "prod",
    "-test": "test",
    "-raw": "prod",
}

def extract_env_from_project(project_name):
    lowered = project_name.lower()
    for suffix, env in ENV_MAP.items():
        if lowered.endswith(suffix):
            return env
    return "prod"

# Core functionality
def ingest_gbq_lineage_to_datahub(**kwargs):
    """Main function for PythonOperator callable"""

    from datahub.emitter.rest_emitter import DatahubRestEmitter 
    import datahub.emitter.mce_builder as builder
    from datahub.emitter.mcp import MetadataChangeProposalWrapper
    from datahub.metadata.schema_classes import (UpstreamClass, UpstreamLineageClass, ChangeTypeClass)
    # 1. Get secrets from context
    datahub_iap_token = get_vault_secret("datahub_iap_token", **kwargs)
    datahub_access_token = get_vault_secret("datahub_personal_token_0714", **kwargs)
    
    # 2. Setup authentication
    IAP_CLIENT_ID = datahub_iap_token['token']
    DATAHUB_GMS_TOKEN = datahub_access_token['token']
    iap_token = id_token.fetch_id_token(Request(), IAP_CLIENT_ID)
    
    # 3. Initialize clients
    gbq_client = bigquery.Client(project="geotab-metadata-prod")
    emitter = DatahubRestEmitter(
        token=DATAHUB_GMS_TOKEN,
        gms_server="https://datahub-stg-us.geotabtest.com/api/gms",
        extra_headers={"Proxy-Authorization": f"Bearer {iap_token}"},
        connect_timeout_sec=30,
        read_timeout_sec=120
    )
    
    # 4. Calculate processing date (previous day)
    execution_date = kwargs['execution_date']
    status_date = execution_date.strftime('%Y-%m-%d')
    
    # 5. Fetch and ingest lineage
    lineage_data = fetch_lineage_data(gbq_client, status_date)
    _ingest_lineage_records(emitter, lineage_data)

def fetch_lineage_data(client, status_date):
    """Fetch lineage data for specific date"""
    query = f"""
        SELECT DISTINCT
            ReferencedProject, ReferencedDataset, ReferencedTable,
            DestinationProject, DestinationDataset, DestinationTable
        FROM `geotab-metadata-prod.Lineage.TableToTable`
        WHERE ExecutionDate = "{status_date}"
    """
    return [dict(row) for row in client.query(query).result()]

def _ingest_lineage_records(emitter, lineage_records):
    """Process lineage records"""

    from datahub.emitter.rest_emitter import DatahubRestEmitter 
    import datahub.emitter.mce_builder as builder
    from datahub.emitter.mcp import MetadataChangeProposalWrapper
    from datahub.metadata.schema_classes import (UpstreamClass, UpstreamLineageClass, ChangeTypeClass)
    
    downstream_map = {}
    for record in lineage_records:
        dest_key = (record['DestinationProject'], record['DestinationDataset'], record['DestinationTable'])
        upstream_ref = (record['ReferencedProject'], record['ReferencedDataset'], record['ReferencedTable'])
        
        downstream_map.setdefault(dest_key, []).append(upstream_ref)
    
    for dest_key, upstream_refs in downstream_map.items():
        dest_project, dest_dataset, dest_table = dest_key
        dest_env = extract_env_from_project(dest_project)
        dest_urn = builder.make_dataset_urn(
            platform="bigquery",
            name=f"{dest_project}.{dest_dataset}.{dest_table}",
            env=dest_env
        )
        
        upstreams = []
        for ref in upstream_refs:
            ref_project, ref_dataset, ref_table = ref
            ref_env = extract_env_from_project(ref_project)
            upstream_urn = builder.make_dataset_urn(
                platform="bigquery",
                name=f"{ref_project}.{ref_dataset}.{ref_table}",
                env=ref_env
            )
            upstreams.append(UpstreamClass(dataset=upstream_urn, type="TRANSFORMED"))
        
        upstream_lineage = UpstreamLineageClass(upstreams=upstreams)
        mcp = MetadataChangeProposalWrapper(
            entityUrn=dest_urn,
            aspect=upstream_lineage,
            changeType=ChangeTypeClass.UPSERT,
            aspectName="upstreamLineage"
        )
        
        try:
            emitter.emit(mcp)
            # time.sleep(0.1)
            print(f"[SUCCESS]✅ Lineage successfully implemented for {dest_urn}")
        except Exception as e:
            print(f"Failed lineage for {dest_urn}: {str(e)}")