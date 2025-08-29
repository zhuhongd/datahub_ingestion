import os
import json
import requests
import datetime
from google.cloud import bigquery
from google.auth.transport.requests import Request
from google.oauth2 import id_token

import logging

logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO").upper())
logger = logging.getLogger(__name__)

DATAHUB_GMS_URL = 'https://datahub-stg-us.geotabtest.com/'  # Updated to production
ORCHESTRATOR="airflow"

def create_platform_instance(platform: str, platform_instance: str):
    from datahub.metadata.schema_classes import (
        DataPlatformInstanceClass,
    )

    from datahub.metadata.urns import (
        DataPlatformUrn,
        DataPlatformInstanceUrn,
    )

    data_platform_urn = DataPlatformUrn(ORCHESTRATOR).urn()

    data_platform_instance_urn = DataPlatformInstanceUrn(
        data_platform_urn,
        instance=platform_instance,
    ).urn()

    data_platform_instance = DataPlatformInstanceClass(
        data_platform_urn,
        data_platform_instance_urn,
    )

    return data_platform_instance

def extract_env_from_project(project_name):
    """Maps BigQuery project names to DataHub environments"""
    lowered = project_name.lower()
    if lowered.endswith("-raw") or lowered.endswith("-prod"):
        return "PROD"
    elif lowered.endswith("-test"):
        return "TEST"
    elif lowered.endswith("-uat"):
        return "UAT"
    return "NON-PROD"

def create_dataflow_entity(dag:dict):
    from datahub.metadata.urns import (
        DataFlowUrn,
    )

    from datahub.metadata.schema_classes import (
        FabricTypeClass,
        DataFlowInfoClass,
        OwnershipClass,
        OwnershipTypeClass,
        OwnerClass,
    )

    import datahub.emitter.mce_builder as builder

    aspects = []

    dag_id = dag["DagId"]
    platform_instance=dag["Environment"]

    data_platform_instance = create_platform_instance(ORCHESTRATOR, platform_instance)

    aspects.append(data_platform_instance)

    urn = DataFlowUrn.create_from_ids(
        orchestrator=ORCHESTRATOR,
        flow_id=dag_id,
        env=FabricTypeClass.PROD,
        platform_instance=platform_instance,
    ).urn()

    # Add ownership information
    owner_str = str(dag.get("Owner", "")).strip()
    steward_str = str(dag.get("DataSteward","")).strip()
    steward_usernames = []

    if steward_str:
        for email in steward_str.split(';'):
            email = email.strip()
            if '@geotab.com' in email:
                username = email.split('@')[0]
                steward_usernames.append(username)
            else:
                steward_usernames.append(email)

    if owner_str or steward_usernames:
        # Prepare owners list
        owners = []
        
        # Process Owners - use raw value as-is
        if owner_str:
            # MODIFIED: Use the entire owner string without splitting
            owners.append(
                OwnerClass(
                    owner=builder.make_user_urn(owner_str),
                    type=OwnershipTypeClass.TECHNICAL_OWNER
                )
            )
        
        # Process Data Stewards
        if steward_usernames:
            for username in steward_usernames:
                owners.append(
                    OwnerClass(
                        owner=builder.make_user_urn(username),
                        type=OwnershipTypeClass.DATA_STEWARD
                    )
                )
        
        # Create and emit ownership aspect
        ownership_aspect = OwnershipClass(owners=owners)

        aspects.append(ownership_aspect)

    # format timestamps
    for ts_prop in ["LastRunTime"]:
        ts = dag.get(ts_prop)
        if ts is not None:
            dag[ts_prop] = ts.strftime('%Y-%m-%d %H:%M:%S %Z')

    # format percentages
    for p_prop in ["ErrorRateLast7Days","ErrorRateLast30Days","ErrorRateLast60Days"]:
        p = dag.get(p_prop)
        if p is not None:
            dag[p_prop] = f"{p:.2%}"

    custom_properties = {key: str(dag[key]) for key in dag.keys() & {
        "LastRunTime", "Schedule","Status",
        "ErrorRateLast7Days","ErrorRateLast30Days","ErrorRateLast60Days",
        "Environment","Region","FunctionalArea"
    } if dag[key] is not None}

    data_flow_info = DataFlowInfoClass(
        name=dag_id,
        customProperties=custom_properties,
        externalUrl=dag["Url"],
        description=dag["Description"],
        project=dag["FunctionalAreaCode"],
        env=FabricTypeClass.PROD,
    )
    
    aspects.append(data_flow_info)

    return urn, aspects

def create_datajob_entity(task:dict):
    from datahub.metadata.urns import (
        DataFlowUrn,
        DataJobUrn,
        DatasetUrn,
    )

    from datahub.metadata.schema_classes import (
        FabricTypeClass,
        DataJobInfoClass,
        DataJobInputOutputClass,
        EdgeClass,
    )
    aspects = []

    task_id = task["TaskId"]
    platform_instance=task["Environment"]

    data_platform_instance = create_platform_instance(ORCHESTRATOR, platform_instance)

    aspects.append(data_platform_instance)

    data_flow_urn = DataFlowUrn.create_from_ids(
        orchestrator="airflow",
        flow_id=task["DagId"],
        env=FabricTypeClass.PROD,
        platform_instance=platform_instance,
    ).urn()

    urn = DataJobUrn.create_from_ids(
        data_flow_urn=data_flow_urn,
        job_id=task_id,
    ).urn()

    # format timestamps
    for ts_prop in ["ExecutionTime", "PipelineRunTime"]:
        ts = task.get(ts_prop)
        if ts is not None:
            task[ts_prop] = ts.strftime('%Y-%m-%d %H:%M:%S %Z')

    custom_properties = {key: str(task[key]) for key in task.keys() & {
       "ExecutionTime","Environment","PipelineRunTime"
    } if task[key] is not None}

    data_job_info = DataJobInfoClass(
        name=task_id,
        type="SQL",
        customProperties=custom_properties,
        externalUrl=task["Url"],
        flowUrn=data_flow_urn,
        env=FabricTypeClass.PROD,
    )

    aspects.append(data_job_info)

    output_edges=[]

    for downstream_table in task["DownstreamTables"]:
        destination_project_id = downstream_table.split(".")[0]
        environment = extract_env_from_project(destination_project_id)

        destination_urn = DatasetUrn.create_from_ids(
            platform_id="bigquery",
            table_name=downstream_table,
            env=environment,
        ).urn()

        output_dataset_edge = EdgeClass(
            destinationUrn=destination_urn,
        )

        output_edges.append(output_dataset_edge)

    if len(output_edges) > 0:
        data_job_input_output = DataJobInputOutputClass(
            inputDatasets=[],
            outputDatasets=[],
            outputDatasetEdges=output_edges,
        )

        aspects.append(data_job_input_output)

    return urn, aspects

def get_vault_secret(secret_name, **kwargs):
    """Fetch secrets from Vault using kwargs context"""
    dag_run = kwargs["dag_run"]
    AC_URL = os.getenv("AC_URL")
    param_values = {
        "run_id": dag_run.run_id,
        "dag_id": dag_run.dag_id,
        "id": dag_run.id,
        "secret_name": secret_name,
    }
    url = f"{AC_URL}/vault-secret"
    
    wop_regulation = os.getenv('WOP_REGULATION', "commercial")
    if wop_regulation == "commercial" or wop_regulation == "conus":
        import op_iap.op_iap as iap
        response = iap.make_iap_request(url, params=param_values)
    else:
        response = requests.get(url, params=param_values)
        
    secret_dict = json.loads(response.text)
    return secret_dict[1]

def ingest_airflow_pipelines_to_datahub(**kwargs):
    from datahub.emitter.rest_emitter import DatahubRestEmitter
    from datahub.emitter.mcp import MetadataChangeProposalWrapper
    from dna_datahub_ingestion.sql_template import SqlTemplate

    token_refresh_time = datetime.datetime.now() + datetime.timedelta(minutes=50)

    # Helper functions scoped inside ingest_metadata_to_datahub
    def refresh_iap_token():
        """Refresh IAP token and update emitter"""
        nonlocal iap_token, token_refresh_time
        new_iap_token = id_token.fetch_id_token(Request(), IAP_CLIENT_ID)
        
        # Update emitter headers
        emitter._session.headers.update({
            "Proxy-Authorization": f"Bearer {new_iap_token}"
        })
        
        iap_token = new_iap_token
        token_refresh_time = datetime.datetime.now() + datetime.timedelta(minutes=50)
        print("🔄 Refreshed IAP token")

    dag_run = kwargs["dag_run"]
    execution_date = kwargs['execution_date']
    status_date = execution_date.strftime('%Y-%m-%d')
    
    print(f"Starting Airflow pipeline ingestion for status_date: {status_date}")
    
    # Get secrets from Vault
    datahub_iap_token = get_vault_secret("datahub_iap_token", **kwargs)
    datahub_access_token = get_vault_secret("datahub_personal_token_0714", **kwargs)
    
    IAP_CLIENT_ID = datahub_iap_token['token']
    DATAHUB_GMS_TOKEN = datahub_access_token['token']
    
    # Initialize BigQuery client
    gbq_client = bigquery.Client(project="geotab-metadata-prod")
    
    # Initialize DataHub emitter
    iap_token = id_token.fetch_id_token(Request(), IAP_CLIENT_ID)
    emitter = DatahubRestEmitter(
        token=DATAHUB_GMS_TOKEN,
        gms_server=f"{DATAHUB_GMS_URL}api/gms",
        extra_headers={
            "Proxy-Authorization": f"Bearer {iap_token}",
            "Authorization": f"Bearer {DATAHUB_GMS_TOKEN}"
        },
        connect_timeout_sec=10,
        read_timeout_sec=60
    )

    pipelines_sql = SqlTemplate(
        "sql/AirflowPipelines.sql",
        gbq_client,
        params={"status_date": status_date},
    )
    pipelines = pipelines_sql.execute()

    for i, pipeline in enumerate(pipelines):

        if i % 500 == 0 or datetime.datetime.now() > token_refresh_time:
            refresh_iap_token()

        pipeline = dict(pipeline)
        urn, aspects = create_dataflow_entity(pipeline)

        try:
            mcps = MetadataChangeProposalWrapper.construct_many(entityUrn=urn, aspects=aspects)
            emitter.emit_mcps(mcps)
            logger.info(f"✅ Processed pipeline {urn}")
        except Exception as e:
            logger.error(f"❌ Failed to process pipeline {urn}: {str(e)}")

def ingest_task_lineage_to_datahub(**kwargs):
    from datahub.emitter.rest_emitter import DatahubRestEmitter
    from datahub.emitter.mcp import MetadataChangeProposalWrapper
    from dna_datahub_ingestion.sql_template import SqlTemplate

    token_refresh_time = datetime.datetime.now() + datetime.timedelta(minutes=50)

    # Helper functions scoped inside ingest_metadata_to_datahub
    def refresh_iap_token():
        """Refresh IAP token and update emitter"""
        nonlocal iap_token, token_refresh_time
        new_iap_token = id_token.fetch_id_token(Request(), IAP_CLIENT_ID)
        
        # Update emitter headers
        emitter._session.headers.update({
            "Proxy-Authorization": f"Bearer {new_iap_token}"
        })
        
        iap_token = new_iap_token
        token_refresh_time = datetime.datetime.now() + datetime.timedelta(minutes=50)
        print("🔄 Refreshed IAP token")

    dag_run = kwargs["dag_run"]
    execution_date = kwargs['execution_date']
    status_date = execution_date.strftime('%Y-%m-%d')
    
    print(f"Starting Airflow task + lineage ingestion for status_date: {status_date}")
    
    # Get secrets from Vault
    datahub_iap_token = get_vault_secret("datahub_iap_token", **kwargs)
    datahub_access_token = get_vault_secret("datahub_personal_token_0714", **kwargs)
    
    IAP_CLIENT_ID = datahub_iap_token['token']
    DATAHUB_GMS_TOKEN = datahub_access_token['token']
    
    # Initialize BigQuery client
    gbq_client = bigquery.Client(project="geotab-metadata-prod")
    
    # Initialize DataHub emitter
    iap_token = id_token.fetch_id_token(Request(), IAP_CLIENT_ID)
    emitter = DatahubRestEmitter(
        token=DATAHUB_GMS_TOKEN,
        gms_server=f"{DATAHUB_GMS_URL}api/gms",
        extra_headers={
            "Proxy-Authorization": f"Bearer {iap_token}",
            "Authorization": f"Bearer {DATAHUB_GMS_TOKEN}"
        },
        connect_timeout_sec=10,
        read_timeout_sec=60
    )

    task_lineage_sql = SqlTemplate(
        "sql/AirflowTaskLineage.sql",
        gbq_client,
        params={"status_date": status_date},
    )
    task_lineage = task_lineage_sql.execute()

    for i, task in enumerate(task_lineage):

        if i % 500 == 0 or datetime.datetime.now() > token_refresh_time:
            refresh_iap_token()

        task = dict(task)
        urn, aspects = create_datajob_entity(task)

        try:
            mcps = MetadataChangeProposalWrapper.construct_many(entityUrn=urn, aspects=aspects)
            emitter.emit_mcps(mcps)
            logger.info(f"✅ Processed task {urn}")
        except Exception as e:
            logger.error(f"❌ Failed to process task {urn}: {str(e)}")
