# dna_datahub/datahub_superset_dataset_ingestion.py
import os
import json
import requests
import datetime
from google.cloud import bigquery
from google.auth.transport.requests import Request
from google.oauth2 import id_token

DATAHUB_GMS_URL = 'https://datahub-stg-us.geotabtest.com/' 

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

def safe_str(value, default=""):
    """Handle null values safely"""
    return str(value) if value is not None else default

def fetch_superset_datasets(gbq_client, status_date):
    """Fetch Superset datasets metadata from GBQ"""
    query = f"""
        SELECT
            StatusDate,
            Id,
            SupersetDatasetName,
            Description,
            Owners,
            FunctionalAreaCode,
            DataSteward,
            CreationTime,
            LastModifiedTime,
            Status,
            ProjectId,
            DatasetId,
            JobsLast90Days,
            UniqueQueriesLast90Days
        FROM `geotab-metadata-prod.MetadataInventory_EU.SupersetDatasets`
        WHERE StatusDate = '{status_date}'
    """
    return [dict(row) for row in gbq_client.query(query).result()]

def ingest_superset_datasets_to_datahub(**kwargs):
    """Main function to ingest Superset datasets to DataHub"""
    from datahub.emitter.mcp import MetadataChangeProposalWrapper
    import datahub.emitter.mce_builder as builder
    from datahub.metadata.schema_classes import (
        DatasetPropertiesClass,
        OwnerClass,
        OwnershipClass,
        OwnershipTypeClass,
        ChangeTypeClass,
        GlobalTagsClass,
        TagAssociationClass,
        StatusClass,
        TimeStampClass
    )
    from datahub.emitter.rest_emitter import DatahubRestEmitter
    from zoneinfo import ZoneInfo

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
    
    print(f"Starting Superset dataset ingestion for status_date: {status_date}")
    
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
        connect_timeout_sec=30,  # Increased timeout
        read_timeout_sec=120
    )
    
    # Fetch and process datasets
    superset_datasets = fetch_superset_datasets(gbq_client, status_date)
    print(f"Fetched {len(superset_datasets)} datasets for ingestion")
    
    toronto_tz = ZoneInfo("America/Toronto")
    now = datetime.datetime.now(toronto_tz)
    offset_hours = int(now.utcoffset().total_seconds() // 3600)   # e.g. -4
    last_updated_str = f"{now.strftime('%Y-%m-%d, %H:%M:%S')} GMT{offset_hours:+d}"

    for i, dataset in enumerate(superset_datasets):
        if i % 3000 == 0 or datetime.datetime.now() > token_refresh_time:
            refresh_iap_token()
        try:
            dataset_id = str(dataset["Id"])
            dataset_urn = builder.make_dataset_urn("superset", dataset_id)
            dataset_url = f"https://superset.geotab.com/explore/?datasource_type=table&datasource_id={dataset_id}"
            aspects = []

            # Prepare custom properties
            custom_props = {
                "functional_area": safe_str(dataset.get("FunctionalAreaCode")),
                "project_id": safe_str(dataset.get("ProjectId")),
                "bq_dataset_id": safe_str(dataset.get("DatasetId")),
                "jobs_last_90_days": safe_str(dataset.get("JobsLast90Days")),
                "unique_queries_last_90_days": safe_str(dataset.get("UniqueQueriesLast90Days")),
                "status": safe_str(dataset.get("Status")),
                "last_updated_in_datahub": last_updated_str
            }
            
            # Prepare timestamps
            created_ts = int(dataset["CreationTime"].timestamp() * 1000) if dataset.get("CreationTime") else None
            modified_ts = int(dataset["LastModifiedTime"].timestamp() * 1000) if dataset.get("LastModifiedTime") else None
            
            # Create DatasetProperties aspect
            dataset_props = DatasetPropertiesClass(
                name=dataset.get("SupersetDatasetName", f"Dataset {dataset_id}"),
                description=safe_str(dataset.get("Description"), "No description available"),
                customProperties=custom_props,
                externalUrl=dataset_url,
                created=TimeStampClass(time=created_ts) if created_ts else None,
                lastModified=TimeStampClass(time=modified_ts) if modified_ts else None
            )

            aspects.append(dataset_props)
            
            # # Emit dataset properties
            # emitter.emit_mcp(MetadataChangeProposalWrapper(
            #     entityUrn=dataset_urn,
            #     aspect=dataset_props,
            #     changeType=ChangeTypeClass.UPSERT
            # ))
            
            # Add status tags
            status = safe_str(dataset.get("Status", "")).lower()
            status_tags = []
            if status == "Not Certified": status_tags.append("Not Certified")
            elif status == "Certified": status_tags.append("Certified")
            # elif status == "published": status_tags.append("Published")
            # elif status == "certified": status_tags.append("Certified")
            
            if status_tags:
                tags = GlobalTagsClass(
                    tags=[TagAssociationClass(tag=builder.make_tag_urn(tag)) for tag in status_tags]
                )
                aspects.append(tags)
                # emitter.emit_mcp(MetadataChangeProposalWrapper(
                #     entityUrn=dataset_urn,
                #     aspect=tags,
                #     changeType=ChangeTypeClass.UPSERT
                # ))
            
            # Handle ownership
            owners = []
            for owner_email in dataset.get("Owners", []):
                if owner_email and "@geotab.com" in owner_email:
                    username = owner_email.split("@")[0]
                    owners.append(OwnerClass(
                        owner=builder.make_user_urn(username),
                        type=OwnershipTypeClass.TECHNICAL_OWNER
                    ))
            
            if dataset.get("DataSteward") and "@geotab.com" in dataset["DataSteward"]:
                username = dataset["DataSteward"].split("@")[0]
                owners.append(OwnerClass(
                    owner=builder.make_user_urn(username),
                    type=OwnershipTypeClass.DATA_STEWARD
                ))
            
            if owners:
                aspects.append(OwnershipClass(owners=owners))
                # emitter.emit_mcp(MetadataChangeProposalWrapper(
                #     entityUrn=dataset_urn,
                #     aspect=OwnershipClass(owners=owners),
                #     changeType=ChangeTypeClass.UPSERT
                # ))
            
            # # Handle archival status
            # if status == "archived":
            #     emitter.emit_mcp(MetadataChangeProposalWrapper(
            #         entityUrn=dataset_urn,
            #         aspect=StatusClass(removed=True),
            #         changeType=ChangeTypeClass.UPSERT
            #     ))

            mcps = MetadataChangeProposalWrapper.construct_many(entityUrn=dataset_urn, aspects=aspects)
            emitter.emit_mcps(mcps)
                
            print(f"✅ Processed dataset {dataset_id}")
            
        except Exception as e:
            print(f"❌ Failed to process dataset {dataset_id}: {str(e)}")
    
    print("Superset dataset ingestion completed")