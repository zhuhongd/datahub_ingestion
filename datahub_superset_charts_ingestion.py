# datahub_superset_chart_ingestion.py
import os
import json
import requests
import datetime
from google.cloud import bigquery
from google.auth.transport.requests import Request
from google.oauth2 import id_token


DATAHUB_GMS_URL = 'https://datahub-stg-us.geotabtest.com/'  # Updated to production

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

def fetch_superset_charts(gbq_client, status_date):
    """Fetch Superset charts metadata from GBQ"""
    query = f"""
        SELECT StatusDate, Id, SupersetChartName, Description, Owners, FunctionalAreaCode, 
               DataSteward, CreationTime, LastModifiedTime, Status, ProjectId, SupersetDatasetId,
               JobsLast90Days, VisitsLast90Days, LastVisitTime
        FROM `geotab-metadata-prod.MetadataInventory_EU.SupersetCharts`
        WHERE StatusDate = '{status_date}'
    """
    return [dict(row) for row in gbq_client.query(query).result()]

def safe_str(value, default=""):
    """Handle null values safely"""
    return str(value) if value is not None else default

def ingest_superset_charts_to_datahub(**kwargs):
    """Main function to ingest Superset charts to DataHub"""
    from datahub.emitter.mcp import MetadataChangeProposalWrapper
    import datahub.emitter.mce_builder as builder
    from datahub.metadata.schema_classes import (
        ChartInfoClass,
        OwnerClass,
        OwnershipClass,
        OwnershipTypeClass,
        ChangeTypeClass,
        AuditStampClass,
        ChangeAuditStampsClass,
        StatusClass,
        GlobalTagsClass,
        TagAssociationClass
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
    
    print(f"Starting Superset chart ingestion for status_date: {status_date}")
    
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
    
    # Fetch and process charts
    superset_charts = fetch_superset_charts(gbq_client, status_date)
    print(f"Fetched {len(superset_charts)} charts for ingestion")

    toronto_tz = ZoneInfo("America/Toronto")
    now = datetime.datetime.now(toronto_tz)
    offset_hours = int(now.utcoffset().total_seconds() // 3600)   # e.g. -4
    last_updated_str = f"{now.strftime('%Y-%m-%d, %H:%M:%S')} GMT{offset_hours:+d}"

    for i, chart in enumerate(superset_charts):
        if i % 5000 == 0 or datetime.datetime.now() > token_refresh_time:
            refresh_iap_token()

        try:
            chart_id = str(chart["Id"])
            chart_urn = builder.make_chart_urn("superset", chart_id)
            chart_url = f"https://superset.geotab.com/explore/?slice_id={chart_id}"
            aspects = []
            
            # Prepare custom properties
            custom_props = {
                "functional_area": safe_str(chart.get("FunctionalAreaCode")),
                "superset_dataset_id": safe_str(chart.get("SupersetDatasetId")),
                "project_id": safe_str(chart.get("ProjectId")),
                "jobs_last_90_days": safe_str(chart.get("JobsLast90Days")),
                "visits_last_90_days": safe_str(chart.get("VisitsLast90Days")),
                "status": safe_str(chart.get("Status")),
                "last_visit_time": chart["LastVisitTime"].isoformat() if chart.get("LastVisitTime") else "",
                "last_updated_in_datahub": last_updated_str
            }
            
            # Prepare timestamps
            created_ts = int(chart["CreationTime"].timestamp() * 1000) if chart.get("CreationTime") else None
            modified_ts = int(chart["LastModifiedTime"].timestamp() * 1000) if chart.get("LastModifiedTime") else None
            
            # Create ChartInfo aspect
            chart_info = ChartInfoClass(
                title=safe_str(chart.get("SupersetChartName")), 
                description=safe_str(chart.get("Description"), "No description available"),
                lastModified=ChangeAuditStampsClass(
                    created=AuditStampClass(time=created_ts, actor="urn:li:corpuser:ingestion") if created_ts else None,
                    lastModified=AuditStampClass(time=modified_ts, actor="urn:li:corpuser:ingestion") if modified_ts else None
                ),
                externalUrl=chart_url,
                customProperties=custom_props
            )
            
            aspects.append(chart_info)
            # # Emit chart info
            # emitter.emit_mcp(MetadataChangeProposalWrapper(
            #     entityUrn=chart_urn,
            #     aspect=chart_info,
            #     changeType=ChangeTypeClass.UPSERT
            # ))
            
            # Add status tags
            status = safe_str(chart.get("Status")).lower()
            status_tags = []
            if status == "draft": status_tags.append("Draft")
            elif status == "archived": status_tags.append("Archived")
            elif status == "published": status_tags.append("Published")
            
            if status_tags:
                tags = GlobalTagsClass(
                    tags=[TagAssociationClass(tag=builder.make_tag_urn(tag)) for tag in status_tags]
                )
                aspects.append(tags)
                # emitter.emit_mcp(MetadataChangeProposalWrapper(
                #     entityUrn=chart_urn,
                #     aspect=tags,
                #     changeType=ChangeTypeClass.UPSERT
                # ))
            
            # Handle ownership
            owners = []
            for owner_email in chart.get("Owners", []):
                if owner_email and "@geotab.com" in owner_email:
                    username = owner_email.split("@")[0]
                    owners.append(OwnerClass(
                        owner=builder.make_user_urn(username),
                        type=OwnershipTypeClass.TECHNICAL_OWNER
                    ))
            
            if chart.get("DataSteward") and "@geotab.com" in chart["DataSteward"]:
                username = chart["DataSteward"].split("@")[0]
                owners.append(OwnerClass(
                    owner=builder.make_user_urn(username),
                    type=OwnershipTypeClass.DATA_STEWARD
                ))
            
            if owners:
                aspects.append(OwnershipClass(owners=owners))
                # emitter.emit_mcp(MetadataChangeProposalWrapper(
                #     entityUrn=chart_urn,
                #     aspect=OwnershipClass(owners=owners),
                #     changeType=ChangeTypeClass.UPSERT
                # ))
            
            # # Handle archival status
            # if status == "archived":
            #     emitter.emit_mcp(MetadataChangeProposalWrapper(
            #         entityUrn=chart_urn,
            #         aspect=StatusClass(removed=True),
            #         changeType=ChangeTypeClass.UPSERT
            #     ))

            mcps = MetadataChangeProposalWrapper.construct_many(entityUrn=chart_urn, aspects=aspects)
            emitter.emit_mcps(mcps)
                
            print(f"✅ Processed chart {chart_id}")
            
        except Exception as e:
            print(f"❌ Failed to process chart {chart_id}: {str(e)}")
    
    print("Superset chart ingestion completed")