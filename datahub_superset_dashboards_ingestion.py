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
PLATFORM="superset"

def create_platform_instance(platform: str, platform_instance: str):
    from datahub.metadata.schema_classes import (
        DataPlatformInstanceClass,
    )

    from datahub.metadata.urns import (
        DataPlatformUrn,
        DataPlatformInstanceUrn,
    )

    data_platform_urn = DataPlatformUrn(platform).urn()

    data_platform_instance_urn = DataPlatformInstanceUrn(
        data_platform_urn,
        instance=platform_instance,
    ).urn()

    data_platform_instance = DataPlatformInstanceClass(
        data_platform_urn,
        data_platform_instance_urn,
    )

    return data_platform_instance


def create_dashboard_entity(dashboard:dict):
    from datahub.metadata.schema_classes import (
        DashboardInfoClass,
        OwnershipClass,
        OwnershipTypeClass,
        OwnerClass,
        GlobalTagsClass,
        TagAssociationClass,
        EdgeClass,
        AuditStampClass,
        ChangeAuditStampsClass,
    )

    import datahub.emitter.mce_builder as builder

    from datahub.metadata.urns import (
        ChartUrn,
        DashboardUrn,
    )

    aspects = []

    dashboard_id = dashboard["Id"]
    platform_instance=dashboard["Environment"]

    data_platform_instance = create_platform_instance(PLATFORM, platform_instance)

    aspects.append(data_platform_instance)

    urn = DashboardUrn.create_from_ids(
        platform=PLATFORM,
        name=dashboard_id,
        platform_instance=platform_instance,
    ).urn()

    # Add ownership information
    owners = []
    for owner_email in dashboard.get("Owners", []):
        if owner_email and "@geotab.com" in owner_email:
            username = owner_email.split("@")[0]
            owners.append(OwnerClass(
                owner=builder.make_user_urn(username),
                type=OwnershipTypeClass.TECHNICAL_OWNER
            ))
    
    if dashboard.get("DataSteward") and "@geotab.com" in dashboard["DataSteward"]:
        username = dashboard["DataSteward"].split("@")[0]
        owners.append(OwnerClass(
            owner=builder.make_user_urn(username),
            type=OwnershipTypeClass.DATA_STEWARD
        ))
    
    if owners:
        ownership_aspect = OwnershipClass(owners=owners)
        aspects.append(ownership_aspect)

    status_tags = []
    status = dashboard.get("Status","")
    if status.lower() in ["draft","archived","published"]:
        status_tags.append(status.title())
    
    if status_tags:
        tags = GlobalTagsClass(
            tags=[TagAssociationClass(tag=builder.make_tag_urn(tag)) for tag in status_tags]
        )
        aspects.append(tags)

    # format timestamps
    for ts_prop in ["LastVisitTime"]:
        ts = dashboard.get(ts_prop)
        if ts is not None:
            dashboard[ts_prop] = ts.strftime('%Y-%m-%d %H:%M:%S %Z')

    # Prepare timestamps
    created_ts = int(dashboard["CreationTime"].timestamp() * 1000) if dashboard.get("CreationTime") else None
    modified_ts = int(dashboard["LastModifiedTime"].timestamp() * 1000) if dashboard.get("LastModifiedTime") else None

    custom_properties = {key: str(dashboard[key]) for key in dashboard.keys() & {
        "FunctionalArea", "JobsLast90Days","VisitsLast90Days",
        "LastVisitTime"
    } if dashboard[key] is not None}

    chart_edges = []

    for chart_id in dashboard.get("ChartIds",[]):
        chart_urn = ChartUrn.create_from_ids(
            PLATFORM,
            str(chart_id),
            platform_instance,
        ).urn()

        chart_edge = EdgeClass(
            destinationUrn=chart_urn
        )

        chart_edges.append(chart_edge)

    dashboard_info = DashboardInfoClass(
        title=dashboard["SupersetDashboardName"],
        description="",
        lastModified=ChangeAuditStampsClass(
            created=AuditStampClass(time=created_ts, actor="urn:li:corpuser:ingestion") if created_ts else None,
            lastModified=AuditStampClass(time=modified_ts, actor="urn:li:corpuser:ingestion") if modified_ts else None
        ),
        customProperties=custom_properties,
        externalUrl=dashboard.get("Url"),
        chartEdges=chart_edges,
        dashboardUrl=dashboard.get("Url"),
    )
    
    aspects.append(dashboard_info)

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

def ingest_superset_dashboards_to_datahub(**kwargs):
    """Main function to ingest Superset dashboards to DataHub"""
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
        connect_timeout_sec=30,  # Increased timeout
        read_timeout_sec=120
    )

    dashboards_sql = SqlTemplate(
        "sql/SupersetDashboards.sql",
        gbq_client,
        params={"status_date": status_date},
    )

    dashboards = dashboards_sql.execute()

    for i, dashboard in enumerate(dashboards):
        if i % 3000 == 0 or datetime.datetime.now() > token_refresh_time:
            refresh_iap_token()

        dashboard = dict(dashboard)
        urn, aspects = create_dashboard_entity(dashboard)

        try:
            mcps = MetadataChangeProposalWrapper.construct_many(entityUrn=urn, aspects=aspects)
            emitter.emit_mcps(mcps)
            logger.info(f"✅ Processed dashboard {urn}")
        except Exception as e:
            logger.error(f"❌ Failed to process dashboard {urn}: {str(e)}")

    
    print("Superset dashboard ingestion completed")
