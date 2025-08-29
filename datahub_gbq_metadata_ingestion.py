# datahub_ingestion_functions.py
import ast
import re
import time
import datetime
import os
import json
import requests
from collections import defaultdict
from google.cloud import bigquery
from google.auth.transport.requests import Request
from google.oauth2 import id_token

DATAHUB_GMS_URL = 'https://datahub-stg-us.geotabtest.com/'

def send_request(url, param_values):
    """Send request based on regulation"""
    response = None
    wop_regulation = os.getenv('WOP_REGULATION', "commercial")
    if wop_regulation == "commercial" or wop_regulation == "conus":
        import op_iap.op_iap as iap
        response = iap.make_iap_request(url, params=param_values)
    else:
        response = requests.get(url, params=param_values)
    return response

def get_vault_secret(secret_name, **kwargs):
    """
    Fetch secrets from Vault using kwargs context
    """
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
    secret_dict = json.loads(resp.text)
    return secret_dict[1]

def fetch_table_metadata_from_gbq(gbq_client, status_date):
    """
    Fetches table-level metadata from GBQTables metadata inventory.
    """
    query = f"""
        SELECT distinct
            CreationTime, ProjectId, DatasetId, TableId, TableType,
            Description as Table_Description, Labels, StorageLastModifiedTime,
            FunctionalAreaCode, DataSteward, Owner, TotalLogicalBytes, JobsLast90Days,
            STRING_AGG(Region, ', ' ORDER BY Region) AS Region
        FROM geotab-metadata-prod.MetadataInventory_EU.GBQTables
        WHERE StatusDate = '{status_date}'

        # im not very proud of filtering data this way, but i think this is the easiest approach i could think of.
        AND tableid not like "%_2020%"
        AND tableid not like "%_2021%"
        AND tableid not like "%_2022%"
        AND tableid not like "%_2023%"
        AND tableid not like "%_2024%"

    -- 1. Skip all testing projects
        AND LOWER(projectid) NOT LIKE "%-test"


        -- 2. Only include projects end in raw, prod, and ... you get the idea.
                AND (
            ProjectId LIKE "geotab-%-raw"
            OR ProjectId LIKE "geotab-%-prod"
            OR ProjectId LIKE "geotab-%-production"
            OR ProjectId LIKE "geotab-%-staging"
            OR ProjectId LIKE "%-external"
            OR ProjectId = "geotab-bi"
            OR ProjectId = "geotab-bigdata"
            OR ProjectId = "geotab-data-platform"
            OR ProjectId = "geotab-finance"
        )

        -- 3. Ignore Terraform / GKE infra datasets, dockerlogs, test
        AND LOWER(datasetid) NOT LIKE 'tf_gke_%'
        AND LOWER(datasetid) NOT LIKE '%dockerlogs%'
        
        -- 4. In any geotab‑its‑* project, discard backups and log buckets

        AND NOT (projectid LIKE 'geotab-its-%'
                AND (datasetid LIKE 'BKP%'        -- backup datasets
                        OR LOWER(datasetid) LIKE 'logs_%'))  -- logs_*

        -- 5. Exclude de‑hashed safety benchmarking datasets
        AND NOT (projectid = 'geotab-safety-prod'
                AND datasetid IN ('SafetyBenchmarking_Dehashed_EU',
                                    'SafetyBenchmarking_Dehashed_US'))

        -- 6. Drop GIS importer staging area in mapserver
        AND NOT (projectid = 'geotab-mapserver-prod'
                AND datasetid = 'gis_importer')

        -- 7. Inside AltitudeSavedAnalysis* keep only the two reference tables
        AND NOT (projectid = 'geotab-its-prod'
                AND (datasetid LIKE 'AltitudeSavedAnalysis%'
                        OR datasetid LIKE 'Converted_AltitudeSavedAnalysis%')
                AND tableid NOT IN ('AnalysisLabels', 'AnalysisMetadata'))


        -- 8. In geotab‑gdc‑prod skip tables containing “backup” or ending “failed”
        AND NOT (projectid = 'geotab-gdc-prod'
                AND (LOWER(tableid) LIKE '%backup%'
                        OR LOWER(tableid) LIKE '%failed'))

        -- exclude cloudaudit_googleapis_com tables
        AND tableid not like "%cloudaudit_googleapis_com%"

        -- 10. Discard table backups in any geotab‑its‑* project
        AND NOT (projectid LIKE 'geotab-its-%'
                AND tableid LIKE 'BKP%')
        AND NOT (projectid = 'geotab-its-prod'
                AND datasetid LIKE 'CACHE_TABLES%')
        
        GROUP BY 
        CreationTime, ProjectId, DatasetId, TableId, TableType, 
        Description, Labels, StorageLastModifiedTime, FunctionalAreaCode, 
        DataSteward, Owner, TotalLogicalBytes, JobsLast90Days
 
    """
    rows = [dict(row) for row in gbq_client.query(query).result()]
    
    # Aggregate regions per table
    table_groups = defaultdict(list)
    for row in rows:
        key = (row['ProjectId'], row['DatasetId'], row['TableId'])
        table_groups[key].append(row)
    
    aggregated = []
    for key, group in table_groups.items():
        regions = sorted(set(rec['Region'] for rec in group))
        base = group[0].copy()
        base['Region'] = ", ".join(regions)
        aggregated.append(base)
        
    return aggregated

def fetch_view_metadata_from_gbq(gbq_client, status_date):
    """
    Fetches view-level metadata from GBQViews metadata inventory and joins with InformationSchema.Views to get view definition.
    """
    query = f"""
        WITH GBQviews AS (
            SELECT distinct
                CreationTime, ProjectId, DatasetId, ViewId, Region, TableType,
                Description, Labels, StorageLastModifiedTime,
                FunctionalAreaCode, DataSteward, Owner, TotalLogicalBytes, JobsLast90Days
            FROM `geotab-metadata-prod.MetadataInventory_EU.GBQViews`
            WHERE StatusDate = '{status_date}'

            -- Apply the same filters as for tables
            AND ViewId not like "%_2020%"
            AND ViewId not like "%_2021%"
            AND ViewId not like "%_2022%"
            AND ViewId not like "%_2023%"
            AND ViewId not like "%_2024%"

            -- 1. Skip all testing projects, exclude everything under "security-integration"
            AND LOWER(projectid) NOT LIKE "%-test"

            -- 2. Only include projects end in raw, prod, and ... you get the idea.
            AND (
                 ProjectId LIKE "geotab-%-raw"
                OR ProjectId LIKE "geotab-%-prod"
                OR ProjectId LIKE "geotab-%-production"
                OR ProjectId LIKE "geotab-%-staging"
                OR ProjectId LIKE "%-external"
                OR ProjectId = "geotab-bi"
                OR ProjectId = "geotab-bigdata"
                OR ProjectId = "geotab-data-platform"
                OR ProjectId = "geotab-finance"
            )

            -- 3. Ignore Terraform / GKE infra datasets, dockerlogs, test
            AND LOWER(datasetid) NOT LIKE 'tf_gke_%'
            AND LOWER(datasetid) NOT LIKE '%dockerlogs%'

            -- 4. In any geotab‑its‑* project, discard backups and log buckets
            AND NOT (projectid LIKE 'geotab-its-%'
                    AND (datasetid LIKE 'BKP%'        -- backup datasets
                            OR LOWER(datasetid) LIKE 'logs_%'))  -- logs_*

            -- 5. Exclude de‑hashed safety benchmarking datasets
            AND NOT (projectid = 'geotab-safety-prod'
                    AND datasetid IN ('SafetyBenchmarking_Dehashed_EU',
                                        'SafetyBenchmarking_Dehashed_US'))

            -- 6. Drop GIS importer staging area in mapserver
            AND NOT (projectid = 'geotab-mapserver-prod'
                    AND datasetid = 'gis_importer')

            -- 7. Inside AltitudeSavedAnalysis* keep only the two reference tables
            AND NOT (projectid = 'geotab-its-prod'
                    AND (datasetid LIKE 'AltitudeSavedAnalysis%'
                            OR datasetid LIKE 'Converted_AltitudeSavedAnalysis%')
                    AND ViewId NOT IN ('AnalysisLabels', 'AnalysisMetadata'))

            -- 8. In geotab‑gdc‑prod skip tables containing "backup" or ending "failed"
            AND NOT (projectid = 'geotab-gdc-prod'
                    AND (LOWER(ViewId) LIKE '%backup%'
                            OR LOWER(ViewId) LIKE '%failed'))

            -- exclude cloudaudit_googleapis_com tables
            AND ViewId not like "%cloudaudit_googleapis_com%"

            -- 10. Discard table backups in any geotab‑its‑* project
            AND NOT (projectid LIKE 'geotab-its-%'
                    AND ViewId LIKE 'BKP%')
            AND NOT (projectid = 'geotab-its-prod'
                    AND datasetid LIKE 'CACHE_TABLES%')
        )
        SELECT 
            CreationTime, ProjectId, DatasetId, ViewId as TableId, g.Region, TableType,
            Description as Table_Description, Labels, StorageLastModifiedTime,
            FunctionalAreaCode, DataSteward, Owner, TotalLogicalBytes, JobsLast90Days, 
            v.view_definition
        FROM GBQviews g 
        JOIN `geotab-metadata-prod.InformationSchema.Views` v
        ON g.ProjectId = v.project_id 
        AND g.DatasetId = v.dataset_id
        AND g.ViewId = v.view_id
        AND v.pull_time = '{status_date}'
    """
    rows = [dict(row) for row in gbq_client.query(query).result()]
    
    # Aggregate regions per view
    view_groups = defaultdict(list)
    for row in rows:
        key = (row['ProjectId'], row['DatasetId'], row['TableId'])
        view_groups[key].append(row)
    
    aggregated = []
    for key, group in view_groups.items():
        regions = sorted(set(rec['Region'] for rec in group))
        base = group[0].copy()
        base['Region'] = ", ".join(regions)
        aggregated.append(base)
        
    return aggregated

def fetch_column_metadata_from_gbq(gbq_client, status_date):
    """
    Fetches column metadata from BigQuery's INFORMATION_SCHEMA.
    """
    query = f"""
        SELECT 
        distinct
            project_id, 
            dataset_id, 
            table_id, 
            region,
            field_path,
            data_type,
            description AS column_description,
            is_nullable, 
            is_partitioning_column,
            ordinal_position
        FROM `geotab-metadata-prod.InformationSchema.Columns`
        WHERE DATE(execution_date) = DATE("{status_date}")

        AND table_id not like "%_2020%"
        AND table_id not like "%_2021%"
        AND table_id not like "%_2022%"
        AND table_id not like "%_2023%"
        AND table_id not like "%_2024%"

    -- 1. Skip all testing projects, exclude everything under "security-integration"
        AND LOWER(project_id) NOT LIKE "%-test"
        
        AND (
                 Project_Id LIKE "geotab-%-raw"
                OR Project_Id LIKE "geotab-%-prod"
                OR Project_Id LIKE "geotab-%-production"
                OR Project_Id LIKE "geotab-%-staging"
                OR Project_Id LIKE "%-external"
                OR Project_Id = "geotab-bi"
                OR Project_Id = "geotab-bigdata"
                OR Project_Id = "geotab-data-platform"
                OR Project_Id = "geotab-finance"
            )

        -- 3. Ignore Terraform / GKE infra datasets, dockerlogs, test
        AND LOWER(dataset_id) NOT LIKE 'tf_gke_%'
        AND LOWER(dataset_id) NOT LIKE '%dockerlogs%'

        AND NOT (project_id LIKE 'geotab-its-%'
                AND (dataset_id LIKE 'BKP%'        -- backup datasets
                        OR LOWER(dataset_id) LIKE 'logs_%'))  -- logs_*

        -- 5. Exclude de‑hashed safety benchmarking datasets
        AND NOT (project_id = 'geotab-safety-prod'
                AND dataset_id IN ('SafetyBenchmarking_Dehashed_EU',
                                    'SafetyBenchmarking_Dehashed_US'))

        -- 6. Drop GIS importer staging area in mapserver
        AND NOT (project_id = 'geotab-mapserver-prod'
                AND dataset_id = 'gis_importer')

        -- 7. Inside AltitudeSavedAnalysis* keep only the two reference tables
        AND NOT (project_id = 'geotab-its-prod'
                AND (dataset_id LIKE 'AltitudeSavedAnalysis%'
                        OR dataset_id LIKE 'Converted_AltitudeSavedAnalysis%')
                AND table_id NOT IN ('AnalysisLabels', 'AnalysisMetadata'))


        -- 8. In geotab‑gdc‑prod skip tables containing “backup” or ending “failed”
        AND NOT (project_id = 'geotab-gdc-prod'
                AND (LOWER(table_id) LIKE '%backup%'
                        OR LOWER(table_id) LIKE '%failed'))

        -- exclude cloudaudit_googleapis_com tables
        AND table_id not like "%cloudaudit_googleapis_com%"

        -- 10. Discard table backups in any geotab‑its‑* project
        AND NOT (project_id LIKE 'geotab-its-%'
                AND table_id LIKE 'BKP%')
        AND NOT (project_id = 'geotab-its-prod'
                AND dataset_id LIKE 'CACHE_TABLES%')
        order by ordinal_position
    """
    rows = [dict(row) for row in gbq_client.query(query).result()]
    
    # Deduplicate columns by field_path
    deduped = {}
    for row in rows:
        key = (row['project_id'], row['dataset_id'], row['table_id'], row['field_path'])
        if key not in deduped:
            deduped[key] = row
    return list(deduped.values())

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

def parse_labels(labels_str):
    """Convert BigQuery STRUCT labels to a readable key-value string"""
    if not labels_str or labels_str == "[]" or labels_str == "None":
        return ""
    
    try:
        cleaned = labels_str.replace("STRUCT", "").replace('"', "'")
        label_list = ast.literal_eval(cleaned)
        return ", ".join(f"{k.strip()}={v.strip()}" for k, v in label_list)
    except (SyntaxError, ValueError, TypeError) as e:
        print(f"Error parsing labels '{labels_str}': {e}")
        return labels_str

# def get_existing_institutional_memory(urn, token, iap_token):
#     """Fetch existing institutional memory aspect for an entity"""
#     from datahub.metadata.schema_classes import InstitutionalMemoryClass
#     headers = {
#         "Authorization": f"Bearer {token}",
#         "Proxy-Authorization": f"Bearer {iap_token}",
#         "Content-Type": "application/json"
#     }
#     url = f"{DATAHUB_GMS_URL}/aspects?urn={urn}&aspect=institutionalMemory"
    
#     try:
#         response = requests.get(url, headers=headers)
#         if response.status_code == 200:
#             data = response.json()
#             if 'institutionalMemory' in data:
#                 return InstitutionalMemoryClass.from_obj(data['institutionalMemory'])
#         return None
#     except Exception as e:
#         print(f"Error fetching institutional memory: {str(e)}")
#         return None

# def add_access_request_link(urn, token, iap_token, emitter):
#     """Add access request link to documentation section if not already present"""

#     from datahub.metadata.schema_classes import (
#         InstitutionalMemoryClass, 
#         InstitutionalMemoryMetadataClass, 
#         AuditStampClass
#     )
#     from datahub.emitter.mcp import MetadataChangeProposalWrapper
#     from datahub.metadata.schema_classes import ChangeTypeClass

#     ACCESS_REQUEST_URL = "https://geotab.atlassian.net/servicedesk/customer/portal/1/group/8/create/22"
#     ACCESS_DESCRIPTION = "Request access to this dataset"
    
#     existing_memory = get_existing_institutional_memory(urn, token, iap_token)
#     elements = []
    
#     link_exists = False
#     if existing_memory:
#         elements = existing_memory.elements
#         for element in elements:
#             if element.url == ACCESS_REQUEST_URL:
#                 link_exists = True
#                 break
    
#     if not link_exists:
#         new_element = InstitutionalMemoryMetadataClass(
#             url=ACCESS_REQUEST_URL,
#             description=ACCESS_DESCRIPTION,
#             createStamp=AuditStampClass(
#                 time=int(time.time() * 1000),
#                 actor="urn:li:corpuser:ingestion"
#             )
#         )
#         elements.append(new_element)
        
#         institutional_memory = InstitutionalMemoryClass(elements=elements)
        
#         emitter.emit_mcp(MetadataChangeProposalWrapper(
#             entityUrn=urn,
#             aspect=institutional_memory,
#             changeType=ChangeTypeClass.UPSERT
#         ))
#         print(f"✅ Added access request link to {urn.split('/')[-1]}")


# Wrapping these functions inside a larger function because, in Airflow, the best practice is to import libraries only within local functions. 
# And importing libraries repeatedly in every single function would be a little troublesome. 
# Additionally, if you don't follow this practice, publishing the pipeline will fail and raise errors.  (- -#)

def ingest_metadata_to_datahub(table_records, column_records, gbq_client, emitter, token, iap_token, iap_client_id):
    """Main ingestion workflow"""
    from zoneinfo import ZoneInfo
    from datahub.emitter.rest_emitter import DatahubRestEmitter
    from datahub.emitter.mcp import MetadataChangeProposalWrapper
    import datahub.emitter.mce_builder as builder
    from datahub.metadata.schema_classes import (
        DatasetPropertiesClass,
        TimeStampClass,
        SchemaMetadataClass,
        SchemaFieldClass,
        SchemaFieldDataTypeClass,
        BooleanTypeClass,
        StringTypeClass,
        NumberTypeClass,
        DateTypeClass,
        TimeTypeClass,
        NullTypeClass,
        BytesTypeClass,
        OtherSchemaClass,
        InstitutionalMemoryClass,
        InstitutionalMemoryMetadataClass,
        AuditStampClass,
        RecordTypeClass,
        ArrayTypeClass,
        ChangeTypeClass,
        OwnershipClass,
        OwnerClass,
        OwnershipTypeClass,
        SubTypesClass,
        ViewPropertiesClass
    )

    # Token refresh mechanism
    token_refresh_time = datetime.datetime.now() + datetime.timedelta(minutes=50)

    # Helper functions scoped inside ingest_metadata_to_datahub
    def refresh_iap_token():
        """Refresh IAP token and update emitter"""
        nonlocal iap_token, token_refresh_time
        new_iap_token = id_token.fetch_id_token(Request(), iap_client_id)
        
        # Update emitter headers
        emitter._session.headers.update({
            "Proxy-Authorization": f"Bearer {new_iap_token}"
        })
        
        iap_token = new_iap_token
        token_refresh_time = datetime.datetime.now() + datetime.timedelta(minutes=50)
        print("🔄 Refreshed IAP token")

    def get_existing_institutional_memory(urn):
        headers = {
            "Authorization": f"Bearer {token}",
            "Proxy-Authorization": f"Bearer {iap_token}",
            "Content-Type": "application/json"
        }
        url = f"{DATAHUB_GMS_URL}/aspects?urn={urn}&aspect=institutionalMemory"
        
        try:
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                data = response.json()
                if 'institutionalMemory' in data:
                    return InstitutionalMemoryClass.from_obj(data['institutionalMemory'])
            return None
        except Exception as e:
            print(f"Error fetching institutional memory: {str(e)}")
            return None

    def create_access_request_link(urn):
        ACCESS_REQUEST_URL = "https://geotab.atlassian.net/servicedesk/customer/portal/1/group/8/create/22"
        ACCESS_DESCRIPTION = "Request access to this dataset"
        
        existing_memory = get_existing_institutional_memory(urn)
        elements = []
        
        link_exists = False
        if existing_memory:
            elements = existing_memory.elements
            for element in elements:
                if element.url == ACCESS_REQUEST_URL:
                    link_exists = True
                    break
        
        if not link_exists:
            new_element = InstitutionalMemoryMetadataClass(
                url=ACCESS_REQUEST_URL,
                description=ACCESS_DESCRIPTION,
                createStamp=AuditStampClass(
                    time=int(time.time() * 1000),
                    actor="urn:li:corpuser:ingestion"
                )
            )
            elements.append(new_element)
            
            institutional_memory = InstitutionalMemoryClass(elements=elements)
            return institutional_memory

    def get_field_type(bq_type):
        """Maps BigQuery data types to DataHub schema types"""
        if bq_type.startswith("ARRAY<STRUCT") or bq_type.startswith("ARRAY<"):
            return ArrayTypeClass()
        elif "STRUCT" in bq_type:
            return RecordTypeClass()
        
        type_mapping = {
            "STRING": StringTypeClass(),
            "BYTES": BytesTypeClass(),
            "INT64": NumberTypeClass(),
            "FLOAT64": NumberTypeClass(),
            "BOOL": BooleanTypeClass(),
            "BOOLEAN": BooleanTypeClass(),
            "TIMESTAMP": TimeTypeClass(),
            "DATE": DateTypeClass(),
            "TIME": TimeTypeClass(),
            "DATETIME": TimeTypeClass(),
            "NUMERIC": NumberTypeClass(),
            "BIGNUMERIC": NumberTypeClass(),
            "GEOGRAPHY": StringTypeClass(),
        }
        return type_mapping.get(bq_type, NullTypeClass())

    def build_compatible_schema_fields(cols):
        """Create field list with proper hierarchy"""
        valid_cols = [col for col in cols if col['field_path'] is not None]
        sorted_cols = sorted(valid_cols, key=lambda x: x['field_path'])
        
        fields = []
        for col in sorted_cols:
            field_path = col['field_path']
            
            col_description = col.get('column_description')
            if not col_description or col_description.strip() == "":
                col_description = None
                
            fields.append(SchemaFieldClass(
                fieldPath=field_path,
                type=SchemaFieldDataTypeClass(type=get_field_type(col['data_type'])),
                nativeDataType=col.get('data_type', 'UNKNOWN'),
                description=col_description,
                nullable=col.get('is_nullable', 'YES') == 'YES',
                isPartitioningKey=col.get('is_partitioning_column', 'YES') == 'YES'
            ))
        
        return fields

    # Create mapping for column metadata
    column_map = {}
    for col in column_records:
        key = (col['project_id'], col['dataset_id'], col['table_id'])
        if key not in column_map:
            column_map[key] = []
        column_map[key].append(col)
    
    # Process each table/view
    for i, record in enumerate(table_records, 1):
        # Refresh token every 500 tables or when near expiration
        if i % 500 == 0 or datetime.datetime.now() > token_refresh_time:
            refresh_iap_token()

        project_id = record['ProjectId']
        dataset_id = record['DatasetId']
        table_id = record['TableId']
        table_type = record.get('TableType', '').upper()
        
        # Determine if this is a view
        is_view = table_type in ('VIEW', 'MATERIALIZED VIEW', 'SNAPSHOT')
        
        dataset_name = f"{project_id}.{dataset_id}.{table_id}"
        environment = extract_env_from_project(project_id)
        urn = builder.make_dataset_urn(platform="bigquery", name=dataset_name, env=environment)
        aspects = []

        try:
            # 1. Construct BigQuery URL
            url = (
                "https://console.cloud.google.com/bigquery?ws=!1m5!1m4!4m3"
                f"!1s{project_id}!2s{dataset_id}!3s{table_id}"
            )
            
            formatted_creation = ""
            formatted_modification = ""
            created_ts = None
            last_modified_ts = None
            
            if record.get("CreationTime"):
                formatted_creation = record["CreationTime"].strftime('%Y-%m-%d %H:%M:%S UTC')
                created_ms = int(record["CreationTime"].timestamp() * 1000)
                created_ts = TimeStampClass(time=created_ms)
            
            if record.get("StorageLastModifiedTime"):
                formatted_modification = record["StorageLastModifiedTime"].strftime('%Y-%m-%d %H:%M:%S UTC')
                modified_ms = int(record["StorageLastModifiedTime"].timestamp() * 1000)
                last_modified_ts = TimeStampClass(time=modified_ms)

            # 2. Prepare dataset properties
            description = record["Table_Description"] or "No description available"
            toronto_tz = ZoneInfo("America/Toronto")
            now = datetime.datetime.now(toronto_tz)
            offset_hours = int(now.utcoffset().total_seconds() // 3600)
            last_updated_str = f"{now.strftime('%Y-%m-%d, %H:%M:%S')} GMT{offset_hours:+d}"
            
            custom_props = {
                "CreationTime": formatted_creation,
                "Region": record["Region"],
                "TableType": record["TableType"],
                "Labels": parse_labels(str(record.get("Labels", ""))),
                "StorageLastModifiedTime": formatted_modification,
                "FunctionalAreaCode": str(record.get("FunctionalAreaCode", "")),
                "Owner": str(record.get("Owner", "")),
                "TotalLogicalBytes": str(record.get("TotalLogicalBytes", "")),
                "JobsLast90Days": str(record.get("JobsLast90Days", "")),
                "last_updated_in_datahub": last_updated_str
            }
            
            # Parse labels for access control
            labels_dict = {}
            if record.get("Labels") and record["Labels"] != "[]" and record["Labels"] != "None":
                try:
                    cleaned = str(record["Labels"]).replace("STRUCT", "").replace('"', "'")
                    label_list = ast.literal_eval(cleaned)
                    labels_dict = {k.strip(): v.strip() for k, v in label_list}
                except Exception as e:
                    print(f"Error parsing labels to dict: {e}")
            
            # Add access request link if needed
            if labels_dict.get("clearance") and labels_dict["clearance"] != "1":
                access_request = create_access_request_link(urn)
                if access_request:
                    aspects.append(access_request)
                    print(f"✅ Added access request link to {urn.split('/')[-1]}")
            
            # For views, add ViewProperties and SubTypes aspects
            if is_view:
                view_definition = record.get('view_definition', '')
                
                view_properties = ViewPropertiesClass(
                    materialized=(table_type == "MATERIALIZED VIEW"),
                    viewLanguage="SQL",
                    viewLogic=view_definition
                )
                
                aspects.append(view_properties)
                
                subtypes = SubTypesClass(typeNames=["view"])
                aspects.append(subtypes)
                
                print(f"✅ Added view-specific aspects for {table_id}")
            
            # Add partitioning info if available and not a view
            key = (project_id, dataset_id, table_id)
            if not is_view and key in column_map:
                # Create partitioning info for this table
                partitioning_cols = []
                time_based = False
                
                for col in column_map[key]:
                    field_path = col.get('field_path')
                    if not field_path:
                        continue
                        
                    # Capture actual partitioning columns
                    if col.get('is_partitioning_column') == 'YES' and not field_path.startswith('_'):
                        partitioning_cols.append(field_path)
                    
                    # Check for time-based partitioning pseudo columns
                    if field_path in ('_PARTITIONTIME', '_PARTITIONDATE'):
                        time_based = True
                
                partitioning_info = ""
                if partitioning_cols:
                    partitioning_info = f"{', '.join(partitioning_cols)}"
                elif time_based:
                    partitioning_info = "TIME: Ingestion time"
                
                if partitioning_info:
                    custom_props["partitioning_type"] = partitioning_info
            
            # Create dataset properties
            new_props = DatasetPropertiesClass(
                description=description,
                customProperties=custom_props,
                externalUrl=url,
                lastModified=last_modified_ts,
                created=created_ts,
            )

            aspects.append(new_props)
            
            # Add ownership information
            owner_str = str(record.get("Owner") or "").strip()
            steward_str = str(record.get("DataSteward") or "").strip()
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
                
                # Create ownership aspect
                ownership_aspect = OwnershipClass(owners=owners)
                aspects.append(ownership_aspect)
                print(f"✅ Added {len(owners)} owner(s) to {table_id}")
            
            # Emit column metadata
            if key in column_map:
                cols = column_map[key]
                
                print(f"\nColumns for {'view' if is_view else 'table'} {table_id}:")
                for col in cols:
                    print(f"  {col['field_path']} ({col['data_type']})")
                
                fields = build_compatible_schema_fields(cols)
                
                schema_metadata = SchemaMetadataClass(
                    schemaName=table_id,
                    platform=builder.make_data_platform_urn("bigquery"),
                    version=0,
                    hash="",
                    platformSchema=OtherSchemaClass(rawSchema=""),
                    fields=fields
                )

                aspects.append(schema_metadata)
                print(f"✅ Updated {len(fields)} columns for {table_id}")
            else:
                print(f"⚠️ No column metadata found for {table_id}")

            # Emit all aspects at once
            if aspects:
                mcps = []
                for aspect in aspects:
                    mcp = MetadataChangeProposalWrapper(
                        entityUrn=urn,
                        aspect=aspect,
                        changeType=ChangeTypeClass.UPSERT
                    )
                    mcps.append(mcp)
                
                emitter.emit_mcps(mcps)
                print(f"✅ Emitted all metadata for {table_id}")
                
        except Exception as e:
            print(f"❌ Failed to update {table_id}: {str(e)}")
            import traceback
            traceback.print_exc()
            continue

def ingest_gbq_metadata(**kwargs):
    """Main function to run DataHub metadata ingestion"""
    from datahub.emitter.rest_emitter import DatahubRestEmitter
    from datahub.emitter.mcp import MetadataChangeProposalWrapper
    import datahub.emitter.mce_builder as builder
    from datahub.metadata.schema_classes import (
        DatasetPropertiesClass,
        TimeStampClass,
        SchemaMetadataClass,
        SchemaFieldClass,
        SchemaFieldDataTypeClass,
        BooleanTypeClass,
        StringTypeClass,
        NumberTypeClass,
        DateTypeClass,
        TimeTypeClass,
        NullTypeClass,
        BytesTypeClass,
        OtherSchemaClass,
        InstitutionalMemoryClass,
        InstitutionalMemoryMetadataClass,
        AuditStampClass,
        RecordTypeClass,
        ArrayTypeClass,
        ChangeTypeClass,
        OwnershipClass,
        OwnerClass,
        OwnershipTypeClass,
        SubTypesClass,
        ViewPropertiesClass
    )

    dag_run = kwargs["dag_run"]
    execution_date = kwargs['execution_date']
    status_date = execution_date.strftime('%Y-%m-%d')
    
    print(f"Starting DataHub ingestion for status_date: {status_date}")
    
    # Get secrets from Vault
    datahub_iap_token = get_vault_secret("datahub_iap_token", **kwargs)
    datahub_access_token = get_vault_secret("datahub_personal_token_0714", **kwargs)
    
    IAP_CLIENT_ID = datahub_iap_token['token']
    DATAHUB_GMS_TOKEN = datahub_access_token['token']
    
    # Initialize BigQuery client
    gbq_client = bigquery.Client(project="geotab-metadata-prod")
    
    # Set up DataHub emitter with IAP token
    iap_token = id_token.fetch_id_token(Request(), IAP_CLIENT_ID)
    emitter = DatahubRestEmitter(
        token=DATAHUB_GMS_TOKEN,
        gms_server="https://datahub-stg-us.geotabtest.com/api/gms",
        extra_headers={
        "Proxy-Authorization": f"Bearer {iap_token}",
        "Authorization": f"Bearer {DATAHUB_GMS_TOKEN}"  # Explicitly set this
    },
        connect_timeout_sec=10,
        read_timeout_sec=60
    )
    
    # Fetch metadata
    table_metadata = fetch_table_metadata_from_gbq(gbq_client, status_date)
    view_metadata = fetch_view_metadata_from_gbq(gbq_client, status_date)
    all_metadata = table_metadata + view_metadata

    column_metadata = fetch_column_metadata_from_gbq(gbq_client, status_date)
    
    # Run ingestion
    ingest_metadata_to_datahub(all_metadata, column_metadata, gbq_client, emitter, DATAHUB_GMS_TOKEN, iap_token, IAP_CLIENT_ID)