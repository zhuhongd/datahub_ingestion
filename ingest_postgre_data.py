# Note:
#
# This file is not used in the DAG pipeline.
# I included here in case anyone is curious about how PostgreSQL data is ingested into DataHub.
# Note that the Postgre table list [does not] capture all tables in the PostgreSQL database,
# but it should be sufficient to support ALL GBQ-to-PostgreSQL lineage tracking.

from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    DatasetPropertiesClass,
    SchemaMetadataClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    StringTypeClass,
    GlobalTagsClass,
    TagAssociationClass,
    OtherSchemaClass
)
from datahub.emitter.mce_builder import make_tag_urn
from google.oauth2 import id_token
from google.auth.transport.requests import Request
from google.cloud import bigquery
import os
import pandas as pd

# Configuration
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = ""

GCP_PROJECT = "geotab-dna-test"
BQ_TABLE = "LineagePostgre.PostgreTableInformation"
DATAHUB_GMS_URL = "https://datahub-stg-us.geotabtest.com/" 
DATA_PLATFORM = "postgres"
DATAHUB_GMS_TOKEN = ""  #removed for safety purpose

ENV = "PROD"
IAP_CLIENT_ID = ""  #removed
iap_token = id_token.fetch_id_token(Request(), IAP_CLIENT_ID)

# Initialize clients
bq_client = bigquery.Client(project=GCP_PROJECT)
emitter = DatahubRestEmitter(
    token=DATAHUB_GMS_TOKEN,
    gms_server="https://datahub-stg-us.geotabtest.com/api/gms",
    extra_headers={"Proxy-Authorization": f"Bearer {iap_token}"},
    connect_timeout_sec=10,
    read_timeout_sec=60
)

def get_table_urn(table_name: str) -> str:
    return f"urn:li:dataset:(urn:li:dataPlatform:{DATA_PLATFORM},{table_name},{ENV})"

# Query BigQuery for metadata
query = f"""
    SELECT 
        postgre_table_name, 
        pii_information, 
        table_description, 
        column_name, 
        column_description 
    FROM `{GCP_PROJECT}.{BQ_TABLE}`
    WHERE postgre_table_name = "addresszone"
"""
df = bq_client.query(query).to_dataframe()

# Clean data - remove rows with null column names
df = df.dropna(subset=["column_name"])
df["column_name"] = df["column_name"].astype(str).str.strip()
df = df[df["column_name"] != ""]

# Group by table name
grouped = df.groupby("postgre_table_name")

for table_name, group in grouped:
    first_row = group.iloc[0]
    table_description = first_row["table_description"] or "No description available"
    dataset_urn = get_table_urn(table_name)

    print(f"Processing table: {table_name}")

    # 1. Emit DatasetProperties
    try:
        emitter.emit_mcp(
            MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=DatasetPropertiesClass(
                    description=table_description,
                    name=table_name,
                )
            )
        )
        print(f"  ✅ Emitted table properties")
    except Exception as e:
        print(f"  ❌ Failed to emit table properties: {str(e)}")

    # 2. Emit SchemaMetadata with column-level tags
    fields = []
    for _, row in group.iterrows():
        # Skip invalid rows
        if not row["column_name"] or pd.isna(row["column_name"]):
            continue
        
        # Check for PII
        pii_info = row.get("pii_information", "")
        global_tags = None
        if pii_info and "contains pii" in pii_info.lower():
            global_tags = GlobalTagsClass(
                tags=[TagAssociationClass(tag=make_tag_urn("PII"))]
            )
        
        fields.append(
            SchemaFieldClass(
                fieldPath=str(row["column_name"]),  # Ensure string type
                type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                description=row["column_description"] or "No description available",
                nativeDataType="text",
                globalTags=global_tags  # Include PII tag if applicable
            )
        )
    
    if fields:
        try:
            schema_metadata = SchemaMetadataClass(
                schemaName=table_name,
                platform=f"urn:li:dataPlatform:{DATA_PLATFORM}",
                version=0,
                hash="",
                platformSchema=OtherSchemaClass(rawSchema=""),
                fields=fields,
            )
            
            emitter.emit_mcp(
                MetadataChangeProposalWrapper(
                    entityUrn=dataset_urn,
                    aspect=schema_metadata
                )
            )
            print(f"  ✅ Emitted {len(fields)} columns with PII tags")
        except Exception as e:
            print(f"  ❌ Failed to emit schema: {str(e)}")
    else:
        print(f"  ⚠️ No valid columns found for {table_name}")

print("="*50)
print(f"Processed {len(grouped)} tables")
print(f"DataHub GMS: {DATAHUB_GMS_URL}")