# Main DAG file
import datetime
import os
from pathlib import Path
import json
import airflow
from airflow.providers.google.cloud.sensors.bigquery import BigQueryTablePartitionExistenceSensor
from airflow.operators.python import PythonOperator

from dna_datahub_ingestion.datahub_gbq_metadata_ingestion import ingest_gbq_metadata
from dna_datahub_ingestion.datahub_gbq_lineage_ingestion import ingest_gbq_lineage_to_datahub
from dna_datahub_ingestion.datahub_superset_charts_ingestion import ingest_superset_charts_to_datahub
from dna_datahub_ingestion.datahub_superset_datasets_ingestion import ingest_superset_datasets_to_datahub 
from dna_datahub_ingestion.datahub_airflow_ingestion import (
    ingest_airflow_pipelines_to_datahub, 
    ingest_task_lineage_to_datahub
)

# DAG Definition
def main():
    dag_args = {
        "dag_id": os.path.splitext(os.path.basename(__file__))[0],
        "schedule": "30 10 * * *",
        "max_active_runs": 1,
        # "dagrun_timeout": datetime.timedelta(minutes=119),
        "template_searchpath": ["/home/airflow/gcs/dags/dna_datahub_ingestion/"],
        "catchup": False,
        "is_paused_upon_creation": True,
        "default_args": {
            "start_date": datetime.datetime(2025, 7, 26),
            "owner": "Hongda",
            "email": ["hongdazhu@geotab.com"],
            "depends_on_past": False,
            "retries": 1,
            "retry_delay": datetime.timedelta(seconds=30),
            "email_on_failure": True,
            "email_on_retry": False,
            "use_legacy_sql": False,
            "time_partitioning": {"type": "DAY"},
            "priority": "BATCH",
        },
    }

    doc_md = f"""
    **Author**: {dag_args["default_args"]["owner"]}\n
    **Email**: {', '.join(dag_args["default_args"]["email"])}\n
    **Purpose**: Ingest metadata for Google Bigquery tables, Airflow pipelines and tasks, Superset datasets, charts, dashboard and lineage into Datahub. \n
    This DAG ingests table and column metadata from BigQuery into DataHub, Airflow pipeline and task metadata, and establishes lineage relationships.\n
    {load_gitlab_metadata('project-metadata.json')}
    """.replace("    ", "")
    
    dag_args["doc_md"] = doc_md
    create_dag(dag_args)

def create_dag(dag_args):
    wop_stage = os.environ.get('WOP_STAGE', 'staging')
    wop_regulation = os.getenv('WOP_REGULATION', "commercial")
    
    sensor_default_args = {
        'partition_id': '{{ macros.ds_format(macros.ds_add(ds, -1), "%Y-%m-%d", "%Y%m%d") }}',
        'location': 'EU',
        'poke_interval': 60,
        'mode': 'reschedule',   # optional: avoids slot blocking
    }
    
    with airflow.DAG(**dag_args) as dag:

        # Check GBQ tables metadata
        check_gbq_metadata_partition = BigQueryTablePartitionExistenceSensor(
            task_id='check_gbq_metadata_partition',
            project_id='geotab-metadata-prod',
            dataset_id='MetadataInventory_EU',
            table_id='GBQTables',
            **sensor_default_args,
        )

        # Check Superset charts metadata
        check_superset_charts_partition = BigQueryTablePartitionExistenceSensor(
            task_id='check_superset_charts_partition',
            project_id='geotab-metadata-prod',
            dataset_id='MetadataInventory_EU',
            table_id='SupersetCharts',
            **sensor_default_args,
        )
        
        # Check Superset datasets metadata
        check_superset_datasets_partition = BigQueryTablePartitionExistenceSensor(
            task_id='check_superset_datasets_partition',
            project_id='geotab-metadata-prod',
            dataset_id='MetadataInventory_EU',
            table_id='SupersetDatasets',
            **sensor_default_args,
        )

        # Check GBQ table-to-table lineage
        check_lineage_partition = BigQueryTablePartitionExistenceSensor(
            task_id='check_lineage_partition',
            project_id='geotab-metadata-prod',
            dataset_id='Lineage',
            table_id='TableToTable',
            **sensor_default_args,
        )

        # Check GBQ view partition
        check_gbq_views_partition = BigQueryTablePartitionExistenceSensor(
            task_id='check_gbq_views_partition',
            project_id='geotab-metadata-prod',
            dataset_id='MetadataInventory_EU',
            table_id='GBQViews',
            **sensor_default_args,
        )

        # Check Airflow pipelines metadata
        check_airflow_pipelines_partition = BigQueryTablePartitionExistenceSensor(
            task_id='check_airflow_pipelines_partition',
            project_id='geotab-metadata-prod',
            dataset_id='MetadataInventory_EU',
            table_id='AirflowPipelines',
            **sensor_default_args,
        )

        # Check Airflow task lineage metadata
        check_airflow_task_lineage_partition = BigQueryTablePartitionExistenceSensor(
            task_id='check_airflow_task_lineage_partition',
            project_id='geotab-metadata-prod',
            dataset_id='MetadataInventory_EU',
            table_id='AirflowTaskLineage',
            **sensor_default_args,
        )

        # GBQ metadata ingestion
        run_gbq_metadata_ingestion = PythonOperator(
            task_id='run_gbq_metadata_ingestion',
            python_callable=ingest_gbq_metadata,
            provide_context=True,  # keep only if your callable expects **kwargs
        )
        
        # GBQ table-to-table lineage
        run_lineage_ingestion = PythonOperator(
            task_id='run_gbq_table_to_table_lineage_ingestion',
            python_callable=ingest_gbq_lineage_to_datahub,
            provide_context=True,
        )

        # Superset chart ingestion task
        run_superset_charts_ingestion = PythonOperator(
            task_id='run_superset_chart_ingestion',
            python_callable=ingest_superset_charts_to_datahub,
            provide_context=True,
        )

        # Superset datasets ingestion task
        run_superset_datasets_ingestion = PythonOperator(
            task_id='run_superset_dataset_ingestion',
            python_callable=ingest_superset_datasets_to_datahub,
            provide_context=True,
        )

        # Airflow pipelines ingestion
        run_airflow_pipelines_ingestion = PythonOperator(
            task_id='run_airflow_pipelines_ingestion',
            python_callable=ingest_airflow_pipelines_to_datahub,
            provide_context=True,
        )

        # Airflow task lineage ingestion
        run_airflow_task_lineage_ingestion = PythonOperator(
            task_id='run_airflow_task_lineage_ingestion',
            python_callable=ingest_task_lineage_to_datahub,
            provide_context=True,
        )

        [check_gbq_metadata_partition, check_gbq_views_partition] >> run_gbq_metadata_ingestion

        check_lineage_partition >> run_lineage_ingestion
        check_superset_charts_partition >> run_superset_charts_ingestion
        check_superset_datasets_partition >> run_superset_datasets_ingestion
        check_airflow_pipelines_partition >> run_airflow_pipelines_ingestion
        check_airflow_task_lineage_partition >> run_airflow_task_lineage_ingestion

    globals()[dag.dag_id] = dag

def load_gitlab_metadata(metadata_file):
    dag_file = Path(__file__)
    if os.path.exists(project_metadata_json := f"{dag_file.parents[0]}/{metadata_file}"):
        with open(project_metadata_json) as f:
            return "\n\n".join([f"**{k}**: {v}" for k, v in json.load(f).items()])
    else:
        return "No Metadata"

main()