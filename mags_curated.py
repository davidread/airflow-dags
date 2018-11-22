from datetime import datetime, timedelta

import airflow
from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.utils.dates import days_ago

# GLOBAL ENV VARIABLES
DB_VERSION='v1'
MAGS_IMAGE = "593291632749.dkr.ecr.eu-west-1.amazonaws.com/airflow-magistrates-data-engineering:v0.0.6"
MAGS_ROLE = "airflow_mags_data_processor"

go_time = datetime.now()
go_time.strftime("%Y-%m-%dT%H:%M:%S")

# TAR GLUE JOB SCRIPT ENVs
GLUE_JOB_BUCKET="alpha-mojap-curated-mags"
AIRFLOW_JOB_NAME="airflow_curated_tar"
GLUE_JOB_FOLDER_NAME="curated_tar"
DATABASE_RUN_DATETIME=go_time.strftime("%Y-%m-%dT%H:%M:%S")
SNAPSHOT_DATE=go_time.strftime("%Y-%m-%d")
ALLOCATED_CAPACITY="8"

# SCHEMA REBUILD ENVs
DB_REBUILD_SCRIPT = 'rebuild_databases.py'
DBS_TO_REBUILD = 'mags_processed'

task_args = {
    "depends_on_past": False,
    "email_on_failure": True,
    "owner": "isichei",
    "email": ["karik.isichei@digital.justice.gov.uk"],
}

# Define process dag that just processes raw tar and hocas data in parallel.
# Once complete rebuild processed DB only
dag = DAG(
    "mags_data_curated",
    default_args=task_args,
    description="Process mags data (HOCAS and TAR)",
    start_date=datetime.now(),
    schedule_interval=None,
)

tasks = {}

task_id = "tar-curated"
tasks[task_id] = KubernetesPodOperator(
    dag=dag,
    namespace="airflow",
    image=MAGS_IMAGE,
    env_vars={
        "GLUE_JOB_BUCKET": GLUE_JOB_BUCKET,
        "GLUE_JOB_ROLE": MAGS_ROLE,
        "AIRFLOW_JOB_NAME": AIRFLOW_JOB_NAME,
        "GLUE_JOB_FOLDER_NAME": GLUE_JOB_FOLDER_NAME,
        "DATABASE_RUN_DATETIME": DATABASE_RUN_DATETIME,
        "SNAPSHOT_DATE": SNAPSHOT_DATE,
        "ALLOCATED_CAPACITY": ALLOCATED_CAPACITY,
        "DB_VERSION": DB_VERSION
    },
    arguments=["{{ ds }}"],
    labels={"app": dag.dag_id},
    name=task_id,
    in_cluster=True,
    task_id=task_id,
    get_logs=True,
    annotations={"iam.amazonaws.com/role": MAGS_ROLE},
)

task_id = "rebuild-athena-schemas"
tasks[task_id] = KubernetesPodOperator(
    dag=dag,
    namespace="airflow",
    image=MAGS_IMAGE,
    env_vars={
        "PYTHON_SCRIPT_NAME": DB_REBUILD_SCRIPT,
        "DB_VERSION": DB_VERSION,
        "DBS_TO_REBUILD" : DBS_TO_REBUILD
    },
    arguments=["{{ ds }}"],
    labels={"app": dag.dag_id},
    name=task_id,
    in_cluster=True,
    task_id=task_id,
    get_logs=True,
    annotations={"iam.amazonaws.com/role": MAGS_ROLE},
)

# Set dependencies
tasks['curated-tar'] >> tasks["rebuild-athena-schemas"]