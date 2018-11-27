from datetime import datetime, timedelta

import airflow
from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.utils.dates import days_ago

# GLOBAL ENV VARIABLES
DB_VERSION='v1'
MAGS_IMAGE = "593291632749.dkr.ecr.eu-west-1.amazonaws.com/airflow-magistrates-data-engineering:v0.0.7"
MAGS_ROLE = "airflow_mags_data_processor"

# TAR/HOCAS PROCESS SCRIPT ENVs
S3_RELATIVE_FOLDER_PATHS = ""
TAR_PROCESS_SCRIPT = "tar_raw_to_process.py"
HOCAS_PROCESS_SCRIPT = "hocas_raw_to_process.py"

# rebuild_databases.py tar_raw_to_process.py hocas_raw_to_process.py run_glue_job.py

# TAR GLUE JOB SCRIPT ENVs
GLUE_JOB_BUCKET="alpha-mojap-curated-mags"
GLUE_JOB_ROLE="alpha_user_isichei"
AIRFLOW_JOB_NAME="airflow_curated_tar"
GLUE_JOB_FOLDER_NAME="curated_tar"
DATABASE_RUN_DATETIME="2018-11-16T00:00:00"
SNAPSHOT_DATE="2018-11-16"
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
    "mags_data_raw_to_processed",
    default_args=task_args,
    description="Process mags data (HOCAS and TAR)",
    start_date=datetime.now(),
    schedule_interval=None,
)

tasks = {}

task_id = "process-tar"
tasks[task_id] = KubernetesPodOperator(
    dag=dag,
    namespace="airflow",
    image=MAGS_IMAGE,
    env_vars={
        "PYTHON_SCRIPT_NAME": TAR_PROCESS_SCRIPT,
        "S3_RELATIVE_FOLDER_PATHS": S3_RELATIVE_FOLDER_PATHS,
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

task_id = "process-hocas"
tasks[task_id] = KubernetesPodOperator(
    dag=dag,
    namespace="airflow",
    image=MAGS_IMAGE,
    env_vars={
        "PYTHON_SCRIPT_NAME": HOCAS_PROCESS_SCRIPT,
        "S3_RELATIVE_FOLDER_PATHS": S3_RELATIVE_FOLDER_PATHS,
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
tasks['process-tar'] >> tasks["rebuild-athena-schemas"]
tasks['process-hocas'] >> tasks["rebuild-athena-schemas"]