from datetime import datetime, timedelta

import airflow
from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.utils.dates import days_ago

TAR_PROCESS_SCRIPT = "tar_raw_to_process.py"
DB_REBUILD_SCRIPT = "rebuild_databases.py"
DB_VERSION = 'v1'
DBS_TO_REBUILD = 'mags_processed'

# HOCAS_RAW_FOLDER = ""
# HOCAS_PYTHON_SCRIPT_NAME = "hocas_raw_to_process.py"

MAGS_IMAGE = "593291632749.dkr.ecr.eu-west-1.amazonaws.com/airflow-magistrates-data-engineering:v0.0.7"
MAGS_ROLE = "airflow_mags_data_processor"

task_args = {
    "depends_on_past": False,
    "email_on_failure": True,
    "owner": "isichei",
    "email": ["karik.isichei@digital.justice.gov.uk"],
}

# Catch-up on dates before today-REUPDATE_LAST_N_DAYS days
dag = DAG(
    "mags_data_raw_to_processed_fast_forward",
    default_args=task_args,
    description="Process mags data (HOCAS and TAR) running all preprocessing in parallel",
    start_date=datetime.now(),
    schedule_interval=None,
)

tasks = {}

file_land_timestamps = [
"file_land_timestamp=1541030400",
"file_land_timestamp=1541116800",
"file_land_timestamp=1541203200",
"file_land_timestamp=1541289600",
"file_land_timestamp=1541376000",
"file_land_timestamp=1541462400",
"file_land_timestamp=1541548800",
"file_land_timestamp=1541635200",
"file_land_timestamp=1541721600"
]

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

# Run each set of paths in parallel and set rebuild of databases downstream
for i, flt in enumerate(file_land_timestamps) :
    task_id = f"process-tar-{i}"
    tasks[task_id] = KubernetesPodOperator(
        dag=dag,
        namespace="airflow",
        image=MAGS_IMAGE,
        env_vars={
            "PYTHON_SCRIPT_NAME": TAR_PROCESS_SCRIPT,
            "DB_VERSION": DB_VERSION,
            "S3_RELATIVE_FOLDER_PATHS": flt,
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
    tasks[task_id] >> tasks["rebuild-athena-schemas"]

