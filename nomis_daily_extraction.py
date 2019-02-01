from datetime import datetime, timedelta

import airflow
from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.utils.dates import days_ago

# GLOBAL ENV VARIABLES
IMAGE_VERSION = "v2.0.0"
IMAGE = f"593291632749.dkr.ecr.eu-west-1.amazonaws.com/airflow-nomis-ap:{IMAGE_VERSION}"
ROLE = "airflow_nomis_extraction"
NOMIS_T62_FETCH_SIZE = '100000'

# TAR/HOCAS PROCESS SCRIPT ENVs
EXTRACTION_SCRIPT = "nomis_delta_extract.py"
EXTRACTION_CHECK_SCRIPT = "test_extraction_outputs_and_move_to_raw.py"

task_args = {
    "depends_on_past": False,
    "email_on_failure": True,
    "owner": "isichei",
    "email": ["karik.isichei@digital.justice.gov.uk"],
}

dag = DAG(
    "nomis_daily_extract",
    default_args=task_args,
    description="Extract data from the NOMIS T62 Database",
    start_date=datetime(2018, 12, 16),
    schedule_interval="@daily",
    catchup=False,
)

tasks = {}

task_id = "batch-extract"
tasks[task_id] = KubernetesPodOperator(
    dag=dag,
    namespace="airflow",
    image=IMAGE,
    env_vars={
        "PYTHON_SCRIPT_NAME": EXTRACTION_SCRIPT,
        "IMAGE_VERSION": IMAGE_VERSION,
        "NOMIS_T62_FETCH_SIZE": NOMIS_T62_FETCH_SIZE
    },
    arguments=["{{ ds }}"],
    labels={"app": dag.dag_id},
    name=task_id,
    in_cluster=True,
    task_id=task_id,
    get_logs=True,
    annotations={"iam.amazonaws.com/role": ROLE},
    node_selectors = {"failure-domain.beta.kubernetes.io/zone": "eu-west-1a"}
)

task_id = "check-batch-extract"
tasks[task_id] = KubernetesPodOperator(
    dag=dag,
    namespace="airflow",
    image=IMAGE,
    env_vars={
        "PYTHON_SCRIPT_NAME": EXTRACTION_CHECK_SCRIPT,
        "IMAGE_VERSION": IMAGE_VERSION
    },
    arguments=["{{ ds }}"],
    labels={"app": dag.dag_id},
    name=task_id,
    in_cluster=True,
    task_id=task_id,
    get_logs=True,
    annotations={"iam.amazonaws.com/role": ROLE},
    is_delete_operator_pod=True
)

# Set dependencies
tasks['batch-extract'] >> tasks["check-batch-extract"]
