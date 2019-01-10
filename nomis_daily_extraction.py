from datetime import datetime, timedelta

import airflow
from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.utils.dates import days_ago

# GLOBAL ENV VARIABLES
IMAGE = "593291632749.dkr.ecr.eu-west-1.amazonaws.com/airflow-nomis-ap:v1.0.5"
ROLE = "airflow_nomis_extraction"

# TAR/HOCAS PROCESS SCRIPT ENVs
EXTRACTION_SCRIPT = "nomis_batch_extract.py"
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
        "PYTHON_SCRIPT_NAME": "nomis_batch_extract.py"
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
        "PYTHON_SCRIPT_NAME": "test_extraction_outputs_and_move_to_raw.py"
    },
    arguments=["{{ ds }}"],
    labels={"app": dag.dag_id},
    name=task_id,
    in_cluster=True,
    task_id=task_id,
    get_logs=True,
    annotations={"iam.amazonaws.com/role": ROLE},
)

# Set dependencies
tasks['batch-extract'] >> tasks["check-batch-extract"]
