from datetime import datetime, timedelta

import airflow
from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.utils.dates import days_ago

TAR_RAW_FOLDER = "s3://mojap-raw/hmcts/tar/"
TAR_PYTHON_SCRIPT_NAME = "tar_raw_to_process.py"

# HOCAS_RAW_FOLDER = "s3://mojap-raw/hmcts/hocas/"
# HOCAS_PYTHON_SCRIPT_NAME = "hocas_raw_to_process.py"

MAGS_RAW_TO_PROCESSED_IMAGE = "593291632749.dkr.ecr.eu-west-1.amazonaws.com/airflow-magistrates-data-engineering:v0.0.2"
MAGS_RAW_TO_PROCESSED_ROLE = "airflow_mags_data_processor"

task_args = {
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 4,
    "retry_delay": timedelta(seconds=30),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=15),
    "execution_timeout": timedelta(minutes=10),
    "owner": "isichei",
    "email": ["karik.isichei@digital.justice.gov.uk"],
}

# Catch-up on dates before today-REUPDATE_LAST_N_DAYS days
dag = DAG(
    "mags_data_raw_to_processed",
    default_args=task_args,
    description="Process mags data (HOCAS and TAR)",
    start_date=datetime.now(),
    schedule_interval=None,
)

process_tar = KubernetesPodOperator(
    dag=dag,
    namespace="airflow",
    image=MAGS_RAW_TO_PROCESSED_IMAGE,
    env_vars={
        "PYTHON_SCRIPT_NAME": TAR_PYTHON_SCRIPT_NAME,
        "S3_RAW_FILEPATHS": TAR_RAW_FOLDER,
    },
    arguments=["{{ ds }}"],
    labels={"app": dag.dag_id},
    name="process-tar",
    in_cluster=True,
    task_id="process_tar",
    get_logs=True,
    annotations={"iam.amazonaws.com/role": MAGS_RAW_TO_PROCESSED_ROLE},
)