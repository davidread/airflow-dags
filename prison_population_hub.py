from datetime import datetime, timedelta

import airflow
from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.utils.dates import days_ago


IMAGE = "qu593291632749.dkr.ecr.eu-west-1.amazonaws.com/airflow-prison-population-hub:v1.0.3"
IAM_ROLE = "airflow_prison_population_hub"

task_args = {
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 20,
    "retry_delay": timedelta(seconds=30),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=15),
    "execution_timeout": timedelta(minutes=10),
    "owner": "davidread",
    "email": ["david.read@digital.justice.gov.uk"],
}

# Catch-up on dates before today-REUPDATE_LAST_N_DAYS days
dag = DAG(
    "prison_population_hub",
    default_args=task_args,
    description="From NOMIS it calculates prison population stats and uploads to The Hub",
    start_date=days_ago(1),
    schedule_interval=timedelta(days=1),
)

task_dag = KubernetesPodOperator(
    dag=dag,
    namespace="airflow",
    image=IMAGE,
    env_vars={
    },
    arguments=["{{ ds }}"],
    labels={"app": dag.dag_id},
    name="prison-pop",
    in_cluster=True,
    task_id="prison-pop",
    get_logs=True,
    annotations={"iam.amazonaws.com/role": IAM_ROLE},
)
