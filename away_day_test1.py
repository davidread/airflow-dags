from datetime import datetime, timedelta

import airflow
from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.utils.dates import days_ago

# Define your docker image and the AWS role that will run the image (based on your airflow-repo)
IMAGE = "593291632749.dkr.ecr.eu-west-1.amazonaws.com/away-day-test:latest"
ROLE = "airflow_away_day_tester"

# Task arguments
task_args = {
    "depends_on_past": False,
    "email_on_failure": True,
    "owner": "karolagraupner",
    "email": ["karola.graupner1@justice.gov.uk"],
}

# # # Define your DAG
# Some notes:
# setting - (start_date=datetime.now() and schedule_interval=None) is a way to set up you tag so it can only be triggered manually
# To actually put it on a schedule you can set something like:
# start_date=datetime(2018, 8, 1), schedule_interval=timedelta(days=1)
dag = DAG(
    "away_day_test1",
    default_args=task_args,
    description="Whatever",
    start_date=datetime.now(),
    schedule_interval=None
)

task1 = KubernetesPodOperator(
    dag=dag,
    namespace="airflow",
    image=IMAGE,
    env_vars={
    },
    labels={"app": dag.dag_id},
    name="script1",
    in_cluster=True,
    task_id="script1",
    get_logs=True,
    annotations={"iam.amazonaws.com/role": ROLE},
)
