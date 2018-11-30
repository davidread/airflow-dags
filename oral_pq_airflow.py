from datetime import datetime, timedelta

import airflow
from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.utils.dates import days_ago

# Define your docker image and the AWS role that will run the image (based on your airflow-repo)
IMAGE = "593291632749.dkr.ecr.eu-west-1.amazonaws.com/airflow-oral-pqs:v1.0.3"
ROLE = "airflow_oral_pqs"

# Task arguments
task_args = {
    "depends_on_past": False,
    "email_on_failure": True,
    "owner": "samnlindsay",
    "email": ["sam.lindsay@digital.justice.gov.uk"],
}

# # # Define your DAG
# Some notes:
# setting - (start_date=datetime.now() and schedule_interval=None) is a way to set up you tag so it can only be triggered manually
# To actually put it on a schedule you can set something like:
# start_date=datetime(2018, 8, 1), schedule_interval=timedelta(days=1)
dag = DAG(
    "oral_pqs",
    default_args=task_args,
    description="Go get some oral PQs and put them in S3",
    start_date=datetime(2018, 11, 28),
    schedule_interval=None
)

task1 = KubernetesPodOperator(
    dag=dag,
    namespace="airflow",
    image=IMAGE,
    env_vars={
        "SCRIPT_NAME": "Oral_PQs_api_call.py",
        "script1_var": '1'
    },
    labels={"app": dag.dag_id},
    name="oral_pqs_api_call",
    in_cluster=True,
    task_id="oral_pqs_api_call",
    get_logs=True,
    annotations={"iam.amazonaws.com/role": ROLE},
)