from datetime import datetime, timedelta

import airflow
from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.utils.dates import days_ago

# Define your docker image and the AWS role that will run the image (based on your airflow-repo)
IMAGE = "593291632749.dkr.ecr.eu-west-1.amazonaws.com/airflow-enforcement-data-engineering:v0.0.4"
ROLE = "airflow_enforcement_data_processing"

FINES_DATASET=['closed', 'transactions', 'live']
YEAR='2018'
MONTH='10'
BUCKET='alpha-enforcement-data-engineering'

# Task arguments
task_args = {
    "depends_on_past": False,
    "email_on_failure": True,
    "owner": "samtazzyman",
    "email": ["samuel.tazzyman@digital.justice.gov.uk"],
}

# # # Define your DAG
# Some notes:
# setting - (start_date=datetime.now() and schedule_interval=None) is a way to set up you tag so it can only be triggered manually
# To actually put it on a schedule you can set something like:
# start_date=datetime(2018, 8, 1), schedule_interval=timedelta(days=1)
dag = DAG(
    "enforcement_fines_data",
    default_args=task_args,
    description="Cleaning and processing the enforcement fines datasets",
    start_date=datetime(2018, 11, 30),
    schedule_interval=None
)


# for dataset in FINES_DATASET:
#     task_id = '"{0}"'.format(''.join(['enforcement-fines-data-', dataset]))
#     task = KubernetesPodOperator(
#     dag=dag,
#     namespace="airflow",
#     image=IMAGE,
#     env_vars={
#         "DATASET": '"{0}"'.format(''.join(dataset)),
#         "YEAR": YEAR,
#         "MONTH": MONTH,
#         "BUCKET": BUCKET,
#     },
#     labels={"app": dag.dag_id},
#     name=task_id,
#     in_cluster=True,
#     task_id=task_id,
#     get_logs=True,
#     annotations={"iam.amazonaws.com/role": ROLE},
# )

for dataset in FINES_DATASET:
    task_id = f"'enforcement-fines-data-{dataset}'"
    task = KubernetesPodOperator(
    dag=dag,
    namespace="airflow",
    image=IMAGE,
    env_vars={
        "DATASET": f"'{dataset}'",
        "YEAR": YEAR,
        "MONTH": MONTH,
        "BUCKET": BUCKET,
    },
    labels={"app": dag.dag_id},
    name=task_id,
    in_cluster=True,
    task_id=task_id,
    get_logs=True,
    annotations={"iam.amazonaws.com/role": ROLE},
)