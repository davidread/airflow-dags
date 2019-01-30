from datetime import datetime, timedelta

import airflow
from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.utils.dates import days_ago
#import boto3
#import re

# Define your docker image and the AWS role that will run the image (based on your airflow-repo)
IMAGE = "593291632749.dkr.ecr.eu-west-1.amazonaws.com/airflow-enforcement-data-engineering:v0.0.4"
ROLE = "airflow_enforcement_data_processing"

FINES_DATASETS=['closed', 'transactions', 'live']
bucket='alpha-enforcement-data-engineering'

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
    schedule_interval= "@monthly",
)


# s3 = boto3.resource('s3')
# date_list = []

# for dataset in s3.Bucket('alpha-enforcement-data-engineering').objects.filter(Prefix="input_folder/"):       
#     if re.search(r'(20\d{2})(\d{2})', dataset.key):
#         match = re.search(r'(20\d{2})(\d{2})', dataset.key)
#         date = match.group() if match else None
#         date_list.append(date)
#         #print(dataset.key.split("/")[1])
#         #print(date)

# unique_date_list = list(set(date_list))

# print(unique_date_list)


# for dataset in s3.Bucket(bucket).objects.filter(Prefix="input_folder/"):       
#     if re.search(r'(20\d{2})(\d{2})', dataset.key):
#             match = re.search(r'(20\d{2})(\d{2})', dataset.key)
#             year = match.group(1) if match else None
#             month = match.group(2) if match else None
#             if re.search(r'(SR0550)', dataset.key):
#                 dataset_type = 'live'
#             if re.search(r'(SR0413)', dataset.key):
#                 dataset_type = 'closed'
#             if re.search(r'(SR0494)', dataset.key):
#                 dataset_type = 'transactions'
#             filename = dataset.key.split("/")[1]
#             destFileKey = f'{dataset_type}/{dataset_type}_raw/{month}-{year}' + '/' + f'{filename}'
#             print(destFileKey)
#             copySource = bucket + '/' + dataset.key
#             s3.Object(bucket, destFileKey).copy_from(CopySource=copySource)
#             s3.Object(bucket, copySource).delete()


#for date in unique_date_list:
for fine_type in FINES_DATASETS:
        task_id = f"enforcement-fines-data-{fine_type}"
        task = KubernetesPodOperator(
            dag=dag,
            namespace="airflow",
            image=IMAGE,
            env_vars={
                "DATASET": f"{fine_type}",
                #"YEAR": date[0:4],
                #"MONTH": date[4:6],
                "BUCKET": bucket,
            },
            labels={"app": dag.dag_id},
            name=task_id,
            in_cluster=True,
            task_id=task_id,
            get_logs=True,
            annotations={"iam.amazonaws.com/role": ROLE},
        )