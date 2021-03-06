from airflow.hooks.S3_hook import S3Hook
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from airflow.contrib.operators.emr_create_job_flow_operator import (
    EmrCreateJobFlowOperator,
)

from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.contrib.operators.emr_terminate_job_flow_operator import (
    EmrTerminateJobFlowOperator,
)

from datetime import datetime, timedelta
import os
import logging

# Configurations
BUCKET_NAME = "udacity-capstone-2021"
local_csv = "dags/data/csv"
local_sas = "dags/data/SAS_data"
local_scripts = "dags/scripts"
local_jars = "dags/jars"
s3_data = "source_files/csv/"
s3_script = "source_files/scripts/"
s3_jars = "source_files/jars/"
s3_sas = "source_files/SAS/"
s3_clean = "output/"

default_args = {
    "owner": "Tarek Atwan",
    "start_date": datetime(2021, 3, 4, 0, 0, 0, 0),
    "depends_on_past": False,
    "email": ["tatwan@outlook.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

# EMR Spark
SPARK_STEPS = [
    {
        "Name": "Spark script files from S3 to HDFS",
        "ActionOnFailure": "CANCEL_AND_WAIT", 
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "s3-dist-cp",
                "--src=s3://{{ params.BUCKET_NAME }}/{{ params.s3_script }}",
                "--dest=/source",
            ],
        },
    },
    {
        "Name": "Data files from S3 to HDFS",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "s3-dist-cp",
                "--src=s3://{{ params.BUCKET_NAME }}/{{ params.s3_data}}",
                "--dest=/source",
            ],
        },
    },
    {
        "Name": "JAR file from S3 to HDFS",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "s3-dist-cp",
                "--src=s3://{{ params.BUCKET_NAME }}/{{ params.jar_files}}",
                "--dest=/usr/lib/spark/jars/",
            ],
        },
    },
    {
        "Name": "SAS files from S3 to HDFS",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "s3-dist-cp",
                "--src=s3://{{ params.BUCKET_NAME }}/{{ params.sas_files}}",
                "--dest=/source",
            ],
        },
    },
    {
        "Name": "immigration data ETL",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--packages",
                "saurfang:spark-sas7bdat:2.0.0-s_2.11",
                "--deploy-mode",
                "client",
                "s3://udacity-capstone-2021/source_files/scripts/spark_etl.py",
            ],
        },
    },
    {
        "Name": "Move clean data from HDFS to S3",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "s3-dist-cp",
                "--src=/output",
                "--dest=s3://{{ params.BUCKET_NAME }}/{{ params.s3_clean }}",
            ],
        },
    },
]


JOB_FLOW_OVERRIDES = {
    "Name": "Capstone_Project",
    "ReleaseLabel": "emr-5.28.0",
    "Applications": [{"Name": "Hadoop"}, {"Name": "Spark"}],
    "Configurations": [
        {
            "Classification": "spark-env",
            "Configurations": [
                {
                    "Classification": "export",
                    "Properties": {"PYSPARK_PYTHON": "/usr/bin/python3",  "JAVA_HOME": "/usr/lib/jvm/java-1.8.0"},
                },
            ],

        },
    ],
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Master node",
                "Market": "ON_DEMAND",
                "InstanceRole": "MASTER",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            },
            {
                "Name": "Core Node",
                "Market": "ON_DEMAND",
                "InstanceRole": "CORE",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 2,
            },
        ],
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False,
    },
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
    "LogUri": "s3://udacity-capstone-2021/job",
    "VisibleToAllUsers": True
}

# define DAG
dag = DAG("emr_dag",
          default_args=default_args,
          description="Load and transform with Airflow and AWS EMR-Spark",
          schedule_interval="@daily"
          )


def local_to_s3(filepath, key, bucket_name=BUCKET_NAME):
    """
    Function to to load files to S3 bucket. 
    
    Input: 
        filepath: source file to be migrated
        key: S3 folder/key
        bucket_name: S3 bucket with a default value already in place

    Output:
        Return nothing. This function ensure the files in the filepath are migrated to S3.
    """

    s3 = S3Hook(aws_conn_id="aws_default")
    for root, dirs, files in os.walk(f"{os.getcwd()}/{filepath}"):
        for filename in files:
            file_path = f"{root}/{filename}"
            s3.load_file(
                filename=file_path, key=f"{key}{filename}", bucket_name=bucket_name, replace=True)


start_operator = DummyOperator(task_id="Begin_execution",  dag=dag)

# identify all CSV type files to be migrated
csv_to_s3 = PythonOperator(
    dag=dag,
    task_id="csv_to_s3",
    python_callable = local_to_s3,
    op_kwargs={"filepath": local_csv, "key": s3_data,}
)

# identify all script files to be migratred
script_to_s3 = PythonOperator(
    dag=dag,
    task_id="scripts_to_s3",
    python_callable=local_to_s3,
    op_kwargs={"filepath": local_scripts, "key": s3_script, }
)

# migrated any necessary Spark JAR files
jar_to_s3 = PythonOperator(
    dag=dag,
    task_id="jar_to_s3",
    python_callable=local_to_s3,
    op_kwargs={"filepath": local_jars, "key": s3_jars, }
)

# identify all SAS data files to be migratred
sas_to_s3 = PythonOperator(
    dag=dag,
    task_id="sas_to_s3",
    python_callable=local_to_s3,
    op_kwargs={"filepath": local_sas, "key": s3_sas,}
)

# Instantiate the AWS EMR Cluster 
create_emr_cluster = EmrCreateJobFlowOperator(
    task_id="create_emr_cluster",
    job_flow_overrides=JOB_FLOW_OVERRIDES,
    aws_conn_id="aws_default",
    emr_conn_id="emr_default",
    dag=dag
)

# Add the steps once the cluster is up 
step_adder = EmrAddStepsOperator(
    task_id="add_steps",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id="aws_default",
    steps=SPARK_STEPS,
    params={
        "BUCKET_NAME": BUCKET_NAME,
        "s3_data": s3_data,
        "s3_script": s3_script,
        "sas_files": s3_sas,
        "s3_clean": s3_clean,
        "jar_files": s3_jars
    },
    dag=dag,
)

last_step = len(SPARK_STEPS) - 1
# wait for the steps to complete
step_checker = EmrStepSensor(
    task_id="watch_step",
    job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')["
    + str(last_step)
    + "] }}",
    aws_conn_id="aws_default",
    dag=dag,
)

# temrinate the cluster once steps/tasks are completed 
terminate_emr_cluster = EmrTerminateJobFlowOperator(
    task_id="terminate_emr_cluster",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id="aws_default",
    dag=dag,
)

end_data_pipeline = DummyOperator(task_id="end_data_pipeline", dag=dag)


start_operator >> jar_to_s3 >> create_emr_cluster
start_operator >> script_to_s3 >> create_emr_cluster
start_operator >> sas_to_s3 >> create_emr_cluster
start_operator >> csv_to_s3 >> create_emr_cluster
create_emr_cluster >> step_adder >> step_checker >> terminate_emr_cluster
terminate_emr_cluster >> end_data_pipeline
