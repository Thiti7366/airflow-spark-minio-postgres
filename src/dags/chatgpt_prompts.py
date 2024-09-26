import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.email import send_email
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

###############################################
# Parameters
###############################################
spark_conn = os.environ.get("SPARK_CONN_ID", "spark_default")
postgres_host = os.environ.get("POSTGRES_HOST", "postgres")
postgres_db = os.environ.get("POSTGRES_DB", "airflow")
postgres_user = os.environ.get("POSTGRES_USER", "airflow")
postgres_pwd = os.environ.get("POSTGRES_PWD", "airflow")
postgres_port = os.environ.get("POSTGRES_PORT", "5432")

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),  # เริ่มต้น 1 วันก่อนหน้านี้ (หรือปรับตามความเหมาะสม)
    "email_on_failure": True,
    "email_on_retry": False,
    "email": ["alert@airflow.com"],  # อีเมลสำหรับแจ้งเตือน
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'chatgpt_prompts_to_postgres',
    default_args=default_args,
    description='DAG สำหรับเขียนและอ่านข้อมูลจาก PostgreSQL โดยใช้ Spark',
    schedule_interval='@daily',  # รันทุกวัน หรือกำหนดเป็น cron expression ได้
    catchup=False,  # ไม่ต้องรันย้อนหลัง
)

# Task เริ่มต้น (DummyOperator)
start = DummyOperator(task_id="start", dag=dag)

# Task สำหรับการเขียนข้อมูลลง PostgreSQL
write_chatgpt_to_postgres = SparkSubmitOperator(
    task_id='write_chatgpt_to_postgres',
    application="/usr/local/spark/applications/write_chatgpt_to_postgres.py",
    conn_id=spark_conn,
    application_args=[postgres_host, postgres_db, postgres_user, postgres_pwd, postgres_port],
    retries=1,
    retry_delay=timedelta(minutes=1),
    dag=dag,
)

# Task สำหรับการอ่านข้อมูลจาก PostgreSQL
read_chatgpt_from_postgres = SparkSubmitOperator(
    task_id='read_chatgpt_from_postgres',
    application="/usr/local/spark/applications/read_chatgpt_from_postgres.py",
    conn_id=spark_conn,
    application_args=[postgres_host, postgres_db, postgres_user, postgres_pwd, postgres_port],
    retries=1,
    retry_delay=timedelta(minutes=1),
    dag=dag,
)

# Task สุดท้าย (DummyOperator)
end = DummyOperator(task_id="end", dag=dag)

# การจัดลำดับของ Tasks
start >> write_chatgpt_to_postgres >> read_chatgpt_from_postgres >> end