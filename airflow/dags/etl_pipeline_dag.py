"""
DAG: etl_pipeline_dag
Description: Orchestration du pipeline ETL avec Airflow
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

from src.extraction.web_extractor import WebExtractor
from src.extraction.csv_extractor import CSVExtractor
from src.extraction.sql_extractor import SQLExtractor
from src.transformation.spark_transformer import SparkTransformer
from src.loading.minio_loader import MinIOLoader


default_args = {
    "owner": "ABADOULAHI",
    "email": ["abdoulahihamadououmarou@gmail.com"],
    "email_on_failure": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def extract_data(**context):
    web_df = WebExtractor().extract()
    csv_df = CSVExtractor(
        csv_url="https://drive.google.com/uc?id=1s-x76gQ-eoM5sqT2Hhcfn087Aw5D__hD"
    ).extract()

    sql_df = SQLExtractor(
        user="etl_user",
        password="etl_password",
        host="postgres",
        port=5432,
        database="etl_db",
        table="books",
    ).extract()

    context["ti"].xcom_push(key="dataframes", value=[web_df, csv_df, sql_df])


def transform_data(**context):
    dfs = context["ti"].xcom_pull(key="dataframes", task_ids="extract_task")

    transformer = SparkTransformer()
    df_final = transformer.transform(dfs)

    context["ti"].xcom_push(key="df_final", value=df_final)


def load_data(**context):
    df_final = context["ti"].xcom_pull(key="df_final", task_ids="transform_task")

    loader = MinIOLoader(bucket="datalake", dataset="books")
    loader.load(df_final)


with DAG(
    dag_id="etl_pipeline_dag",
    description="Pipeline ETL industrialisé avec Spark, Airflow et MinIO",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["etl", "spark", "minio"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_task",
        python_callable=extract_data,
        provide_context=True,
    )

    transform_task = PythonOperator(
        task_id="transform_task",
        python_callable=transform_data,
        provide_context=True,
    )

    load_task = PythonOperator(
        task_id="load_task",
        python_callable=load_data,
        provide_context=True,
    )

    extract_task >> transform_task >> load_task
