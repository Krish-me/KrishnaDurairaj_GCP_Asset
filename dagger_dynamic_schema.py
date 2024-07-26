import airflow
from datetime import datetime
import io
import json
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from google.cloud import storage
import logging

def infer_schema(bucket_name, file_name):
    """
    Infers the schema of the CSV file in GCS.
    """
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(file_name)
    content = blob.download_as_text()

    # Use io.StringIO to read the CSV content
    df = pd.read_csv(io.StringIO(content))

    # Generate BigQuery schema
    schema = []
    for column_name, dtype in zip(df.columns, df.dtypes):
        if pd.api.types.is_integer_dtype(dtype):
            field_type = "INTEGER"
        elif pd.api.types.is_float_dtype(dtype):
            field_type = "FLOAT"
        elif pd.api.types.is_bool_dtype(dtype):
            field_type = "BOOLEAN"
        else:
            field_type = "STRING"

        schema.append({"name": column_name, "type": field_type, "mode": "NULLABLE"})

    logging.info(f"Inferred schema: {schema}")  # Log the inferred schema
    return schema

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 7, 26),
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    "gcs_to_bigquery_dynamic_schema",
    default_args=default_args,
    schedule_interval=None,  # Define your schedule interval
    catchup=False,
) as dag:

    bucket_name = "bio-buckets"
    file_name = "books.csv"
    dataset_name = "bio_cloud"
    table_name = "bio_insertion"

    def prepare_schema(**kwargs):
        """
        Task to prepare the schema dynamically.
        """
        schema = infer_schema(bucket_name, file_name)
        # Log and return schema as JSON
        logging.info(f"Prepared schema: {json.dumps(schema)}")
        return schema

    prepare_schema_task = PythonOperator(
        task_id="prepare_schema_task",
        python_callable=prepare_schema,
        provide_context=True
    )

    def load_to_bigquery(**kwargs):
        """
        Load data from GCS to BigQuery using the dynamically prepared schema.
        """
        schema = kwargs['ti'].xcom_pull(task_ids='prepare_schema_task')
        logging.info(f"Loading data with schema: {schema}")

        load_task = GCSToBigQueryOperator(
            task_id="load_to_bigquery_task",
            bucket=bucket_name,
            source_objects=[file_name],
            destination_project_dataset_table=f"{dataset_name}.{table_name}",
            schema_fields=schema,
            source_format="CSV",
            skip_leading_rows=1,
            write_disposition="WRITE_TRUNCATE",
        )
        load_task.execute(kwargs)

    load_to_bigquery_task = PythonOperator(
        task_id="load_to_bigquery_task",
        python_callable=load_to_bigquery,
        provide_context=True
    )

    prepare_schema_task >> load_to_bigquery_task
