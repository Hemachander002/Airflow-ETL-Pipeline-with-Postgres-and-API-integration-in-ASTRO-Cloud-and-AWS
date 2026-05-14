from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime
import pandas as pd
from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator
import json

with DAG(
    dag_id ='etl_dag',
    start_date=datetime(2026, 5, 14),
    schedule='@daily',
    catchup=False
) as dag:

    @task
    def create_table():
        pg_hook = PostgresHook(postgres_conn_id='my_postgres_connection')
        create_table_query = """
        CREATE TABLE IF NOT EXISTS apod_data (
            id SERIAL PRIMARY KEY,
            title VARCHAR(255),
            explanation TEXT,
            url TEXT,
            date DATE,
            media_type VARCHAR(50)
        );
        """
        pg_hook.run(create_table_query)

    extract_data = HttpOperator(
        task_id='extract_data',
        method='GET',
        http_conn_id='nasa_api',
        endpoint='planetary/apod',
        data={'api_key':"{{conn.nasa_api.extra_dejson.api_key}}"},
        response_filter=lambda response: response.json()
    )

    @task
    def transform_apod_data(response):
        title = response.get('title',"")
        explanation = response.get('explanation',"")
        url = response.get('url',"")
        date = response.get('date',"")
        media_type = response.get('media_type',"")
        return {
            'title': title,
            'explanation': explanation,
            'url': url,
            'date': date,
            'media_type': media_type
        }
    @task
    def load_to_postgres(transformed_data):
        pg_hook = PostgresHook(postgres_conn_id='my_postgres_connection')
        insert_query = """
        INSERT INTO apod_data (title, explanation, url, date, media_type) VALUES (%s, %s, %s, %s, %s)
        """
        pg_hook.run(insert_query, parameters=(
            transformed_data['title'],
            transformed_data['explanation'],
            transformed_data['url'],
            transformed_data['date'],
            transformed_data['media_type']
        ))

        ## step 5

    create_table() >> extract_data
    api_response = extract_data.output
    transformed_data = transform_apod_data(api_response)
    load_to_postgres(transformed_data)
