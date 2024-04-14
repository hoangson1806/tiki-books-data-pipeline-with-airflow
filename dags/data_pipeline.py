from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.empty import EmptyOperator
import scripts.extract_product_id
import scripts.extract_product_data
import scripts.create_fact_table
import scripts.create_dim_book_table
import scripts.create_dim_category_table
import scripts.transform_load_dim_book_table
import scripts.transform_load_dim_category_table
import scripts.transform_load_fact_table

default_agrs = {
    "owner": "Hoang Son",
    "start_date": days_ago(0),
    "email": "maihoangson180602@gmail.com",
    "email_on_failure": False,
    "retries": False,
    "retry_delay": timedelta(minutes=5),
}
with DAG(
    dag_id="data_pipeline",
    default_args=default_agrs,
    start_date=datetime(2024, 3, 19),
    schedule="@daily",
) as dag:
    start_operator = EmptyOperator(task_id="start_data_pipeline")
    extract_product_id = PythonOperator(
        task_id="extract_product_id", python_callable=scripts.extract_product_id.main
    )
    extract_product_data = PythonOperator(
        task_id="extract_product_data", python_callable=scripts.extract_product_data.main
    )
    create_fact_table = PythonOperator(
        task_id="create_fact_table", python_callable=scripts.create_fact_table.main
    )
    create_dim_book_table = PythonOperator(
        task_id="create_dim_book_table", python_callable=scripts.extrcreate_dim_book_tableact_product_id.main
    )
    create_dim_category_table = PythonOperator(
        task_id="create_dim_category_table",
        python_callable=scripts.create_dim_category_table.main,
    )
    transform_load_dim_book_table = PythonOperator(
        task_id="transform_load_dim_book_table",
        python_callable=scripts.transform_load_dim_book_table.main,
    )
    transform_load_dim_category_table = PythonOperator(
        task_id="transform_load_dim_category_table",
        python_callable=scripts.transform_load_dim_category_table.main,
    )
    transform_load_fact_table = PythonOperator(
        task_id="transform_load_fact_table",
        python_callable=scripts.transform_load_fact_table.main,
    )
    end_operator = EmptyOperator(task_id="end_data_pipeline")

    (
        start_operator
        >> extract_product_id
        >> extract_product_data
        >> create_fact_table
        >> [create_dim_book_table, create_dim_category_table]
    )
    create_dim_book_table.set_downstream(transform_load_dim_book_table)
    create_dim_category_table.set_downstream(transform_load_dim_category_table)
    (
        [transform_load_dim_book_table, transform_load_dim_category_table]
        >> transform_load_fact_table
        >> end_operator
    )
