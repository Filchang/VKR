from airflow import DAG
from airflow.models.baseoperator import TaskInstance
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 5, 19),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

dag = DAG(
    dag_id='etl_task_54',
    default_args=default_args,
    schedule="@daily",
    catchup=False
)

create_aggregated_orders_table = PostgresOperator(
    task_id="create_aggregated_orders_table",
    dag=dag,
    sql=f"""
        CREATE TABLE IF NOT EXISTS etl_workspace.aggregated_orders (
            date DATE PRIMARY KEY,
            total_amount NUMERIC(12, 2),
            num_orders INT,
            avg_order_amount NUMERIC(12, 2)
        );
    """,
    postgres_conn_id="postgres_default"
)

insert_or_update_aggregated_orders = PostgresOperator(
    task_id="insert_or_update_aggregated_orders",
    dag=dag,
    sql=f"""
        INSERT INTO etl_workspace.aggregated_orders 
        SELECT 
            created_at::DATE AS date,
            SUM(amount) AS total_amount,
            COUNT(*) AS num_orders,
            AVG(amount) AS avg_order_amount
        FROM etl_workspace.orders
        WHERE created_at::DATE >= '{days_ago(1).date()}'
        GROUP BY created_at::DATE
        ON CONFLICT (date) DO UPDATE 
        SET total_amount = EXCLUDED.total_amount,
            num_orders = EXCLUDED.num_orders,
            avg_order_amount = EXCLUDED.avg_order_amount;
    """,
    postgres_conn_id="postgres_default"
)

create_aggregated_orders_table >> insert_or_update_aggregated_orders