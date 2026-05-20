from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 5, 19),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

dag = DAG(
    dag_id='etl_task_55',
    schedule="@daily",
    default_args=default_args,
    catchup=False
)

create_aggregated_orders_table = PostgresOperator(
    task_id="create_aggregated_orders_table",
    sql=f"""
        CREATE TABLE IF NOT EXISTS etl_workspace.aggregated_orders (
            date DATE PRIMARY KEY,
            total_amount NUMERIC(12, 2),
            num_orders INT,
            avg_order_amount NUMERIC(12, 2)
        );
    """,
    postgres_conn_id="postgres_default",
    dag=dag
)

insert_or_update_aggregated_orders = PostgresOperator(
    task_id="insert_or_update_aggregated_orders",
    sql=f"""
        INSERT INTO etl_workspace.aggregated_orders 
        (
            date,
            total_amount,
            num_orders,
            avg_order_amount
        )
        SELECT
            created_at::DATE AS date,
            SUM(amount) AS total_amount,
            COUNT(*) AS num_orders,
            AVG(amount) AS avg_order_amount
        FROM etl_workspace.orders
        WHERE created_at::DATE >= CURRENT_DATE - INTERVAL '1 day'
        GROUP BY created_at::DATE
        ON CONFLICT (date) DO UPDATE 
        SET 
            total_amount = EXCLUDED.total_amount,
            num_orders = EXCLUDED.num_orders,
            avg_order_amount = EXCLUDED.avg_order_amount;
    """,
    postgres_conn_id="postgres_default",
    dag=dag
)

create_aggregated_orders_table >> insert_or_update_aggregated_orders