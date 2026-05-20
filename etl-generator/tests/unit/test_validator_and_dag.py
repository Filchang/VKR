from unittest.mock import Mock

from api.routes import _format_validation_error
from core.prompt_builder import PromptBuilder
from generators.dag_generator import DAGGenerator
from generators.validator import Validator


def test_sql_validator_accepts_create_insert_select():
    validator = Validator("sqlite:///:memory:")
    result = validator.validate_sql(
        """
        CREATE TABLE sample (id INTEGER, name TEXT);
        INSERT INTO sample (id, name) VALUES (1, 'Alice');
        SELECT * FROM sample;
        """
    )

    assert result["valid"] is True
    assert result["error"] is None


def test_sql_validator_reports_statement_index():
    validator = Validator("sqlite:///:memory:")
    result = validator.validate_sql(
        """
        CREATE TABLE sample (id INTEGER);
        INSERT INTO missing_table (id) VALUES (1);
        """
    )

    assert result["valid"] is False
    assert result["stage"] in {"explain", "execution"}
    assert "Statement #2" in result["error"]


def test_sql_validator_rejects_plain_select_only():
    validator = Validator("sqlite:///:memory:")
    result = validator.validate_sql(
        """
        SELECT *
        FROM orders
        """
    )

    assert result["valid"] is False
    assert result["stage"] == "logic"
    assert "contains only SELECT statements" in result["error"]


def test_sql_validator_rejects_placeholders():
    validator = Validator("sqlite:///:memory:")
    result = validator.validate_sql(
        """
        CREATE TABLE mart_orders AS
        SELECT *
        FROM orders
        WHERE created_at BETWEEN %s AND %s
        """
    )

    assert result["valid"] is False
    assert result["stage"] == "logic"
    assert "contains placeholders or bind parameters" in result["error"]


def test_format_validation_error_uses_actual_stage():
    message = _format_validation_error(
        {
            "valid": False,
            "stage": "logic",
            "error": "SQL script contains placeholders or bind parameters.",
        }
    )

    assert "stage 'logic'" in message


def test_format_validation_error_uses_first_error_entry():
    message = _format_validation_error(
        {
            "valid": False,
            "errors": [
                {
                    "stage": "security",
                    "error": "Usage of eval() is forbidden inside DAG files.",
                }
            ],
            "warnings": [],
        }
    )

    assert "stage 'security'" in message
    assert "eval()" in message


def test_dag_generator_accepts_python_dag_response():
    fake_client = Mock()
    fake_client.send_message_with_retry.return_value = """```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

with DAG(
    dag_id='etl_task_1',
    start_date=datetime(2026, 4, 28),
    schedule='@daily',
    catchup=False,
) as dag:
    def step(**context):
        print('ok')

    step_task = PythonOperator(
        task_id='step',
        python_callable=step,
    )
```"""

    generator = DAGGenerator()
    generator.gigachat_client = fake_client

    result = generator.generate(
        "Build DAG",
        {"orders": [{"column": "id", "type": "INT", "nullable": False}]},
        {
            "dag_id": "etl_task_1",
            "schedule": "@daily",
            "retries": 1,
            "start_date": "2026-04-28",
        },
    )

    assert result["success"] is True
    assert "with DAG(" in result["dag_code"]


def test_dag_generator_accepts_assigned_dag_response():
    fake_client = Mock()
    fake_client.send_message_with_retry.return_value = """```python
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime

dag = DAG(
    dag_id='etl_task_1',
    start_date=datetime(2026, 4, 28),
    schedule='@daily',
    catchup=False,
)

start = EmptyOperator(
    task_id='start',
    dag=dag,
)
```"""

    generator = DAGGenerator()
    generator.gigachat_client = fake_client

    result = generator.generate(
        "Build DAG",
        {"orders": [{"column": "id", "type": "INT", "nullable": False}]},
        {
            "dag_id": "etl_task_1",
            "schedule": "@daily",
            "retries": 1,
            "start_date": "2026-04-28",
        },
    )

    assert result["success"] is True
    assert "dag = DAG(" in result["dag_code"]


def test_dag_generator_prompt_explicitly_requires_timedelta_retry_delay():
    generator = DAGGenerator()
    _, user_prompt = generator._build_prompts(
        "Build DAG",
        {"orders": [{"column": "id", "type": "INT", "nullable": False}]},
        {
            "dag_id": "etl_task_1",
            "schedule": "@daily",
            "retries": 1,
            "start_date": "2026-04-28",
            "retry_delay_minutes": 5,
        },
    )

    assert "retry_delay_python: timedelta(minutes=5)" in user_prompt


def test_dag_generator_prompt_requires_schema_qualified_table_names():
    generator = DAGGenerator()
    _, user_prompt = generator._build_prompts(
        "Build DAG",
        {"orders": [{"column": "id", "type": "INT", "nullable": False}]},
        {
            "dag_id": "etl_task_1",
            "schedule": "@daily",
            "retries": 1,
            "start_date": "2026-04-28",
            "retry_delay_minutes": 5,
        },
    )

    assert "fully qualified table names" in user_prompt
    assert "etl_workspace" in user_prompt


def test_airflow_correction_prompt_rejects_00_05_retry_delay_format():
    prompt = PromptBuilder().get_correction_prompt(
        "default_args = {'retry_delay': 00:05}",
        "Python syntax error",
        output_format="airflow_dag",
    )

    assert "timedelta(minutes=5)" in prompt
    assert "00:05" in prompt


def test_dag_validator_accepts_assigned_dag_and_non_python_operator():
    validator = Validator("sqlite:///:memory:")
    result = validator.validate_dag(
        """
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime

dag = DAG(
    dag_id='etl_task_1',
    start_date=datetime(2026, 4, 28),
    schedule='@daily',
    catchup=False,
)

start = EmptyOperator(
    task_id='start',
    dag=dag,
)
"""
    )

    assert result["valid"] is True


def test_dag_validator_rejects_variable_connection_string():
    validator = Validator("sqlite:///:memory:")
    result = validator.validate_dag(
        """
from airflow import DAG
from airflow.models import Variable
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

dag = DAG(
    dag_id='etl_task_39',
    start_date=datetime(2026, 4, 28),
    schedule='@once',
    catchup=False,
)

conn_string = Variable.get("connection_string")

task = PostgresOperator(
    task_id='load_data',
    sql='SELECT 1',
    postgres_conn_id='postgres_default',
    dag=dag,
)
"""
    )

    assert result["valid"] is False
    assert result["stage"] == "logic"
    assert "connection string" in result["error"]


def test_dag_validator_rejects_postgres_operator_without_postgres_default():
    validator = Validator("sqlite:///:memory:")
    result = validator.validate_dag(
        """
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

dag = DAG(
    dag_id='etl_task_40',
    start_date=datetime(2026, 4, 28),
    schedule='@once',
    catchup=False,
)

task = PostgresOperator(
    task_id='load_data',
    sql='SELECT 1',
    postgres_conn_id='custom_pg',
    dag=dag,
)
"""
    )

    assert result["valid"] is False
    assert result["stage"] == "logic"
    assert "postgres_default" in result["error"]


def test_dag_validator_rejects_string_retry_delay_in_default_args():
    validator = Validator("sqlite:///:memory:")
    result = validator.validate_dag(
        """
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(2),
    'retry_delay': '00:05:00',
}

dag = DAG(
    dag_id='etl_task_44',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
)

start = EmptyOperator(
    task_id='start',
    dag=dag,
)
"""
    )

    assert result["valid"] is False
    assert result["stage"] == "logic"
    assert "retry_delay" in result["error"]
    assert "timedelta" in result["error"]


def test_dag_validator_rejects_datetime_symbol_timedelta_usage():
    validator = Validator("sqlite:///:memory:")
    result = validator.validate_dag(
        """
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from airflow.utils.timezone import datetime

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(2),
    'retry_delay': datetime.timedelta(minutes=5),
}

dag = DAG(
    dag_id='etl_task_45',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
)

start = EmptyOperator(
    task_id='start',
    dag=dag,
)
"""
    )

    assert result["valid"] is False
    assert result["stage"] == "logic"
    assert "datetime.timedelta" in result["error"]
    assert "not as a module" in result["error"]


def test_dag_validator_rejects_deprecated_postgres_import():
    validator = Validator("sqlite:///:memory:")
    result = validator.validate_dag(
        """
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from datetime import datetime

dag = DAG(
    dag_id='etl_task_50',
    start_date=datetime(2026, 4, 28),
    schedule='@daily',
    catchup=False,
)

task = PostgresOperator(
    task_id='load_data',
    sql='SELECT 1',
    postgres_conn_id='postgres_default',
    dag=dag,
)
"""
    )

    assert result["valid"] is False
    assert result["stage"] == "compatibility"
    assert "Deprecated import" in result["error"]


def test_dag_validator_rejects_cross_task_temporary_table_usage():
    validator = Validator("sqlite:///:memory:")
    result = validator.validate_dag(
        """
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 5, 19),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='etl_task_50',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
)

t1 = PostgresOperator(
    task_id='create_temp_table',
    sql='''
        CREATE TEMPORARY TABLE temp_orders_aggregate AS
        SELECT user_id, SUM(amount) AS total_amount
        FROM orders
        GROUP BY user_id;
    ''',
    postgres_conn_id='postgres_default',
    dag=dag,
)

t2 = PostgresOperator(
    task_id='aggregate_users',
    sql='''
        INSERT INTO users_aggregated (user_id, total_amount)
        SELECT t.user_id, t.total_amount
        FROM temp_orders_aggregate t
    ''',
    postgres_conn_id='postgres_default',
    dag=dag,
)

t1 >> t2
"""
    )

    assert result["valid"] is False
    assert result["stage"] == "runtime"
    assert "temporary table" in result["error"]
    assert "create_temp_table" in result["error"]
    assert "aggregate_users" in result["error"]


def test_dag_validator_rejects_dag_start_date_used_in_f_string():
    validator = Validator("sqlite:///:memory:")
    result = validator.validate_dag(
        """
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 5, 19),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='etl_task_52',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
)

task = PostgresOperator(
    task_id='aggregate_orders',
    sql=f'''
        SELECT *
        FROM orders
        WHERE created_at::DATE >= '{dag.start_date.date()}'
    ''',
    postgres_conn_id='postgres_default',
    dag=dag,
)
"""
    )

    assert result["valid"] is False
    assert result["stage"] == "runtime"
    assert "dag.start_date" in result["error"]
    assert "import time" in result["error"]


def test_dag_validator_rejects_unqualified_postgres_table_names():
    validator = Validator("sqlite:///:memory:")
    result = validator.validate_dag(
        """
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 5, 19),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='etl_task_53',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
)

task = PostgresOperator(
    task_id='aggregate_orders',
    sql='''
        INSERT INTO aggregated_orders (date, total_amount)
        SELECT created_at::DATE, SUM(amount)
        FROM orders
        GROUP BY created_at::DATE
    ''',
    postgres_conn_id='postgres_default',
    dag=dag,
)
"""
    )

    assert result["valid"] is False
    assert result["stage"] == "logic"
    assert "without schema qualification" in result["error"]
    assert "etl_workspace" in result["error"]
