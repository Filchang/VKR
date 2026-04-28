from pathlib import Path

from jinja2 import Environment, FileSystemLoader

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


def test_dag_template_renders_valid_python():
    templates_dir = Path(__file__).resolve().parents[2] / "generators"
    environment = Environment(
        loader=FileSystemLoader(str(templates_dir)),
        trim_blocks=True,
        lstrip_blocks=True,
        keep_trailing_newline=True,
    )
    template = environment.get_template("dag_template.j2")
    dag_code = template.render(
        dag_id="etl_task_1",
        schedule="@daily",
        retries=1,
        retry_delay_minutes=5,
        start_year=2026,
        start_month=4,
        start_day=28,
        tasks=[
            {
                "task_id": "extract-orders",
                "function_name": "extract_orders",
                "variable_name": "extract_orders",
                "code": "print('ok')",
            }
        ],
        dependencies=[],
    )

    validator = Validator("sqlite:///:memory:")
    result = validator.validate_dag(dag_code)

    assert result["valid"] is True


from unittest.mock import Mock

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
        "Построй DAG",
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
    assert result["success"] is True
    assert "with DAG(" in result["dag_code"]
