from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from core.etl_classifier import ETLClassification


_LOAD_STRATEGY_HINTS: dict[str, str] = {
    "truncate_insert": (
        "Use TRUNCATE + INSERT INTO ... SELECT or "
        "CREATE TABLE IF NOT EXISTS + TRUNCATE + INSERT."
    ),
    "insert_only": (
        "Use INSERT INTO ... SELECT with a filter that keeps only new rows "
        "based on date or a surrogate key."
    ),
    "upsert": (
        "Use INSERT INTO ... ON CONFLICT (key) DO UPDATE SET ... "
        "or MERGE INTO to update existing rows and insert new ones."
    ),
    "create_replace": (
        "Use DROP TABLE IF EXISTS + CREATE TABLE AS SELECT "
        "or CREATE OR REPLACE TABLE AS SELECT."
    ),
}


class PromptBuilder:
    def build_system_prompt(self, output_format: str | None = None) -> str:
        base = (
            "You are an expert ETL engineer.\n"
            "Generate production-ready ETL code from the task description and database schema.\n\n"
            "General rules:\n"
            "1. Always return code inside a single fenced code block matching the target language.\n"
            "2. Return complete executable code without manual placeholders or TODOs.\n"
            "3. Use only tables and columns from the provided schema. Do not invent fields.\n"
            "4. Prefer minimal-risk assumptions when the task is ambiguous.\n"
            "5. The result must materialize data in the database, not only read it."
        )

        if output_format == "sql":
            base += (
                "\n\nSQL rules:\n"
                "1. Return executable SQL only.\n"
                "2. SELECT-only scripts are forbidden. Use materialization such as CREATE TABLE AS, "
                "INSERT INTO ... SELECT, TRUNCATE + INSERT, or MERGE.\n"
                "3. Create target tables in schema etl_workspace unless the task says otherwise.\n"
                "4. If the target table is not given, choose a meaningful name.\n"
                "5. Forbidden placeholders: %s, %(name)s, :name, ?, $1. Use literals or SQL expressions.\n"
                "6. The script must end with a valid SQL statement."
            )
        elif output_format == "python":
            base += (
                "\n\nPython rules:\n"
                "1. Use pandas with psycopg2 or SQLAlchemy.\n"
                "2. Read data with pd.read_sql(), pd.read_csv(), or equivalent.\n"
                "3. Perform transformations in Python.\n"
                "4. Write results with to_sql(), executemany(), or equivalent.\n"
                "5. Include if __name__ == '__main__'.\n"
                "6. Use standard logging.\n"
                "7. Read database credentials from environment variables."
            )
        elif output_format == "airflow_dag":
            base += (
                "\n\nAirflow DAG rules:\n"
                "1. Return Python DAG code only.\n"
                "2. Include Airflow imports, a DAG definition, and at least one operator or @task.\n"
                "3. Use PythonOperator or PostgresOperator for ETL work.\n"
                "4. Define retries and retry_delay.\n"
                "5. For retry_delay use only timedelta(minutes=N) with "
                "from datetime import datetime, timedelta.\n"
                "6. Never write retry_delay as 00:05, 00:05:00, a string, or "
                "datetime.timedelta(...) when datetime was imported via from ... import datetime.\n"
                "7. Set catchup=False unless the task explicitly requires otherwise.\n"
                "8. Each operator should perform one logical ETL step.\n"
                "9. For PostgreSQL always use postgres_conn_id='postgres_default'.\n"
                "10. In PostgresOperator SQL always use fully qualified table names such as "
                "etl_workspace.orders and etl_workspace.target_table for all real source and target tables.\n"
                "11. Never use Variable.get('connection_string'), BaseHook.get_connection('connection_string'), "
                "or custom raw connection strings."
            )

        return base

    def build_user_prompt(
        self,
        task_description: str,
        schema: dict,
        output_format: str,
        etl_classification: ETLClassification | None = None,
    ) -> str:
        schema_lines: list[str] = []
        for table_name, columns in schema.items():
            schema_lines.append(f"Table: {table_name}")
            for col in columns:
                nullable = "NULL" if col.get("nullable", False) else "NOT NULL"
                schema_lines.append(
                    f"  - {col.get('column')}: {col.get('type')} ({nullable})"
                )
        schema_text = "\n".join(schema_lines) if schema_lines else "Schema not provided."

        parts = [
            f"Task description:\n{task_description}",
            f"Database schema:\n{schema_text}",
            f"Required output format: {output_format}",
        ]

        if etl_classification is not None:
            pattern_block = (
                f"Detected ETL pattern: {etl_classification.pattern}\n"
                f"Load strategy: {etl_classification.load_strategy}\n"
                f"Pattern description: {etl_classification.description}"
            )
            hint = _LOAD_STRATEGY_HINTS.get(etl_classification.load_strategy, "")
            if hint:
                pattern_block += f"\nImplementation hint: {hint}"
            if etl_classification.target_table_hint:
                pattern_block += (
                    f"\nTarget table hint from the task: "
                    f"{etl_classification.target_table_hint}"
                )
            parts.append(pattern_block)

        if output_format == "sql":
            parts.append(
                "Generate a materialization-first SQL ETL script. "
                "The script must create or update a table in etl_workspace and must not be read-only. "
                "Do not use placeholders like %s or :name. "
                "Return SQL code only."
            )
        elif output_format == "airflow_dag":
            parts.append(
                "Return a full Airflow DAG. "
                "Use retry_delay only as timedelta(minutes=N) after importing "
                "datetime and timedelta from the datetime module. "
                "In every PostgresOperator use fully qualified table names in schema etl_workspace."
            )

        return "\n\n".join(parts)

    def get_correction_prompt(
        self,
        original_code: str,
        error_message: str,
        output_format: str | None = None,
    ) -> str:
        intro = (
            "The following code has an error. Fix it and return the full corrected code.\n\n"
            f"Original code:\n{original_code}\n\n"
            f"Error: {error_message}"
        )

        if output_format == "sql" or (
            output_format is None and ("SELECT" in original_code or "INSERT" in original_code)
        ):
            intro += (
                "\n\nRequirements for corrected SQL:\n"
                "- Return a materializing ETL script, not only SELECT.\n"
                "- Do not use placeholders like %s, %(name)s, :name, ?, $1.\n"
                "- Return SQL code only."
            )
        elif output_format == "airflow_dag":
            intro += (
                "\n\nRequirements for corrected DAG:\n"
                "- Return full Python DAG code with imports, DAG definition, and operators.\n"
                "- The code must be syntactically valid Python.\n"
                "- If PostgresOperator is used, set postgres_conn_id='postgres_default'.\n"
                "- In PostgresOperator SQL use fully qualified table names like etl_workspace.orders "
                "and etl_workspace.target_table for all real source and target tables.\n"
                "- For retry_delay use only timedelta(minutes=5) with "
                "from datetime import datetime, timedelta.\n"
                "- Do not use 00:05, 00:05:00, strings, or datetime.timedelta(...) "
                "when datetime was imported via from ... import datetime.\n"
                "- Do not use Variable.get('connection_string') or custom raw connection strings."
            )

        return intro
