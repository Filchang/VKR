import ast
import re

import sqlparse
from sqlalchemy import create_engine, text

from api.database import apply_work_schema


class Validator:
    PLACEHOLDER_PATTERNS = (
        re.compile(r"%s"),
        re.compile(r"%\([A-Za-z_][A-Za-z0-9_]*\)s"),
        re.compile(r":[A-Za-z_][A-Za-z0-9_]*"),
        re.compile(r"\$\d+"),
        re.compile(r"\?"),
    )

    def __init__(self, db_url: str):
        self.engine = create_engine(db_url)

    @staticmethod
    def _split_statements(sql_code: str) -> list[str]:
        return [
            statement.strip()
            for statement in sqlparse.split(sql_code)
            if statement.strip()
        ]

    @staticmethod
    def _statement_type(sql_statement: str) -> str:
        parsed = sqlparse.parse(sql_statement)
        if not parsed:
            return "UNKNOWN"

        statement_type = parsed[0].get_type().upper()
        if statement_type == "UNKNOWN" and sql_statement.lstrip().upper().startswith("WITH "):
            return "SELECT"
        return statement_type

    @classmethod
    def _contains_placeholders(cls, sql_code: str) -> bool:
        return any(pattern.search(sql_code) for pattern in cls.PLACEHOLDER_PATTERNS)

    def validate_sql(self, sql_code: str) -> dict:
        try:
            statements = sqlparse.parse(sql_code)
        except Exception as exc:
            return {"valid": False, "error": str(exc), "stage": "syntax"}

        if not statements:
            return {
                "valid": False,
                "error": "SQL parsing returned no statements",
                "stage": "syntax",
            }

        statements_text = self._split_statements(sql_code)
        if not statements_text:
            return {
                "valid": False,
                "error": "SQL script is empty after normalization",
                "stage": "syntax",
            }

        if self._contains_placeholders(sql_code):
            return {
                "valid": False,
                "error": (
                    "SQL script contains placeholders or bind parameters. "
                    "Use executable SQL with literal values or SQL expressions instead of %s, %(name)s, :name, ?, $1."
                ),
                "stage": "logic",
            }

        explainable_types = {"SELECT", "INSERT", "UPDATE", "DELETE"}
        materializing_types = {"CREATE", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "ALTER", "DROP", "MERGE"}
        statement_types = [self._statement_type(statement_text) for statement_text in statements_text]

        if all(statement_type == "SELECT" for statement_type in statement_types):
            return {
                "valid": False,
                "error": (
                    "SQL script contains only SELECT statements. "
                    "For ETL generation a materializing script is required: "
                    "CREATE TABLE AS, INSERT INTO ... SELECT, TRUNCATE + INSERT, or similar."
                ),
                "stage": "logic",
            }

        if not any(statement_type in materializing_types for statement_type in statement_types):
            return {
                "valid": False,
                "error": (
                    "SQL script does not contain a materializing operation. "
                    "Expected CREATE, INSERT, UPDATE, DELETE, TRUNCATE, ALTER, DROP, or MERGE."
                ),
                "stage": "logic",
            }

        with self.engine.connect() as connection:
            transaction = connection.begin()
            try:
                apply_work_schema(connection)
                for index, statement_text in enumerate(statements_text, start=1):
                    statement_type = statement_types[index - 1]
                    try:
                        if statement_type in explainable_types:
                            connection.execute(text(f"EXPLAIN {statement_text}"))
                        else:
                            connection.execute(text(statement_text))
                    except Exception as exc:
                        return {
                            "valid": False,
                            "error": f"Statement #{index} ({statement_type}): {exc}",
                            "stage": (
                                "explain"
                                if statement_type in explainable_types
                                else "execution"
                            ),
                        }
            finally:
                transaction.rollback()

        return {"valid": True, "error": None}

    def validate_python(self, python_code: str) -> dict:
        try:
            ast.parse(python_code)
        except SyntaxError as exc:
            return {"valid": False, "error": str(exc), "stage": "syntax"}

        if not any(
            source in python_code
            for source in ("pd.read_sql", "read_csv", "psycopg2.connect")
        ):
            return {
                "valid": False,
                "error": "Не найдена точка чтения данных",
                "stage": "logic",
            }

        return {"valid": True, "error": None}

    def validate_dag(self, dag_code: str) -> dict:
        try:
            ast.parse(dag_code)
        except SyntaxError as exc:
            return {"valid": False, "error": str(exc), "stage": "syntax"}

        if "with DAG(" not in dag_code:
            return {
                "valid": False,
                "error": "Не найден блок with DAG(",
                "stage": "structure",
            }

        if "PythonOperator(" not in dag_code:
            return {
                "valid": False,
                "error": "Не найден ни один PythonOperator",
                "stage": "structure",
            }

        return {"valid": True, "error": None}
