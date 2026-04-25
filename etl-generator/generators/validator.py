import ast

import sqlparse
from sqlalchemy import create_engine, text


class Validator:
    def __init__(self, db_url: str):
        self.engine = create_engine(db_url)

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

        connection = self.engine.connect()
        transaction = connection.begin()
        try:
            connection.execute(text(f"EXPLAIN {sql_code}"))
            transaction.rollback()
        except Exception as exc:
            transaction.rollback()
            return {"valid": False, "error": str(exc), "stage": "explain"}
        finally:
            connection.close()

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
            return {"valid": False, "error": str(exc)}

        if "with DAG(" not in dag_code:
            return {"valid": False, "error": "Не найден блок with DAG("}

        if "Operator" not in dag_code:
            return {"valid": False, "error": "Не найден ни один Operator"}

        return {"valid": True, "error": None}
