import ast
import re
from collections import Counter, defaultdict

import sqlparse
from sqlalchemy import create_engine, text

from api.database import apply_work_schema
from core.config import settings


class Validator:
    DAG_DEPRECATED_IMPORTS = {
        "airflow.operators.postgres_operator":
            "Use airflow.providers.postgres.operators.postgres",
        "airflow.operators.python_operator":
            "Use airflow.operators.python",
    }
    DAG_FORBIDDEN_PATTERNS = {
        "Variable.get(\"connection_string\")":
            "Do not read raw DB connection strings from Airflow Variables.",
        "Variable.get('connection_string')":
            "Do not read raw DB connection strings from Airflow Variables.",
        "BaseHook.get_connection(\"connection_string\")":
            "Use standard Airflow connection ids instead of custom connection_string.",
        "BaseHook.get_connection('connection_string')":
            "Use standard Airflow connection ids instead of custom connection_string.",
        "eval(":
            "Usage of eval() is forbidden inside DAG files.",
        "exec(":
            "Usage of exec() is forbidden inside DAG files.",
        "pickle.loads":
            "pickle.loads is unsafe and forbidden in DAG files.",
        "subprocess.Popen":
            "subprocess.Popen usage inside DAGs is restricted.",
        "os.system(":
            "os.system() usage inside DAGs is forbidden.",
    }
    DAG_HARDCODED_SECRET_PATTERNS = [
        re.compile(r"password\s*=\s*['\"].+?['\"]", re.I),
        re.compile(r"passwd\s*=\s*['\"].+?['\"]", re.I),
        re.compile(r"postgresql:\/\/.+:.+@", re.I),
        re.compile(r"api[_-]?key\s*=\s*['\"].+?['\"]", re.I),
        re.compile(r"secret\s*=\s*['\"].+?['\"]", re.I),
    ]
    DAG_FORBIDDEN_TOP_LEVEL_CALLS = {
        "read_csv",
        "read_parquet",
        "read_excel",
        "read_json",
        "connect",
        "execute",
        "requests.get",
        "requests.post",
        "create_engine",
    }

    PLACEHOLDER_PATTERNS = (
        re.compile(r"%s"),
        re.compile(r"%\([A-Za-z_][A-Za-z0-9_]*\)s"),
        re.compile(r":[A-Za-z_][A-Za-z0-9_]*"),
        re.compile(r"\$\d+"),
        re.compile(r"\?"),
    )

    _PYTHON_DATA_READ_PATTERNS = (
        "pd.read_sql",
        "read_sql_query",
        "read_sql_table",
        "read_csv",
        "read_parquet",
        "read_json",
        "read_excel",
        "psycopg2.connect",
        "engine.connect",
        "engine.execute",
        "session.execute",
        "cursor.execute",
        "sqlalchemy",
        "create_engine",
    )
    _DAG_FORBIDDEN_CONNECTION_PATTERNS = (
        "Variable.get(\"connection_string\")",
        "Variable.get('connection_string')",
        "BaseHook.get_connection(\"connection_string\")",
        "BaseHook.get_connection('connection_string')",
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

    @staticmethod
    def _node_name(node: ast.AST) -> str | None:
        if isinstance(node, ast.Name):
            return node.id
        if isinstance(node, ast.Attribute):
            return node.attr
        if isinstance(node, ast.Call):
            return Validator._node_name(node.func)
        return None

    @classmethod
    def _contains_airflow_dag_definition(cls, tree: ast.AST) -> bool:
        for node in ast.walk(tree):
            if isinstance(node, ast.With):
                for item in node.items:
                    if cls._node_name(item.context_expr) == "DAG":
                        return True
            if isinstance(node, ast.Assign) and cls._node_name(node.value) == "DAG":
                return True
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                for decorator in node.decorator_list:
                    if cls._node_name(decorator) == "dag":
                        return True
        return False

    @classmethod
    def _contains_airflow_task(cls, tree: ast.AST) -> bool:
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                func_name = cls._node_name(node.func)
                if func_name == "task" or (
                    func_name is not None and func_name.endswith("Operator")
                ):
                    return True
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                for decorator in node.decorator_list:
                    if cls._node_name(decorator) == "task":
                        return True
        return False

    @staticmethod
    def _dict_string_keys(node: ast.AST) -> dict[str, ast.AST] | None:
        if not isinstance(node, ast.Dict):
            return None

        result: dict[str, ast.AST] = {}
        for key, value in zip(node.keys, node.values):
            if not isinstance(key, ast.Constant) or not isinstance(key.value, str):
                return None
            result[key.value] = value
        return result

    @classmethod
    def _collect_dict_assignments(cls, tree: ast.AST) -> dict[str, dict[str, ast.AST]]:
        assignments: dict[str, dict[str, ast.AST]] = {}
        for node in ast.walk(tree):
            if not isinstance(node, ast.Assign):
                continue

            dict_items = cls._dict_string_keys(node.value)
            if dict_items is None:
                continue

            for target in node.targets:
                if isinstance(target, ast.Name):
                    assignments[target.id] = dict_items
        return assignments

    @classmethod
    def _resolve_dict_argument(
        cls,
        node: ast.AST,
        dict_assignments: dict[str, dict[str, ast.AST]],
    ) -> dict[str, ast.AST] | None:
        inline_dict = cls._dict_string_keys(node)
        if inline_dict is not None:
            return inline_dict
        if isinstance(node, ast.Name):
            return dict_assignments.get(node.id)
        return None

    @staticmethod
    def _is_string_literal(node: ast.AST) -> bool:
        return isinstance(node, ast.Constant) and isinstance(node.value, str)

    @classmethod
    def _find_invalid_retry_delay(cls, tree: ast.AST) -> str | None:
        dict_assignments = cls._collect_dict_assignments(tree)

        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue

            func_name = cls._node_name(node.func) or "call"
            for keyword in node.keywords:
                if keyword.arg == "retry_delay" and cls._is_string_literal(keyword.value):
                    return (
                        f"{func_name} uses retry_delay as a string literal. "
                        "Airflow expects datetime.timedelta, not 'HH:MM:SS'."
                    )

                if keyword.arg != "default_args":
                    continue

                resolved_dict = cls._resolve_dict_argument(keyword.value, dict_assignments)
                if resolved_dict is None:
                    continue

                retry_delay = resolved_dict.get("retry_delay")
                if retry_delay is not None and cls._is_string_literal(retry_delay):
                    return (
                        f"{func_name} default_args uses retry_delay as a string literal. "
                        "Airflow expects datetime.timedelta, not 'HH:MM:SS'."
                    )

        return None

    @staticmethod
    def _collect_imported_names(tree: ast.AST) -> dict[str, tuple[str, str | None]]:
        imported_names: dict[str, tuple[str, str | None]] = {}

        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    local_name = alias.asname or alias.name.split(".")[0]
                    imported_names[local_name] = (alias.name, None)
            elif isinstance(node, ast.ImportFrom):
                module_name = node.module or ""
                for alias in node.names:
                    local_name = alias.asname or alias.name
                    imported_names[local_name] = (module_name, alias.name)

        return imported_names

    @classmethod
    def _find_invalid_datetime_timedelta_usage(cls, tree: ast.AST) -> str | None:
        imported_names = cls._collect_imported_names(tree)
        datetime_import = imported_names.get("datetime")

        if datetime_import is None:
            return None

        module_name, imported_symbol = datetime_import
        if imported_symbol != "datetime":
            return None

        for node in ast.walk(tree):
            if not isinstance(node, ast.Attribute):
                continue
            if cls._node_name(node.value) != "datetime" or node.attr != "timedelta":
                continue

            return (
                "Code uses datetime.timedelta(...) but 'datetime' is imported as a symbol, "
                f"not as a module (from {module_name} import datetime). "
                "Use 'from datetime import timedelta' with 'timedelta(...)' or 'import datetime'."
            )

        return None

    @staticmethod
    def _validation_result(
        errors: list[dict] | None = None,
        warnings: list[str] | None = None,
    ) -> dict:
        errors = errors or []
        warnings = warnings or []
        if errors:
            first_error = errors[0]
            return {
                "valid": False,
                "error": first_error.get("error"),
                "stage": first_error.get("stage"),
                "errors": errors,
                "warnings": warnings,
            }
        return {
            "valid": True,
            "error": None,
            "stage": None,
            "errors": [],
            "warnings": warnings,
        }

    @classmethod
    def _collect_task_ids(cls, tree: ast.AST) -> list[str]:
        task_ids: list[str] = []

        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue

            func_name = cls._node_name(node.func)
            if not (
                func_name == "task"
                or (func_name is not None and func_name.endswith("Operator"))
            ):
                continue

            for keyword in node.keywords:
                if keyword.arg != "task_id":
                    continue
                if isinstance(keyword.value, ast.Constant) and isinstance(keyword.value.value, str):
                    task_ids.append(keyword.value.value)

        return task_ids

    @classmethod
    def _validate_deprecated_imports(cls, tree: ast.AST) -> list[dict]:
        errors: list[dict] = []

        for node in ast.walk(tree):
            if not isinstance(node, ast.ImportFrom):
                continue
            module_name = node.module or ""
            if module_name in cls.DAG_DEPRECATED_IMPORTS:
                errors.append({
                    "stage": "compatibility",
                    "error": (
                        f"Deprecated import '{module_name}'. "
                        f"{cls.DAG_DEPRECATED_IMPORTS[module_name]}"
                    ),
                })

        return errors

    @classmethod
    def _validate_forbidden_patterns(cls, dag_code: str) -> list[dict]:
        errors: list[dict] = []

        for pattern, message in cls.DAG_FORBIDDEN_PATTERNS.items():
            if pattern in dag_code:
                errors.append({
                    "stage": "security",
                    "error": message,
                })

        for pattern in cls.DAG_HARDCODED_SECRET_PATTERNS:
            if pattern.search(dag_code):
                errors.append({
                    "stage": "security",
                    "error": "Possible hardcoded secret detected in DAG code.",
                })

        return errors

    @classmethod
    def _validate_dynamic_start_date(cls, tree: ast.AST) -> list[dict]:
        errors: list[dict] = []

        for node in ast.walk(tree):
            if not isinstance(node, ast.keyword) or node.arg != "start_date":
                continue
            if not isinstance(node.value, ast.Call):
                continue

            func_name = cls._node_name(node.value.func)
            if func_name in {"now", "utcnow", "today"}:
                errors.append({
                    "stage": "logic",
                    "error": "Dynamic start_date detected. Use a fixed datetime instead.",
                })

        return errors

    @classmethod
    def _validate_top_level_execution(cls, tree: ast.AST) -> list[dict]:
        errors: list[dict] = []

        for node in tree.body:
            if not isinstance(node, ast.Assign) or not isinstance(node.value, ast.Call):
                continue
            func_name = cls._node_name(node.value.func)
            if func_name in cls.DAG_FORBIDDEN_TOP_LEVEL_CALLS:
                errors.append({
                    "stage": "runtime",
                    "error": f"Forbidden top-level execution detected: {func_name}.",
                })

        return errors

    @classmethod
    def _validate_dependencies(cls, tree: ast.AST) -> tuple[list[dict], list[str]]:
        errors: list[dict] = []
        warnings: list[str] = []
        graph: defaultdict[str, set[str]] = defaultdict(set)

        for node in ast.walk(tree):
            if isinstance(node, ast.BinOp) and isinstance(node.op, ast.RShift):
                left = cls._node_name(node.left)
                right = cls._node_name(node.right)
                if left and right:
                    graph[left].add(right)

        if not graph:
            warnings.append("No task dependencies detected.")
            return errors, warnings

        visited: set[str] = set()
        recursion: set[str] = set()

        def dfs(vertex: str) -> bool:
            visited.add(vertex)
            recursion.add(vertex)
            for neighbor in graph[vertex]:
                if neighbor not in visited:
                    if dfs(neighbor):
                        return True
                elif neighbor in recursion:
                    return True
            recursion.remove(vertex)
            return False

        for vertex in list(graph):
            if vertex not in visited and dfs(vertex):
                errors.append({
                    "stage": "logic",
                    "error": "Cyclic task dependency detected.",
                })
                break

        return errors, warnings

    @classmethod
    def _validate_operator_sql(cls, tree: ast.AST) -> list[dict]:
        errors: list[dict] = []

        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            if cls._node_name(node.func) != "PostgresOperator":
                continue
            for keyword in node.keywords:
                if keyword.arg != "sql":
                    continue
                if not (
                    isinstance(keyword.value, ast.Constant)
                    and isinstance(keyword.value.value, str)
                ):
                    continue
                try:
                    parsed = sqlparse.parse(keyword.value.value)
                    if not parsed:
                        errors.append({
                            "stage": "sql",
                            "error": "PostgresOperator contains invalid SQL.",
                        })
                except Exception as exc:
                    errors.append({
                        "stage": "sql",
                        "error": f"PostgresOperator SQL parsing failed: {exc}",
                    })

        return errors

    @classmethod
    def _validate_duplicate_task_ids(cls, tree: ast.AST) -> list[dict]:
        task_ids = cls._collect_task_ids(tree)
        duplicates = [
            task_id for task_id, count in Counter(task_ids).items() if count > 1
        ]
        if not duplicates:
            return []
        return [{
            "stage": "logic",
            "error": f"Duplicate task_id values detected: {sorted(duplicates)}",
        }]

    @classmethod
    def _collect_postgres_operator_sql(cls, tree: ast.AST) -> list[tuple[str, str]]:
        tasks: list[tuple[str, str]] = []

        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            if cls._node_name(node.func) != "PostgresOperator":
                continue

            task_id: str | None = None
            sql_text: str | None = None

            for keyword in node.keywords:
                if keyword.arg == "task_id":
                    if isinstance(keyword.value, ast.Constant) and isinstance(keyword.value.value, str):
                        task_id = keyword.value.value
                elif keyword.arg == "sql":
                    if isinstance(keyword.value, ast.Constant) and isinstance(keyword.value.value, str):
                        sql_text = keyword.value.value

            if task_id and sql_text:
                tasks.append((task_id, sql_text))

        return tasks

    @staticmethod
    def _normalize_sql_identifier(identifier: str) -> str:
        return identifier.strip().strip('"').lower()

    @classmethod
    def _validate_cross_task_temp_tables(cls, tree: ast.AST) -> list[dict]:
        errors: list[dict] = []
        sql_tasks = cls._collect_postgres_operator_sql(tree)
        if len(sql_tasks) < 2:
            return errors

        temp_table_pattern = re.compile(
            r"\bcreate\s+temp(?:orary)?\s+table\s+(?:if\s+not\s+exists\s+)?([a-zA-Z_][a-zA-Z0-9_\.]*)",
            re.IGNORECASE,
        )
        from_join_pattern = re.compile(
            r"\b(?:from|join|into|update)\s+([a-zA-Z_][a-zA-Z0-9_\.]*)",
            re.IGNORECASE,
        )

        temp_tables_by_task: dict[str, set[str]] = {}
        for task_id, sql_text in sql_tasks:
            temp_tables = {
                cls._normalize_sql_identifier(match.group(1))
                for match in temp_table_pattern.finditer(sql_text)
            }
            if temp_tables:
                temp_tables_by_task[task_id] = temp_tables

        if not temp_tables_by_task:
            return errors

        for producer_task_id, temp_tables in temp_tables_by_task.items():
            for consumer_task_id, sql_text in sql_tasks:
                if consumer_task_id == producer_task_id:
                    continue

                referenced_tables = {
                    cls._normalize_sql_identifier(match.group(1))
                    for match in from_join_pattern.finditer(sql_text)
                }
                reused_tables = sorted(temp_tables & referenced_tables)
                if reused_tables:
                    errors.append({
                        "stage": "runtime",
                        "error": (
                            f"Task '{consumer_task_id}' reads temporary table(s) {reused_tables} "
                            f"created in task '{producer_task_id}'. Temporary tables are session-scoped "
                            "and cannot be shared across separate Airflow tasks."
                        ),
                    })

        return errors

    @classmethod
    def _expression_uses_dag_start_date(cls, node: ast.AST) -> bool:
        for child in ast.walk(node):
            if isinstance(child, ast.Attribute):
                if cls._node_name(child.value) == "dag" and child.attr == "start_date":
                    return True
            if isinstance(child, ast.Call):
                if cls._node_name(child.func) == "date":
                    if isinstance(child.func, ast.Attribute):
                        value = child.func.value
                        if isinstance(value, ast.Attribute):
                            if cls._node_name(value.value) == "dag" and value.attr == "start_date":
                                return True
        return False

    @classmethod
    def _validate_dag_start_date_fstrings(cls, tree: ast.AST) -> list[dict]:
        errors: list[dict] = []

        for node in ast.walk(tree):
            if not isinstance(node, ast.JoinedStr):
                continue

            for value in node.values:
                if not isinstance(value, ast.FormattedValue):
                    continue
                if not cls._expression_uses_dag_start_date(value.value):
                    continue

                errors.append({
                    "stage": "runtime",
                    "error": (
                        "DAG code interpolates dag.start_date inside an f-string. "
                        "This is evaluated at import time and may fail because dag.start_date can be None. "
                        "Use Airflow macros like {{ ds }}, data_interval_start, CURRENT_DATE, or a fixed literal instead."
                    ),
                })
                return errors

        return errors

    @classmethod
    def _validate_schema_qualified_postgres_tables(
        cls,
        tree: ast.AST,
        required_schema: str,
    ) -> list[dict]:
        errors: list[dict] = []
        sql_tasks = cls._collect_postgres_operator_sql(tree)
        if not sql_tasks or not required_schema:
            return errors

        cte_pattern = re.compile(
            r"\bwith\s+([a-zA-Z_][a-zA-Z0-9_]*)\s+as\b",
            re.IGNORECASE,
        )
        table_ref_pattern = re.compile(
            r"\b(create\s+table(?:\s+if\s+not\s+exists)?|insert\s+into|from|join|update)\s+"
            r"([a-zA-Z_][a-zA-Z0-9_\.]*)",
            re.IGNORECASE,
        )

        for task_id, sql_text in sql_tasks:
            cte_names = {
                cls._normalize_sql_identifier(match.group(1))
                for match in cte_pattern.finditer(sql_text)
            }
            for match in table_ref_pattern.finditer(sql_text):
                operation = match.group(1).lower()
                identifier = match.group(2)
                normalized = cls._normalize_sql_identifier(identifier)

                if "." in normalized:
                    schema_name = normalized.split(".", 1)[0]
                    if schema_name != required_schema.lower():
                        errors.append({
                            "stage": "logic",
                            "error": (
                                f"Task '{task_id}' uses table '{identifier}' in {operation} with schema "
                                f"'{schema_name}'. Expected schema '{required_schema}'."
                            ),
                        })
                    continue

                if normalized in cte_names or normalized == "excluded":
                    continue
                if normalized.startswith("temp_") or normalized.startswith("tmp_"):
                    continue

                errors.append({
                    "stage": "logic",
                    "error": (
                        f"Task '{task_id}' uses table '{identifier}' in {operation} without schema qualification. "
                        f"Use explicit names like '{required_schema}.{identifier}'."
                    ),
                })

        return errors

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
        materializing_types = {
            "CREATE", "INSERT", "UPDATE", "DELETE",
            "TRUNCATE", "ALTER", "DROP", "MERGE",
        }
        statement_types = [
            self._statement_type(s) for s in statements_text
        ]

        if all(t == "SELECT" for t in statement_types):
            return {
                "valid": False,
                "error": (
                    "SQL script contains only SELECT statements. "
                    "For ETL generation a materializing script is required: "
                    "CREATE TABLE AS, INSERT INTO ... SELECT, TRUNCATE + INSERT, or similar."
                ),
                "stage": "logic",
            }

        if not any(t in materializing_types for t in statement_types):
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

        code_lower = python_code.lower()
        has_data_read = any(p.lower() in code_lower for p in self._PYTHON_DATA_READ_PATTERNS)

        if not has_data_read:
            return {
                "valid": False,
                "error": (
                    "Data read step not found. "
                    "Expected one of: pd.read_sql, read_csv, psycopg2.connect, "
                    "create_engine, engine.connect, cursor.execute, etc."
                ),
                "stage": "logic",
            }

        return {"valid": True, "error": None}

    def validate_dag(self, dag_code: str) -> dict:
        try:
            tree = ast.parse(dag_code)
        except SyntaxError as exc:
            return self._validation_result([{
                "stage": "syntax",
                "error": f"Python syntax error: {exc}",
            }])

        errors: list[dict] = []
        warnings: list[str] = []

        if not self._contains_airflow_dag_definition(tree):
            errors.append({
                "stage": "structure",
                "error": "Airflow DAG definition not found. Expected DAG(...), with DAG(...), or @dag.",
            })

        if not self._contains_airflow_task(tree):
            errors.append({
                "stage": "structure",
                "error": "No Airflow task/operator found. Expected @task decorator or *Operator usage.",
            })

        if errors:
            return self._validation_result(errors, warnings)

        errors.extend(self._validate_forbidden_patterns(dag_code))
        errors.extend(self._validate_deprecated_imports(tree))
        errors.extend(self._validate_dynamic_start_date(tree))
        errors.extend(self._validate_top_level_execution(tree))
        errors.extend(self._validate_operator_sql(tree))
        errors.extend(self._validate_duplicate_task_ids(tree))
        errors.extend(self._validate_cross_task_temp_tables(tree))
        errors.extend(self._validate_dag_start_date_fstrings(tree))
        errors.extend(self._validate_schema_qualified_postgres_tables(tree, settings.work_db_schema))

        if "PostgresOperator" in dag_code:
            postgres_conn_variants = (
                "postgres_conn_id='postgres_default'",
                'postgres_conn_id="postgres_default"',
            )

            if not any(v in dag_code for v in postgres_conn_variants):
                errors.append({
                    "stage": "logic",
                    "error": (
                        "PostgresOperator must explicitly use "
                        "postgres_conn_id='postgres_default'."
                    ),
                })

        retry_delay_error = self._find_invalid_retry_delay(tree)
        if retry_delay_error:
            errors.append({
                "stage": "logic",
                "error": retry_delay_error,
            })

        datetime_timedelta_error = self._find_invalid_datetime_timedelta_usage(tree)
        if datetime_timedelta_error:
            errors.append({
                "stage": "logic",
                "error": datetime_timedelta_error,
            })

        dag_calls = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                func_name = self._node_name(node.func)
                if func_name == "DAG":
                    dag_calls.append(node)

        if not dag_calls:
            warnings.append("No direct DAG(...) constructor found. Validation may be incomplete for dynamically generated DAGs.")

        for dag_call in dag_calls:
            keyword_map = {
                kw.arg: kw.value
                for kw in dag_call.keywords
                if kw.arg
            }

            schedule_node = (
                keyword_map.get("schedule_interval")
                or keyword_map.get("schedule")
            )

            if schedule_node is None:
                warnings.append(
                    "DAG has no schedule defined."
                )

            elif isinstance(schedule_node, ast.Constant):

                if schedule_node.value in ("@once", None):
                    warnings.append(
                        "DAG runs only once."
                    )
                elif isinstance(schedule_node.value, str):
                    known_presets = {
                        "@hourly",
                        "@daily",
                        "@weekly",
                        "@monthly",
                        "@yearly",
                        "@annually",
                    }

                    if (
                        not schedule_node.value.startswith("@")
                        and len(schedule_node.value.split()) not in (5, 6)
                    ):
                        errors.append({
                            "stage": "logic",
                            "error": (
                                f"Invalid cron expression: "
                                f"{schedule_node.value}"
                            ),
                        })

                    if (
                        schedule_node.value.startswith("@")
                        and schedule_node.value not in known_presets
                    ):
                        warnings.append(
                            f"Unknown Airflow preset schedule: "
                            f"{schedule_node.value}"
                        )

            catchup_node = keyword_map.get("catchup")
            if isinstance(catchup_node, ast.Constant):
                if catchup_node.value is True:
                    warnings.append("catchup=True may create large historical backfills.")

            default_args_node = keyword_map.get("default_args")
            if default_args_node:
                dict_assignments = self._collect_dict_assignments(tree)
                resolved = self._resolve_dict_argument(
                    default_args_node,
                    dict_assignments,
                )

                if resolved:
                    if "retries" not in resolved:
                        warnings.append("default_args does not define retries.")

                    if "owner" not in resolved:
                        warnings.append("default_args does not define owner.")

                    if "start_date" not in resolved:
                        errors.append({
                            "stage": "logic",
                            "error": "default_args must define start_date.",
                        })

        dependency_errors, dependency_warnings = self._validate_dependencies(tree)
        errors.extend(dependency_errors)
        warnings.extend(dependency_warnings)

        return self._validation_result(errors, warnings)
    
