import re

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from api.models import Base
from core.config import settings


engine = create_engine(settings.db_url)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
SCHEMA_NAME_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def _is_sqlite_engine() -> bool:
    return engine.dialect.name == "sqlite"


def get_work_search_path() -> list[str]:
    schemas: list[str] = []
    for schema_name in (settings.work_db_schema, settings.app_db_schema, "public"):
        if (
            schema_name
            and schema_name not in schemas
            and SCHEMA_NAME_PATTERN.match(schema_name)
        ):
            schemas.append(schema_name)
    return schemas


def apply_work_schema(connection) -> None:
    if _is_sqlite_engine():
        return

    search_path = ", ".join(get_work_search_path())
    connection.exec_driver_sql(f"SET LOCAL search_path TO {search_path}")


def _migrate_add_columns(connection) -> None:
    """Add new columns to existing tables without breaking old deployments."""
    if _is_sqlite_engine():
        _sqlite_add_column(connection, "etl_tasks", "etl_pattern", "VARCHAR(30)")
        _sqlite_add_column(connection, "etl_tasks", "generation_time_ms", "INTEGER")
        _sqlite_add_column(connection, "etl_tasks", "dag_schedule", "VARCHAR(100)")
        return

    schema = settings.app_db_schema
    tbl = f"{schema}.etl_tasks" if schema else "etl_tasks"
    for col, typedef in [
        ("etl_pattern", "VARCHAR(30)"),
        ("generation_time_ms", "INTEGER"),
        ("dag_schedule", "VARCHAR(100)"),
    ]:
        connection.execute(
            text(f"ALTER TABLE IF EXISTS {tbl} ADD COLUMN IF NOT EXISTS {col} {typedef}")
        )


def _sqlite_add_column(connection, table: str, column: str, typedef: str) -> None:
    try:
        connection.execute(text(f"ALTER TABLE {table} ADD COLUMN {column} {typedef}"))
    except Exception:
        pass


def init_db():
    if not _is_sqlite_engine():
        schemas_to_create = [settings.app_db_schema]
        if settings.work_db_schema not in ("", "public", settings.app_db_schema):
            schemas_to_create.append(settings.work_db_schema)

        with engine.begin() as connection:
            for schema_name in schemas_to_create:
                if not SCHEMA_NAME_PATTERN.match(schema_name):
                    raise ValueError(f"Invalid schema name: {schema_name}")
                connection.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema_name}"))

    Base.metadata.create_all(engine)

    with engine.begin() as connection:
        _migrate_add_columns(connection)
