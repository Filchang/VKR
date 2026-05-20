from datetime import datetime

from sqlalchemy import JSON, Boolean, DateTime, ForeignKey, Integer, MetaData, String, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from core.config import settings


SERVICE_SCHEMA = None if settings.db_url.startswith("sqlite") else settings.app_db_schema


def _qualified_table_name(table_name: str) -> str:
    if SERVICE_SCHEMA:
        return f"{SERVICE_SCHEMA}.{table_name}"
    return table_name


class Base(DeclarativeBase):
    metadata = MetaData(schema=SERVICE_SCHEMA)


class ETLTask(Base):
    __tablename__ = "etl_tasks"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    started_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    task_description: Mapped[str] = mapped_column(Text, nullable=False)
    output_format: Mapped[str] = mapped_column(String(20))
    status: Mapped[str] = mapped_column(String(20))
    source_tables: Mapped[list[str] | None] = mapped_column(JSON, nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    etl_pattern: Mapped[str | None] = mapped_column(String(30), nullable=True)
    generation_time_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)
    dag_schedule: Mapped[str | None] = mapped_column(String(100), nullable=True)


class GeneratedArtifact(Base):
    __tablename__ = "generated_artifacts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    task_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey(_qualified_table_name("etl_tasks.id")),
    )
    artifact_type: Mapped[str] = mapped_column(String(20))
    code: Mapped[str] = mapped_column(Text)
    language: Mapped[str] = mapped_column(String(20))
    is_valid: Mapped[bool] = mapped_column(Boolean, default=False)
    validation_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    attempts: Mapped[int] = mapped_column(Integer, default=1)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class ExecutionLog(Base):
    __tablename__ = "execution_logs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    artifact_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey(_qualified_table_name("generated_artifacts.id")),
    )
    started_at: Mapped[datetime] = mapped_column(DateTime)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    status: Mapped[str] = mapped_column(String(20))
    log_output: Mapped[str | None] = mapped_column(Text, nullable=True)
    airflow_run_id: Mapped[str | None] = mapped_column(String(100), nullable=True)
