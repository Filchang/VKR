from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


DAG_SCHEDULE_OPTIONS = {
    "@once": "Однократно (вручную)",
    "@hourly": "Каждый час",
    "@daily": "Ежедневно",
    "@weekly": "Еженедельно",
    "@monthly": "Ежемесячно",
}


class GenerateRequest(BaseModel):
    """Запрос на генерацию ETL-артефакта по текстовому описанию задачи."""

    task_description: str = Field(
        ...,
        description="Описание ETL-задачи на русском языке.",
    )
    output_format: Literal["sql", "python", "airflow_dag"] = Field(
        ...,
        description="Формат артефакта, который нужно сгенерировать.",
    )
    source_tables: list[str] | None = Field(
        default=None,
        description="Необязательный список таблиц-источников для ограничения схемы.",
    )
    dag_schedule: str | None = Field(
        default=None,
        description=(
            "Расписание Airflow DAG. Стандартные пресеты: @once, @hourly, @daily, @weekly, @monthly. "
            "Также поддерживается произвольное cron-выражение (например '0 6 * * 1'). "
            "Применяется только при output_format=airflow_dag."
        ),
    )

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "task_description": "Собери ежедневную витрину заказов с агрегацией по пользователям.",
                "output_format": "airflow_dag",
                "source_tables": ["orders", "users"],
                "dag_schedule": "@daily",
            }
        }
    )


class GenerateResponse(BaseModel):
    """Ответ после постановки задачи в очередь на генерацию."""

    task_id: int = Field(..., description="Уникальный идентификатор созданной задачи.")
    status: str = Field(..., description="Начальный статус задачи после создания.")
    etl_pattern: str | None = Field(
        default=None,
        description="Автоматически определённый ETL-паттерн.",
    )

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "task_id": 42,
                "status": "pending",
                "etl_pattern": "aggregation",
            }
        }
    )


class ArtifactResponse(BaseModel):
    """Описание сгенерированного артефакта, связанного с ETL-задачей."""

    id: int = Field(..., description="Идентификатор артефакта.")
    task_id: int = Field(..., description="Идентификатор связанной задачи.")
    artifact_type: str = Field(..., description="Тип артефакта: sql, python или airflow_dag.")
    code: str = Field(..., description="Содержимое сгенерированного кода.")
    language: str = Field(..., description="Язык кода для отображения и обработки.")
    is_valid: bool = Field(..., description="Результат валидации артефакта.")
    validation_error: str | None = Field(
        default=None,
        description="Текст ошибки валидации, если она возникла.",
    )
    attempts: int = Field(..., description="Количество попыток генерации или исправления.")
    created_at: datetime | None = Field(
        default=None,
        description="Время создания артефакта.",
    )

    model_config = ConfigDict(from_attributes=True)


class TaskResponse(BaseModel):
    """Расширенное представление ETL-задачи с вложенными артефактами."""

    id: int = Field(..., description="Идентификатор задачи.")
    created_at: datetime | None = Field(default=None, description="Время создания задачи.")
    started_at: datetime | None = Field(
        default=None,
        description="Время начала обработки задачи.",
    )
    task_description: str = Field(..., description="Исходное описание ETL-задачи.")
    output_format: str = Field(..., description="Формат целевого артефакта.")
    status: str = Field(..., description="Текущий статус задачи.")
    source_tables: list[str] | None = Field(
        default=None,
        description="Список таблиц, использованных как ограничение схемы.",
    )
    error_message: str | None = Field(
        default=None,
        description="Текст ошибки, если задача завершилась неуспешно.",
    )
    etl_pattern: str | None = Field(
        default=None,
        description="Определённый ETL-паттерн (aggregation, incremental, scd2, и др.).",
    )
    generation_time_ms: int | None = Field(
        default=None,
        description="Время генерации артефакта в миллисекундах.",
    )
    artifacts: list[ArtifactResponse] = Field(
        default_factory=list,
        description="Список связанных с задачей артефактов.",
    )

    model_config = ConfigDict(from_attributes=True)


class RunResponse(BaseModel):
    """Результат запуска ранее сгенерированного артефакта."""

    artifact_id: int = Field(..., description="Идентификатор запущенного артефакта.")
    status: str = Field(..., description="Статус выполнения артефакта.")
    message: str | None = Field(
        default=None,
        description="Дополнительное сообщение о результате запуска.",
    )
    airflow_run_id: str | None = Field(
        default=None,
        description="Идентификатор запуска DAG в Airflow, если применимо.",
    )
    result_data: list[dict[str, Any]] | None = Field(
        default=None,
        description="Строки результата SQL-запроса (для запросов, возвращающих данные). Максимум 500 строк.",
    )

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "artifact_id": 10,
                "status": "success",
                "message": "SQL выполнен успешно. Получено строк: 3.",
                "airflow_run_id": None,
                "result_data": [{"order_id": 1, "total": "1500.00"}],
            }
        }
    )


class ExecutionLogResponse(BaseModel):
    """Информация о запуске или выполнении артефакта."""

    id: int = Field(..., description="Идентификатор записи лога.")
    artifact_id: int = Field(..., description="Идентификатор артефакта.")
    started_at: datetime | None = Field(default=None, description="Время начала выполнения.")
    finished_at: datetime | None = Field(
        default=None,
        description="Время завершения выполнения.",
    )
    status: str = Field(..., description="Статус выполнения.")
    log_output: str | None = Field(
        default=None,
        description="Текстовый лог или сообщение об ошибке.",
    )
    airflow_run_id: str | None = Field(
        default=None,
        description="Идентификатор запуска Airflow DAG, если применимо.",
    )

    model_config = ConfigDict(from_attributes=True)


class SchemaTableColumn(BaseModel):
    column: str
    type: str
    nullable: bool


class SchemaResponse(BaseModel):
    """Схема базы данных рабочего пространства."""

    schema_name: str
    tables: dict[str, list[SchemaTableColumn]]


class StatsResponse(BaseModel):
    """Агрегированная статистика системы генерации ETL."""

    total_tasks: int
    done_tasks: int
    error_tasks: int
    pending_tasks: int
    success_rate: float
    avg_attempts: float
    avg_generation_time_ms: float | None
    patterns: dict[str, int]
    formats: dict[str, int]


# ── CSV ──────────────────────────────────────────────────────────────────────

class CSVColumnInfo(BaseModel):
    name: str
    sql_type: str
    nullable: bool


class CSVUploadResponse(BaseModel):
    """Результат загрузки и обработки CSV-файла."""

    table_name: str
    schema_name: str
    rows_loaded: int
    columns: list[CSVColumnInfo]
    warnings: list[str] = Field(default_factory=list)
    replaced_existing: bool = False

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "table_name": "sales_2026",
                "schema_name": "etl_workspace",
                "rows_loaded": 1540,
                "columns": [
                    {"name": "order_id", "sql_type": "BIGINT", "nullable": False},
                    {"name": "amount", "sql_type": "DOUBLE PRECISION", "nullable": True},
                ],
                "warnings": ["Удалено 3 дублирующихся строк."],
                "replaced_existing": True,
            }
        }
    )


class CSVTableInfo(BaseModel):
    table_name: str
    column_count: int
    columns: list[dict]
