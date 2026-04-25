from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


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

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "task_description": "Собери ежедневную витрину заказов с агрегацией по пользователям.",
                "output_format": "sql",
                "source_tables": ["orders", "users"],
            }
        }
    )


class GenerateResponse(BaseModel):
    """Ответ после постановки задачи в очередь на генерацию."""

    task_id: int = Field(
        ...,
        description="Уникальный идентификатор созданной задачи.",
    )
    status: str = Field(
        ...,
        description="Начальный статус задачи после создания.",
    )

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "task_id": 42,
                "status": "pending",
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

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "id": 10,
                "task_id": 42,
                "artifact_type": "sql",
                "code": "SELECT * FROM orders;",
                "language": "sql",
                "is_valid": True,
                "validation_error": None,
                "attempts": 1,
                "created_at": "2026-04-25T14:30:00",
            }
        }
    )


class TaskResponse(BaseModel):
    """Расширенное представление ETL-задачи с вложенными артефактами."""

    id: int = Field(..., description="Идентификатор задачи.")
    created_at: datetime | None = Field(
        default=None,
        description="Время создания задачи.",
    )
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
    artifacts: list[ArtifactResponse] = Field(
        default_factory=list,
        description="Список связанных с задачей артефактов.",
    )

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "id": 42,
                "created_at": "2026-04-25T14:28:00",
                "started_at": "2026-04-25T14:28:02",
                "task_description": "Построй витрину ежедневных продаж.",
                "output_format": "python",
                "status": "done",
                "source_tables": ["orders", "payments"],
                "error_message": None,
                "artifacts": [
                    {
                        "id": 10,
                        "task_id": 42,
                        "artifact_type": "python",
                        "code": "import pandas as pd",
                        "language": "python",
                        "is_valid": True,
                        "validation_error": None,
                        "attempts": 1,
                        "created_at": "2026-04-25T14:30:00",
                    }
                ],
            }
        }
    )


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

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "artifact_id": 10,
                "status": "success",
                "message": None,
                "airflow_run_id": "manual__2026-04-25T14:45:00.000000",
            }
        }
    )


class ExecutionLogResponse(BaseModel):
    """Информация о запуске или выполнении артефакта."""

    id: int = Field(..., description="Идентификатор записи лога.")
    artifact_id: int = Field(..., description="Идентификатор артефакта.")
    started_at: datetime | None = Field(
        default=None,
        description="Время начала выполнения.",
    )
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

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "id": 5,
                "artifact_id": 10,
                "started_at": "2026-04-25T14:45:00",
                "finished_at": "2026-04-25T14:46:10",
                "status": "success",
                "log_output": "SQL executed successfully",
                "airflow_run_id": None,
            }
        }
    )
