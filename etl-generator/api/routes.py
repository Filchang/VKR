import time
from datetime import datetime
from typing import Any

import sqlparse
from fastapi import APIRouter, BackgroundTasks, Depends, File, HTTPException, UploadFile
from sqlalchemy import func, text
from sqlalchemy.orm import Session

from api.database import SessionLocal, apply_work_schema, engine, get_db
from api.models import ETLTask, ExecutionLog, GeneratedArtifact
from api.schemas import (
    ArtifactResponse,
    CSVTableInfo,
    CSVUploadResponse,
    ExecutionLogResponse,
    GenerateRequest,
    GenerateResponse,
    RunResponse,
    SchemaResponse,
    SchemaTableColumn,
    StatsResponse,
    TaskResponse,
)
from core.airflow_deployer import AirflowDeployer
from core.config import settings
from core.csv_processor import CSVProcessor
from core.etl_classifier import ETLClassifier
from core.schema_inspector import SchemaInspector
from generators.dag_generator import DAGGenerator
from generators.python_generator import PythonGenerator
from generators.sql_generator import SQLGenerator
from generators.validator import Validator


router = APIRouter()
_classifier = ETLClassifier()


def _to_json_safe(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (bool, int, float, str)):
        return value
    return str(value)


def _format_validation_error(validation_result: dict) -> str | None:
    error_message = validation_result.get("error")
    stage = validation_result.get("stage")

    if not error_message:
        errors = validation_result.get("errors") or []
        if errors:
            first_error = errors[0]
            error_message = first_error.get("error") or first_error.get("message")
            stage = stage or first_error.get("stage")

    if not error_message:
        return None

    if stage:
        return f"Validation failed at stage '{stage}': {error_message}"
    return f"Validation failed: {error_message}"


def _format_exception(stage: str, exc: Exception) -> str:
    return f"{stage}: {exc}"


def _serialize_task(task: ETLTask, artifacts: list[GeneratedArtifact]) -> TaskResponse:
    return TaskResponse(
        id=task.id,
        created_at=task.created_at,
        started_at=task.started_at,
        task_description=task.task_description,
        output_format=task.output_format,
        status=task.status,
        source_tables=task.source_tables,
        error_message=task.error_message,
        etl_pattern=task.etl_pattern,
        generation_time_ms=task.generation_time_ms,
        artifacts=[
            ArtifactResponse(
                id=artifact.id,
                task_id=artifact.task_id,
                artifact_type=artifact.artifact_type,
                code=artifact.code,
                language=artifact.language,
                is_valid=artifact.is_valid,
                validation_error=artifact.validation_error,
                attempts=artifact.attempts,
                created_at=artifact.created_at,
            )
            for artifact in artifacts
        ],
    )


def _serialize_log(log: ExecutionLog) -> ExecutionLogResponse:
    return ExecutionLogResponse(
        id=log.id,
        artifact_id=log.artifact_id,
        started_at=log.started_at,
        finished_at=log.finished_at,
        status=log.status,
        log_output=log.log_output,
        airflow_run_id=log.airflow_run_id,
    )


def poll_airflow_status(log_id: int, dag_id: str, dag_run_id: str) -> None:
    db = SessionLocal()
    try:
        final_status = AirflowDeployer().wait_for_completion(dag_id, dag_run_id)
        log = db.query(ExecutionLog).filter(ExecutionLog.id == log_id).first()
        if log is None:
            return
        state = final_status.get("state", "error")
        log.status = "success" if state == "success" else "error"
        log.finished_at = datetime.utcnow()
        log.log_output = (
            f"Airflow state: {state}. "
            f"start_date={final_status.get('start_date')}, "
            f"end_date={final_status.get('end_date')}"
        )
        db.commit()
    except Exception as exc:
        log = db.query(ExecutionLog).filter(ExecutionLog.id == log_id).first()
        if log is not None:
            log.status = "error"
            log.finished_at = datetime.utcnow()
            log.log_output = _format_exception("Airflow monitoring error", exc)
            db.commit()
    finally:
        db.close()


def run_generation(task_id: int) -> None:
    db = SessionLocal()
    try:
        task = db.query(ETLTask).filter(ETLTask.id == task_id).first()
        if task is None:
            return

        task.status = "processing"
        task.started_at = datetime.utcnow()
        task.error_message = None
        db.commit()

        classification = _classifier.classify(task.task_description)
        task.etl_pattern = classification.pattern
        db.commit()

        inspector = SchemaInspector(settings.db_url, settings.work_db_schema)
        schema = inspector.get_full_schema(task.source_tables)

        gen_start = time.monotonic()
        output_format = task.output_format

        if output_format == "sql":
            generator = SQLGenerator()
            result = generator.generate_with_correction(
                task.task_description, schema, classification=classification
            )
            artifact_code = result.get("sql", "")
            language = "sql"
        elif output_format == "python":
            generator = PythonGenerator()
            result = generator.generate_with_dependencies(task.task_description, schema)
            artifact_code = result.get("python", "")
            language = "python"
        elif output_format == "airflow_dag":
            generator = DAGGenerator()
            dag_config = {
                "dag_id": f"etl_task_{task_id}",
                "schedule": task.dag_schedule or "@daily",
                "retries": 1,
                "start_date": datetime.utcnow().strftime("%Y-%m-%d"),
            }
            result = generator.generate_with_correction(
                task.task_description, schema, dag_config
            )
            artifact_code = result.get("dag_code", "")
            language = "python"
        else:
            raise ValueError(f"Unsupported output format: {output_format}")

        gen_ms = int((time.monotonic() - gen_start) * 1000)
        task.generation_time_ms = gen_ms
        db.commit()

        if not result.get("success"):
            validator_result = {
                "valid": False,
                "error": result.get("error") or "Generation failed",
                "stage": result.get("error_stage", "generation"),
            }
        else:
            validator = Validator(settings.db_url)
            if output_format == "sql":
                validator_result = validator.validate_sql(artifact_code)
            elif output_format == "python":
                validator_result = validator.validate_python(artifact_code)
            else:
                validator_result = validator.validate_dag(artifact_code)

        artifact = GeneratedArtifact(
            task_id=task_id,
            artifact_type=output_format,
            code=artifact_code,
            language=language,
            is_valid=validator_result["valid"],
            validation_error=_format_validation_error(validator_result),
            attempts=result.get("attempts", 1),
        )
        db.add(artifact)
        db.commit()

        task.status = "done"
        if not result.get("success") or not validator_result["valid"]:
            task.status = "error"
            task.error_message = (
                result.get("error")
                or _format_validation_error(validator_result)
                or "Generation failed"
            )
        db.commit()
    except Exception as exc:
        task = db.query(ETLTask).filter(ETLTask.id == task_id).first()
        if task is not None:
            task.status = "error"
            task.error_message = _format_exception("Generation pipeline error", exc)
            db.commit()
    finally:
        db.close()


@router.post("/api/generate", response_model=GenerateResponse)
def generate_etl(
    request: GenerateRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
) -> GenerateResponse:
    classification = _classifier.classify(request.task_description)

    task = ETLTask(
        task_description=request.task_description,
        output_format=request.output_format,
        status="pending",
        source_tables=request.source_tables,
        etl_pattern=classification.pattern,
        dag_schedule=request.dag_schedule,
    )
    db.add(task)
    db.commit()
    db.refresh(task)

    background_tasks.add_task(run_generation, task.id)

    return GenerateResponse(
        task_id=task.id,
        status="pending",
        etl_pattern=classification.pattern,
    )


@router.get("/api/tasks/{task_id}", response_model=TaskResponse)
def get_task(task_id: int, db: Session = Depends(get_db)) -> TaskResponse:
    task = db.query(ETLTask).filter(ETLTask.id == task_id).first()
    if task is None:
        raise HTTPException(status_code=404, detail="Task not found")

    artifacts = (
        db.query(GeneratedArtifact)
        .filter(GeneratedArtifact.task_id == task_id)
        .order_by(GeneratedArtifact.created_at.desc())
        .all()
    )
    return _serialize_task(task, artifacts)


@router.get("/api/tasks", response_model=list[TaskResponse])
def list_tasks(db: Session = Depends(get_db)) -> list[TaskResponse]:
    tasks = db.query(ETLTask).order_by(ETLTask.created_at.desc()).limit(50).all()
    task_ids = [task.id for task in tasks]

    artifacts = (
        db.query(GeneratedArtifact)
        .filter(GeneratedArtifact.task_id.in_(task_ids))
        .order_by(GeneratedArtifact.created_at.desc())
        .all()
        if task_ids
        else []
    )

    artifacts_by_task: dict[int, list[GeneratedArtifact]] = {}
    for artifact in artifacts:
        artifacts_by_task.setdefault(artifact.task_id, []).append(artifact)

    return [_serialize_task(task, artifacts_by_task.get(task.id, [])) for task in tasks]


@router.delete("/api/tasks/{task_id}", status_code=204)
def delete_task(task_id: int, db: Session = Depends(get_db)) -> None:
    task = db.query(ETLTask).filter(ETLTask.id == task_id).first()
    if task is None:
        raise HTTPException(status_code=404, detail="Task not found")

    artifact_ids = [
        a.id
        for a in db.query(GeneratedArtifact)
        .filter(GeneratedArtifact.task_id == task_id)
        .all()
    ]
    if artifact_ids:
        db.query(ExecutionLog).filter(
            ExecutionLog.artifact_id.in_(artifact_ids)
        ).delete(synchronize_session=False)
        db.query(GeneratedArtifact).filter(
            GeneratedArtifact.task_id == task_id
        ).delete(synchronize_session=False)

    db.delete(task)
    db.commit()


@router.post("/api/run/{artifact_id}", response_model=RunResponse)
def run_artifact(
    artifact_id: int,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
) -> RunResponse:
    artifact = (
        db.query(GeneratedArtifact)
        .filter(GeneratedArtifact.id == artifact_id)
        .first()
    )
    if artifact is None:
        raise HTTPException(status_code=404, detail="Artifact not found")

    if artifact.artifact_type == "python":
        return RunResponse(
            artifact_id=artifact.id,
            status="manual",
            message="Скачайте скрипт и запустите его локально",
            airflow_run_id=None,
        )

    if artifact.artifact_type == "sql":
        log = ExecutionLog(
            artifact_id=artifact_id,
            started_at=datetime.utcnow(),
            status="running",
        )
        db.add(log)
        db.commit()
        db.refresh(log)

        try:
            result_rows: list[dict] | None = None
            with engine.begin() as connection:
                apply_work_schema(connection)
                statements = [s.strip() for s in sqlparse.split(artifact.code) if s.strip()]
                last_result = None
                for stmt in statements:
                    last_result = connection.execute(text(stmt))
                if last_result is not None and last_result.returns_rows:
                    raw_rows = last_result.mappings().all()
                    result_rows = [
                        {k: _to_json_safe(v) for k, v in dict(row).items()}
                        for row in raw_rows[:500]
                    ]

            row_msg = f" Получено строк: {len(result_rows)}." if result_rows else ""
            log.status = "success"
            log.finished_at = datetime.utcnow()
            log.log_output = f"SQL executed successfully.{row_msg}"
            db.commit()
            return RunResponse(
                artifact_id=artifact.id,
                status="success",
                message=f"SQL выполнен успешно.{row_msg}",
                airflow_run_id=None,
                result_data=result_rows,
            )
        except Exception as exc:
            log.status = "error"
            log.finished_at = datetime.utcnow()
            log.log_output = _format_exception("SQL execution error", exc)
            db.commit()
            return RunResponse(
                artifact_id=artifact.id,
                status="error",
                message=_format_exception("SQL execution error", exc),
                airflow_run_id=None,
            )

    if artifact.artifact_type == "airflow_dag":
        deployer = AirflowDeployer()
        task = db.query(ETLTask).filter(ETLTask.id == artifact.task_id).first()
        dag_id = f"etl_task_{task.id}" if task is not None else f"artifact_{artifact.id}"

        deploy_result = deployer.deploy_dag(dag_id, artifact.code)
        if not deploy_result["deployed"]:
            log = ExecutionLog(
                artifact_id=artifact_id,
                started_at=datetime.utcnow(),
                finished_at=datetime.utcnow(),
                status="error",
                log_output=deploy_result["error"],
            )
            db.add(log)
            db.commit()
            return RunResponse(
                artifact_id=artifact.id,
                status="error",
                message=deploy_result["error"],
                airflow_run_id=None,
            )

        trigger_result = deployer.trigger_dag(dag_id)
        if trigger_result["error"]:
            log = ExecutionLog(
                artifact_id=artifact_id,
                started_at=datetime.utcnow(),
                finished_at=datetime.utcnow(),
                status="error",
                log_output=trigger_result["error"],
            )
            db.add(log)
            db.commit()
            return RunResponse(
                artifact_id=artifact.id,
                status="error",
                message=trigger_result["error"],
                airflow_run_id=None,
            )

        log = ExecutionLog(
            artifact_id=artifact_id,
            started_at=datetime.utcnow(),
            status="running",
            log_output="DAG deployed and triggered",
            airflow_run_id=trigger_result["dag_run_id"],
        )
        db.add(log)
        db.commit()
        db.refresh(log)

        background_tasks.add_task(
            poll_airflow_status,
            log.id,
            dag_id,
            trigger_result["dag_run_id"],
        )

        return RunResponse(
            artifact_id=artifact.id,
            status="running",
            message="DAG развёрнут и запущен, идёт мониторинг выполнения",
            airflow_run_id=trigger_result["dag_run_id"],
        )

    raise HTTPException(
        status_code=400,
        detail=f"Unsupported artifact type: {artifact.artifact_type}",
    )


@router.get("/api/logs/{artifact_id}", response_model=list[ExecutionLogResponse])
def get_artifact_logs(
    artifact_id: int,
    db: Session = Depends(get_db),
) -> list[ExecutionLogResponse]:
    artifact = (
        db.query(GeneratedArtifact)
        .filter(GeneratedArtifact.id == artifact_id)
        .first()
    )
    if artifact is None:
        raise HTTPException(status_code=404, detail="Artifact not found")

    logs = (
        db.query(ExecutionLog)
        .filter(ExecutionLog.artifact_id == artifact_id)
        .order_by(ExecutionLog.started_at.desc())
        .all()
    )
    return [_serialize_log(log) for log in logs]


@router.get("/api/schema", response_model=SchemaResponse)
def get_schema() -> SchemaResponse:
    inspector = SchemaInspector(settings.db_url, settings.work_db_schema)
    try:
        all_tables = inspector.get_all_tables()
        schema_data: dict = {}
        for table_name in all_tables:
            columns = inspector.get_table_schema(table_name)
            schema_data[table_name] = [
                SchemaTableColumn(
                    column=col["column"],
                    type=col["type"],
                    nullable=col["nullable"],
                )
                for col in columns
            ]
        return SchemaResponse(
            schema_name=settings.work_db_schema or "public",
            tables=schema_data,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Schema inspection error: {exc}") from exc


@router.get("/api/stats", response_model=StatsResponse)
def get_stats(db: Session = Depends(get_db)) -> StatsResponse:
    total = db.query(func.count(ETLTask.id)).scalar() or 0
    done = db.query(func.count(ETLTask.id)).filter(ETLTask.status == "done").scalar() or 0
    error = db.query(func.count(ETLTask.id)).filter(ETLTask.status == "error").scalar() or 0
    pending = (
        db.query(func.count(ETLTask.id))
        .filter(ETLTask.status.in_(["pending", "processing"]))
        .scalar()
        or 0
    )

    avg_attempts_row = db.query(func.avg(GeneratedArtifact.attempts)).scalar()
    avg_attempts = round(float(avg_attempts_row), 2) if avg_attempts_row else 0.0

    avg_time_row = (
        db.query(func.avg(ETLTask.generation_time_ms))
        .filter(ETLTask.generation_time_ms.isnot(None))
        .scalar()
    )
    avg_time = round(float(avg_time_row), 1) if avg_time_row else None

    pattern_rows = (
        db.query(ETLTask.etl_pattern, func.count(ETLTask.id))
        .filter(ETLTask.etl_pattern.isnot(None))
        .group_by(ETLTask.etl_pattern)
        .all()
    )
    patterns = {row[0]: row[1] for row in pattern_rows}

    format_rows = (
        db.query(ETLTask.output_format, func.count(ETLTask.id))
        .group_by(ETLTask.output_format)
        .all()
    )
    formats = {row[0]: row[1] for row in format_rows}

    success_rate = round(done / total * 100, 1) if total > 0 else 0.0

    return StatsResponse(
        total_tasks=total,
        done_tasks=done,
        error_tasks=error,
        pending_tasks=pending,
        success_rate=success_rate,
        avg_attempts=avg_attempts,
        avg_generation_time_ms=avg_time,
        patterns=patterns,
        formats=formats,
    )


# ── CSV endpoints ─────────────────────────────────────────────────────────────

@router.post("/api/csv/upload", response_model=CSVUploadResponse)
async def upload_csv(
    file: UploadFile = File(...),
    table_name: str | None = None,
) -> CSVUploadResponse:
    """
    Загрузить CSV-файл, очистить и нормализовать данные,
    создать таблицу в etl_workspace для использования в ETL.
    """
    if not file.filename or not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Ожидается файл с расширением .csv")

    content = await file.read()
    if len(content) == 0:
        raise HTTPException(status_code=400, detail="Файл пустой")

    processor = CSVProcessor(settings.db_url, settings.work_db_schema)
    try:
        result = processor.process_and_load(
            file_data=content,
            desired_table_name=table_name,
            original_filename=file.filename,
            replace_if_exists=True,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Ошибка обработки CSV: {exc}") from exc

    return CSVUploadResponse(
        table_name=result.table_name,
        schema_name=result.schema_name,
        rows_loaded=result.rows_loaded,
        columns=[
            {"name": c["name"], "sql_type": c["sql_type"], "nullable": c["nullable"]}
            for c in result.columns
        ],
        warnings=result.warnings,
        replaced_existing=result.replaced_existing,
    )


@router.get("/api/csv/tables", response_model=list[CSVTableInfo])
def list_csv_tables() -> list[CSVTableInfo]:
    """Вернуть список всех таблиц в рабочей схеме etl_workspace."""
    processor = CSVProcessor(settings.db_url, settings.work_db_schema)
    try:
        tables = processor.list_csv_tables()
        return [
            CSVTableInfo(
                table_name=t["table_name"],
                column_count=t["column_count"],
                columns=t["columns"],
            )
            for t in tables
        ]
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Ошибка получения таблиц: {exc}") from exc
