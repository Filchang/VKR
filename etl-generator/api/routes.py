from datetime import datetime

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException
from sqlalchemy import text
from sqlalchemy.orm import Session

from api.database import SessionLocal, apply_work_schema, engine, get_db
from api.models import ETLTask, ExecutionLog, GeneratedArtifact
from api.schemas import (
    ArtifactResponse,
    ExecutionLogResponse,
    GenerateRequest,
    GenerateResponse,
    RunResponse,
    TaskResponse,
)
from core.airflow_deployer import AirflowDeployer
from core.config import settings
from core.schema_inspector import SchemaInspector
from generators.dag_generator import DAGGenerator
from generators.python_generator import PythonGenerator
from generators.sql_generator import SQLGenerator
from generators.validator import Validator


router = APIRouter()


def _format_validation_error(validation_result: dict) -> str | None:
    error_message = validation_result.get("error")
    if not error_message:
        return None

    stage = validation_result.get("stage")
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

        inspector = SchemaInspector(settings.db_url, settings.work_db_schema)
        schema = inspector.get_full_schema(task.source_tables)

        output_format = task.output_format
        if output_format == "sql":
            generator = SQLGenerator()
            result = generator.generate_with_correction(task.task_description, schema)
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
                "schedule": "@daily",
                "retries": 1,
                "start_date": datetime.utcnow().strftime("%Y-%m-%d"),
            }
            result = generator.generate(task.task_description, schema, dag_config)
            artifact_code = result.get("dag_code", "")
            language = "python"
        else:
            raise ValueError(f"Unsupported output format: {output_format}")

        if not result.get("success"):
            validator_result = {
                "valid": False,
                "error": result.get("error") or "Generation failed",
                "stage": "generation",
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
    task = ETLTask(
        task_description=request.task_description,
        output_format=request.output_format,
        status="pending",
        source_tables=request.source_tables,
    )
    db.add(task)
    db.commit()
    db.refresh(task)

    background_tasks.add_task(run_generation, task.id)

    return GenerateResponse(task_id=task.id, status="pending")


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
    tasks = db.query(ETLTask).order_by(ETLTask.created_at.desc()).limit(20).all()
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
            with engine.begin() as connection:
                apply_work_schema(connection)
                connection.execute(text(artifact.code))

            log.status = "success"
            log.finished_at = datetime.utcnow()
            log.log_output = "SQL executed successfully"
            db.commit()
            return RunResponse(
                artifact_id=artifact.id,
                status="success",
                message="SQL выполнен успешно",
                airflow_run_id=None,
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
