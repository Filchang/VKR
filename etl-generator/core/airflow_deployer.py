import time
from pathlib import Path

import httpx

from core.config import settings


class AirflowDeployer:
    def __init__(self):
        self.base_url = settings.airflow_url
        self.auth = (settings.airflow_user, settings.airflow_password)
        self.dags_folder = settings.dags_folder
        self.registration_timeout_seconds = settings.airflow_registration_timeout_seconds
        self.registration_poll_interval_seconds = (
            settings.airflow_registration_poll_interval_seconds
        )

    @staticmethod
    def _build_error_message(exc: Exception) -> str:
        if isinstance(exc, httpx.HTTPStatusError):
            response = exc.response
            return f"HTTP {response.status_code}: {response.text}"
        return str(exc)

    def _wait_for_dag_registration(self, dag_id: str, timeout_seconds: int | None = None) -> None:
        timeout_seconds = timeout_seconds or self.registration_timeout_seconds
        started_at = time.time()
        last_error: str | None = None

        while time.time() - started_at < timeout_seconds:
            try:
                with httpx.Client(auth=self.auth, timeout=10.0) as client:
                    response = client.get(f"{self.base_url}/api/v1/dags/{dag_id}")
                    response.raise_for_status()

                    unpause_response = client.patch(
                        f"{self.base_url}/api/v1/dags/{dag_id}",
                        json={"is_paused": False},
                    )
                    unpause_response.raise_for_status()
                return
            except Exception as exc:
                last_error = self._build_error_message(exc)
                time.sleep(self.registration_poll_interval_seconds)

        raise RuntimeError(
            f"DAG {dag_id} was not registered in Airflow within {timeout_seconds}s. "
            f"Last error: {last_error or 'unknown'}"
        )

    def deploy_dag(self, dag_id: str, dag_code: str) -> dict:
        dag_path = Path(self.dags_folder) / f"{dag_id}.py"
        dag_path.parent.mkdir(parents=True, exist_ok=True)
        dag_path.write_text(dag_code, encoding="utf-8")

        try:
            self._wait_for_dag_registration(dag_id)
            return {"deployed": True, "error": None}
        except Exception as exc:
            return {"deployed": False, "error": self._build_error_message(exc)}

    def trigger_dag(self, dag_id: str, conf: dict | None = None) -> dict:
        try:
            with httpx.Client(auth=self.auth, timeout=30.0) as client:
                response = client.post(
                    f"{self.base_url}/api/v1/dags/{dag_id}/dagRuns",
                    json={"conf": conf or {}},
                )
                response.raise_for_status()
                data = response.json()
            return {
                "dag_run_id": data.get("dag_run_id", ""),
                "state": data.get("state", ""),
                "error": None,
            }
        except Exception as exc:
            return {
                "dag_run_id": "",
                "state": "",
                "error": self._build_error_message(exc),
            }

    def get_dag_run_status(self, dag_id: str, dag_run_id: str) -> dict:
        with httpx.Client(auth=self.auth, timeout=30.0) as client:
            response = client.get(
                f"{self.base_url}/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}"
            )
            response.raise_for_status()
            data = response.json()

        return {
            "state": data.get("state", ""),
            "start_date": data.get("start_date", ""),
            "end_date": data.get("end_date"),
        }

    def wait_for_completion(
        self,
        dag_id: str,
        dag_run_id: str,
        timeout_seconds: int = 300,
    ) -> dict:
        started_at = time.time()

        while time.time() - started_at < timeout_seconds:
            status = self.get_dag_run_status(dag_id, dag_run_id)
            if status["state"] in ("success", "failed"):
                return status
            time.sleep(5)

        return self.get_dag_run_status(dag_id, dag_run_id)
