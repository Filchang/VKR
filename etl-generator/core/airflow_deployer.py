import time
from pathlib import Path

import httpx

from core.config import settings


class AirflowDeployer:
    def __init__(self):
        self.base_url = settings.airflow_url
        self.auth = (settings.airflow_user, settings.airflow_password)
        self.dags_folder = settings.dags_folder

    def deploy_dag(self, dag_id: str, dag_code: str) -> dict:
        dag_path = Path(self.dags_folder) / f"{dag_id}.py"
        dag_path.parent.mkdir(parents=True, exist_ok=True)
        dag_path.write_text(dag_code, encoding="utf-8")

        time.sleep(3)

        try:
            with httpx.Client(auth=self.auth, timeout=30.0) as client:
                response = client.get(f"{self.base_url}/api/v1/dags/{dag_id}")
                response.raise_for_status()
            return {"deployed": True, "error": None}
        except Exception as exc:
            return {"deployed": False, "error": str(exc)}

    def trigger_dag(self, dag_id: str, conf: dict = {}) -> dict:
        try:
            with httpx.Client(auth=self.auth, timeout=30.0) as client:
                response = client.post(
                    f"{self.base_url}/api/v1/dags/{dag_id}/dagRuns",
                    json={"conf": conf},
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
                "error": str(exc),
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
