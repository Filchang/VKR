import re
from datetime import date, datetime

from core.config import settings
from core.gigachat_client import GigaChatClient
from core.prompt_builder import PromptBuilder
from core.response_parser import ResponseParser
from generators.validator import Validator


class DAGGenerator:
    def __init__(self):
        self.gigachat_client = GigaChatClient()
        self.prompt_builder = PromptBuilder()
        self.response_parser = ResponseParser()
        self.validator = Validator(settings.db_url)

    @staticmethod
    def _parse_start_date(start_date: str | date | datetime) -> tuple[int, int, int]:
        if isinstance(start_date, datetime):
            parsed = start_date.date()
        elif isinstance(start_date, date):
            parsed = start_date
        else:
            parsed = datetime.strptime(start_date, "%Y-%m-%d").date()
        return parsed.year, parsed.month, parsed.day

    @staticmethod
    def _looks_like_airflow_dag(dag_code: str) -> bool:
        dag_patterns = (
            r"\bwith\s+DAG\s*\(",
            r"\bDAG\s*\(",
            r"@dag\b",
        )
        task_patterns = (
            r"\b[A-Za-z_][A-Za-z0-9_]*Operator\s*\(",
            r"@task\b",
        )
        has_dag = any(re.search(pattern, dag_code) for pattern in dag_patterns)
        has_task = any(re.search(pattern, dag_code) for pattern in task_patterns)
        return has_dag and has_task

    def _build_prompts(
        self,
        task_description: str,
        schema: dict,
        dag_config: dict,
    ) -> tuple[str, str]:
        start_year, start_month, start_day = self._parse_start_date(dag_config["start_date"])
        retry_delay_minutes = dag_config.get("retry_delay_minutes", 5)

        system_prompt = (
            f"{self.prompt_builder.build_system_prompt('airflow_dag')}\n\n"
            "Return only executable Python code inside a ```python block. "
            "The result must be a complete Airflow DAG."
        )
        user_prompt = (
            f"{self.prompt_builder.build_user_prompt(task_description, schema, output_format='airflow_dag')}\n\n"
            "DAG configuration:\n"
            f"- dag_id: {dag_config['dag_id']}\n"
            f"- schedule: {dag_config['schedule']}\n"
            f"- retries: {dag_config['retries']}\n"
            f"- start_date: datetime({start_year}, {start_month}, {start_day})\n"
            f"- retry_delay_minutes: {retry_delay_minutes}\n"
            f"- retry_delay_python: timedelta(minutes={retry_delay_minutes})\n\n"
            "Return Python DAG code only, without explanations."
        )
        return system_prompt, user_prompt

    def generate(self, task_description: str, schema: dict, dag_config: dict) -> dict:
        system_prompt, user_prompt = self._build_prompts(task_description, schema, dag_config)
        response = self.gigachat_client.send_message_with_retry(system_prompt, user_prompt)
        parsed = self.response_parser.parse_etl_response(response, "python")

        if not parsed["success"]:
            return {
                "dag_code": "",
                "dag_id": dag_config["dag_id"],
                "success": False,
                "error": "Failed to extract Python DAG from the model response.",
            }

        if not self._looks_like_airflow_dag(parsed["code"]):
            return {
                "dag_code": "",
                "dag_id": dag_config["dag_id"],
                "success": False,
                "error": "Model response does not contain a valid Airflow DAG.",
            }

        return {
            "dag_code": parsed["code"].strip(),
            "dag_id": dag_config["dag_id"],
            "success": True,
        }

    def generate_with_correction(
        self,
        task_description: str,
        schema: dict,
        dag_config: dict,
        max_corrections: int = 2,
    ) -> dict:
        result = self.generate(task_description, schema, dag_config)
        attempts = 1
        system_prompt, _ = self._build_prompts(task_description, schema, dag_config)

        if not result["success"]:
            validation_result: dict = {
                "valid": False,
                "error": result.get("error", "Model response does not contain a valid Airflow DAG."),
            }
            current_code = result.get("dag_code", "")
        else:
            validation_result = self.validator.validate_dag(result["dag_code"])
            current_code = result["dag_code"]

        while not validation_result["valid"] and attempts <= max_corrections:
            correction_prompt = self.prompt_builder.get_correction_prompt(
                current_code or "# No DAG code",
                validation_result["error"],
                output_format="airflow_dag",
            )
            response = self.gigachat_client.send_message_with_retry(
                system_prompt,
                correction_prompt,
            )
            parsed = self.response_parser.parse_etl_response(response, "python")
            attempts += 1

            if not parsed["success"]:
                return {
                    "dag_code": "",
                    "dag_id": dag_config["dag_id"],
                    "success": False,
                    "error": "Failed to extract Python DAG from the correction response.",
                    "attempts": attempts,
                }

            if not self._looks_like_airflow_dag(parsed["code"]):
                current_code = parsed["code"]
                validation_result = {
                    "valid": False,
                    "error": "Model response does not contain a valid Airflow DAG.",
                }
                continue

            result = {
                "dag_code": parsed["code"].strip(),
                "dag_id": dag_config["dag_id"],
                "success": True,
            }
            current_code = result["dag_code"]
            validation_result = self.validator.validate_dag(result["dag_code"])

        result["attempts"] = attempts
        if not validation_result["valid"]:
            result["success"] = False
            result["error"] = validation_result["error"]
            result["error_stage"] = validation_result.get("stage")

        return result
