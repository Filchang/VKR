from datetime import date, datetime

from core.gigachat_client import GigaChatClient
from core.prompt_builder import PromptBuilder
from core.response_parser import ResponseParser


class DAGGenerator:
    def __init__(self):
        self.gigachat_client = GigaChatClient()
        self.prompt_builder = PromptBuilder()
        self.response_parser = ResponseParser()

    @staticmethod
    def _parse_start_date(start_date: str | date | datetime) -> tuple[int, int, int]:
        if isinstance(start_date, datetime):
            parsed = start_date.date()
        elif isinstance(start_date, date):
            parsed = start_date
        else:
            parsed = datetime.strptime(start_date, "%Y-%m-%d").date()

        return parsed.year, parsed.month, parsed.day

    def _generate_from_python_dag(self, dag_config: dict, dag_code: str) -> dict:
        return {
            "dag_code": dag_code.strip(),
            "dag_id": dag_config["dag_id"],
            "success": True,
        }

    def generate(self, task_description: str, schema: dict, dag_config: dict) -> dict:
        start_year, start_month, start_day = self._parse_start_date(
            dag_config["start_date"]
        )
        system_prompt = (
            f"{self.prompt_builder.build_system_prompt('python')}\n\n"
            "Для Airflow DAG верни только готовый Python-код в блоке ```python. "
            "Нужен полноценный исполняемый DAG для Airflow. "
            "Не используй YAML, JSON и промежуточные DSL-структуры. "
            "В коде обязательно должны быть imports Airflow, блок with DAG(...) и хотя бы один Operator."
        )
        user_prompt = (
            f"{self.prompt_builder.build_user_prompt(task_description, schema, output_format='airflow_dag')}\n\n"
            f"Конфигурация DAG:\n"
            f"- dag_id: {dag_config['dag_id']}\n"
            f"- schedule: {dag_config['schedule']}\n"
            f"- retries: {dag_config['retries']}\n"
            f"- start_date: datetime({start_year}, {start_month}, {start_day})\n"
            f"- retry_delay_minutes: {dag_config.get('retry_delay_minutes', 5)}\n\n"
            "Верни только Python-код DAG без пояснений."
        )

        response = self.gigachat_client.send_message_with_retry(
            system_prompt,
            user_prompt,
        )
        parsed_response = self.response_parser.parse_etl_response(response, "python")

        if not parsed_response["success"]:
            return {
                "dag_code": "",
                "dag_id": dag_config["dag_id"],
                "success": False,
                "error": "Failed to extract Python DAG from LLM response",
            }

        if "with DAG(" not in parsed_response["code"]:
            return {
                "dag_code": "",
                "dag_id": dag_config["dag_id"],
                "success": False,
                "error": "LLM response does not contain a valid Airflow DAG block",
            }

        return self._generate_from_python_dag(
            dag_config,
            parsed_response["code"],
        )
