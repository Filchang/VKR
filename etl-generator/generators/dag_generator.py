from datetime import date, datetime
from pathlib import Path

import yaml
from jinja2 import Environment, FileSystemLoader

from core.gigachat_client import GigaChatClient
from core.prompt_builder import PromptBuilder
from core.response_parser import ResponseParser


class DAGGenerator:
    def __init__(self):
        self.gigachat_client = GigaChatClient()
        self.prompt_builder = PromptBuilder()
        self.response_parser = ResponseParser()

        templates_dir = Path(__file__).resolve().parent
        environment = Environment(loader=FileSystemLoader(str(templates_dir)))
        self.template = environment.get_template("dag_template.j2")

    def _parse_start_date(self, start_date: str | date | datetime) -> tuple[int, int, int]:
        if isinstance(start_date, datetime):
            parsed = start_date.date()
        elif isinstance(start_date, date):
            parsed = start_date
        else:
            parsed = datetime.strptime(start_date, "%Y-%m-%d").date()

        return parsed.year, parsed.month, parsed.day

    def generate(self, task_description: str, schema: dict, dag_config: dict) -> dict:
        system_prompt = (
            f"{self.prompt_builder.build_system_prompt()}\n\n"
            "Для Airflow DAG верни только YAML в блоке ```yaml. "
            "YAML должен содержать поля tasks (list) и dependencies (list). "
            "Каждый элемент tasks должен содержать task_id, function_name и code. "
            "Каждый элемент dependencies должен содержать from и to."
        )
        user_prompt = (
            f"{self.prompt_builder.build_user_prompt(task_description, schema, output_format='airflow_dag')}\n\n"
            f"Конфигурация DAG:\n"
            f"- dag_id: {dag_config['dag_id']}\n"
            f"- schedule: {dag_config['schedule']}\n"
            f"- retries: {dag_config['retries']}\n"
            f"- start_date: {dag_config['start_date']}"
        )

        response = self.gigachat_client.send_message_with_retry(
            system_prompt,
            user_prompt,
        )
        parsed_response = self.response_parser.parse_etl_response(response, "yaml")

        if not parsed_response["success"]:
            return {
                "dag_code": "",
                "dag_id": dag_config["dag_id"],
                "success": False,
            }

        yaml_data = yaml.safe_load(parsed_response["code"])
        start_year, start_month, start_day = self._parse_start_date(
            dag_config["start_date"]
        )

        dag_code = self.template.render(
            dag_id=dag_config["dag_id"],
            schedule=dag_config["schedule"],
            retries=dag_config["retries"],
            retry_delay_minutes=dag_config.get("retry_delay_minutes", 5),
            start_year=start_year,
            start_month=start_month,
            start_day=start_day,
            tasks=yaml_data.get("tasks", []),
            dependencies=yaml_data.get("dependencies", []),
        )

        return {
            "dag_code": dag_code,
            "dag_id": dag_config["dag_id"],
            "success": True,
        }
