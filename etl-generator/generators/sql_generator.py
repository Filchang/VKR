from __future__ import annotations

from typing import TYPE_CHECKING

from core.config import settings
from core.gigachat_client import GigaChatClient
from core.prompt_builder import PromptBuilder
from core.response_parser import ResponseParser
from generators.validator import Validator

if TYPE_CHECKING:
    from core.etl_classifier import ETLClassification


class SQLGenerator:
    def __init__(self):
        self.gigachat_client = GigaChatClient()
        self.prompt_builder = PromptBuilder()
        self.response_parser = ResponseParser()
        self.validator = Validator(settings.db_url)

    @staticmethod
    def _build_sql_system_prompt(base_prompt: str) -> str:
        return (
            f"{base_prompt}\n\n"
            "Return executable SQL only. Do not use placeholders or bind parameters. "
            "Forbidden forms: %s, %(name)s, :name, ?, $1. "
            "If filtering by date or status, write the values directly in SQL using literals or SQL expressions "
            "such as CURRENT_DATE, INTERVAL, DATE('now', '-30 day'), or explicit string literals."
        )

    def generate(
        self,
        task_description: str,
        schema: dict,
        classification: ETLClassification | None = None,
    ) -> dict:
        system_prompt = self._build_sql_system_prompt(
            self.prompt_builder.build_system_prompt("sql")
        )
        user_prompt = self.prompt_builder.build_user_prompt(
            task_description,
            schema,
            output_format="sql",
            etl_classification=classification,
        )
        response = self.gigachat_client.send_message_with_retry(system_prompt, user_prompt)
        parsed_response = self.response_parser.parse_etl_response(response, "sql")

        return {
            "sql": parsed_response["code"],
            "raw_response": parsed_response["raw_response"],
            "success": parsed_response["success"],
        }

    def generate_with_correction(
        self,
        task_description: str,
        schema: dict,
        max_corrections: int = 2,
        classification: ETLClassification | None = None,
    ) -> dict:
        result = self.generate(task_description, schema, classification=classification)
        attempts = 1

        if not result["success"]:
            result["attempts"] = attempts
            result["error"] = "Failed to extract SQL from LLM response"
            return result

        validation_result = self.validator.validate_sql(result["sql"])
        while not validation_result["valid"] and attempts <= max_corrections:
            correction_prompt = self.prompt_builder.get_correction_prompt(
                result["sql"],
                validation_result["error"],
                output_format="sql",
            )
            if "placeholders or bind parameters" in validation_result["error"]:
                correction_prompt += (
                    "\nRewrite the SQL so it contains no placeholders at all. "
                    "Replace bind parameters with executable SQL literals or SQL date/time expressions. "
                    "Example: use DATE('now', '-30 day') or '2026-04-28' instead of %s or :start_date."
                )

            system_prompt = self._build_sql_system_prompt(
                self.prompt_builder.build_system_prompt("sql")
            )
            response = self.gigachat_client.send_message_with_retry(
                system_prompt,
                correction_prompt,
            )
            parsed_response = self.response_parser.parse_etl_response(response, "sql")

            result = {
                "sql": parsed_response["code"],
                "raw_response": parsed_response["raw_response"],
                "success": parsed_response["success"],
            }
            attempts += 1

            if not result["success"]:
                result["attempts"] = attempts
                result["error"] = "Failed to extract SQL from LLM response"
                return result

            validation_result = self.validator.validate_sql(result["sql"])

        result["attempts"] = attempts
        if not validation_result["valid"]:
            result["success"] = False
            result["error"] = validation_result["error"]
            result["error_stage"] = validation_result.get("stage")

        return result
