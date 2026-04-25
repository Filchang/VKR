from core.config import settings
from core.gigachat_client import GigaChatClient
from core.prompt_builder import PromptBuilder
from core.response_parser import ResponseParser
from generators.validator import Validator


class SQLGenerator:
    def __init__(self):
        self.gigachat_client = GigaChatClient()
        self.prompt_builder = PromptBuilder()
        self.response_parser = ResponseParser()
        self.validator = Validator(settings.db_url)

    def generate(self, task_description: str, schema: dict) -> dict:
        system_prompt = self.prompt_builder.build_system_prompt("sql")
        user_prompt = self.prompt_builder.build_user_prompt(
            task_description,
            schema,
            output_format="sql",
        )
        response = self.gigachat_client.send_message_with_retry(
            system_prompt,
            user_prompt,
        )
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
    ) -> dict:
        result = self.generate(task_description, schema)
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
            )
            system_prompt = self.prompt_builder.build_system_prompt("sql")
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

        return result
