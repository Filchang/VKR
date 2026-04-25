import logging

from core.config import settings
from core.gigachat_client import GigaChatClient
from core.prompt_builder import PromptBuilder
from core.response_parser import ResponseParser
from generators.validator import Validator


logger = logging.getLogger(__name__)


class RetryPipeline:
    def __init__(self):
        self.gigachat_client = GigaChatClient()
        self.prompt_builder = PromptBuilder()
        self.response_parser = ResponseParser()
        self.validator = Validator(settings.db_url)

    def _validate(self, output_format: str, code: str) -> dict:
        if output_format == "sql":
            return self.validator.validate_sql(code)
        if output_format == "python":
            return self.validator.validate_python(code)
        if output_format == "airflow_dag":
            return self.validator.validate_dag(code)
        return {"valid": False, "error": f"Unsupported output format: {output_format}"}

    def run(
        self,
        task_description: str,
        schema: dict,
        output_format: str,
        max_attempts: int = 3,
    ) -> dict:
        system_prompt = self.prompt_builder.build_system_prompt()
        user_prompt = self.prompt_builder.build_user_prompt(
            task_description,
            schema,
            output_format=output_format,
        )

        response = self.gigachat_client.send_message_with_retry(system_prompt, user_prompt)
        parsed_response = self.response_parser.parse_etl_response(response, output_format)

        code = parsed_response["code"]
        language = parsed_response["language"]
        attempts = 1
        history = []

        if not parsed_response["success"]:
            history.append(
                {
                    "attempt": attempts,
                    "code": "",
                    "error": "Failed to extract code from LLM response",
                }
            )
            return {
                "code": "",
                "language": "",
                "valid": False,
                "attempts": attempts,
                "validation_error": "Failed to extract code from LLM response",
                "history": history,
            }

        validation_result = self._validate(output_format, code)
        history.append(
            {
                "attempt": attempts,
                "code": code,
                "error": validation_result["error"],
            }
        )

        while not validation_result["valid"] and attempts < max_attempts:
            logger.error(
                "Validation failed for %s on attempt %s/%s: %s",
                output_format,
                attempts,
                max_attempts,
                validation_result["error"],
            )

            correction_prompt = (
                f"Ты сгенерировал следующий {output_format} код:\n{code}\n\n"
                f"При валидации возникла ошибка: {validation_result['error']}\n\n"
                "Исправь ошибку и верни полный исправленный код."
            )

            response = self.gigachat_client.send_message_with_retry(
                system_prompt,
                correction_prompt,
            )
            parsed_response = self.response_parser.parse_etl_response(response, output_format)
            attempts += 1

            if not parsed_response["success"]:
                code = ""
                language = ""
                validation_result = {
                    "valid": False,
                    "error": "Failed to extract code from corrected LLM response",
                }
                history.append(
                    {
                        "attempt": attempts,
                        "code": "",
                        "error": validation_result["error"],
                    }
                )
                break

            code = parsed_response["code"]
            language = parsed_response["language"]
            validation_result = self._validate(output_format, code)
            history.append(
                {
                    "attempt": attempts,
                    "code": code,
                    "error": validation_result["error"],
                }
            )

        return {
            "code": code,
            "language": language,
            "valid": validation_result["valid"],
            "attempts": attempts,
            "validation_error": validation_result["error"],
            "history": history,
        }
