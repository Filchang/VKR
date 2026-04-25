import re

from core.config import settings
from core.gigachat_client import GigaChatClient
from core.prompt_builder import PromptBuilder
from core.response_parser import ResponseParser
from generators.validator import Validator


class PythonGenerator:
    def __init__(self):
        self.gigachat_client = GigaChatClient()
        self.prompt_builder = PromptBuilder()
        self.response_parser = ResponseParser()
        self.validator = Validator(settings.db_url)

    def _build_python_system_prompt(self) -> str:
        base_prompt = self.prompt_builder.build_system_prompt()
        python_requirements = (
            "\n\n"
            "Для Python-скриптов используй pandas и psycopg2. Скрипт должен:\n"
            "- читать данные из источника через pd.read_sql()\n"
            "- выполнять трансформации через pandas\n"
            "- записывать результат через df.to_sql() или executemany()\n"
            "- содержать блок if __name__ == '__main__'\n"
            "- логировать этапы через logging"
        )
        return f"{base_prompt}{python_requirements}"

    def generate(self, task_description: str, schema: dict) -> dict:
        system_prompt = self._build_python_system_prompt()
        user_prompt = self.prompt_builder.build_user_prompt(
            task_description,
            schema,
            output_format="python",
        )
        response = self.gigachat_client.send_message_with_retry(
            system_prompt,
            user_prompt,
        )
        parsed_response = self.response_parser.parse_etl_response(response, "python")

        return {
            "python": parsed_response["code"],
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
            result["error"] = "Failed to extract Python code from LLM response"
            return result

        validation_result = self.validator.validate_python(result["python"])
        while not validation_result["valid"] and attempts <= max_corrections:
            correction_prompt = self.prompt_builder.get_correction_prompt(
                result["python"],
                validation_result["error"],
            )
            system_prompt = self._build_python_system_prompt()
            response = self.gigachat_client.send_message_with_retry(
                system_prompt,
                correction_prompt,
            )
            parsed_response = self.response_parser.parse_etl_response(
                response,
                "python",
            )

            result = {
                "python": parsed_response["code"],
                "raw_response": parsed_response["raw_response"],
                "success": parsed_response["success"],
            }
            attempts += 1

            if not result["success"]:
                result["attempts"] = attempts
                result["error"] = "Failed to extract Python code from LLM response"
                return result

            validation_result = self.validator.validate_python(result["python"])

        result["attempts"] = attempts
        if not validation_result["valid"]:
            result["success"] = False
            result["error"] = validation_result["error"]

        return result

    def generate_with_dependencies(
        self,
        task_description: str,
        schema: dict,
        max_corrections: int = 2,
    ) -> dict:
        result = self.generate_with_correction(
            task_description,
            schema,
            max_corrections=max_corrections,
        )

        code = result.get("python", "")
        matches = re.findall(r"^import (\S+)|^from (\S+)", code, re.MULTILINE)
        imports = [direct_import or from_import for direct_import, from_import in matches]

        result["imports"] = imports
        return result
