import re


class ResponseParser:
    def extract_code_block(self, llm_response: str, language: str) -> str | None:
        pattern = rf"```{re.escape(language)}\s*(.*?)```"
        match = re.search(pattern, llm_response, re.DOTALL)
        if match:
            return match.group(1).strip()
        return None

    def extract_any_code_block(self, llm_response: str) -> tuple[str, str] | None:
        priority_languages = ["sql", "python", "yaml"]

        for language in priority_languages:
            code = self.extract_code_block(llm_response, language)
            if code is not None:
                return language, code

        match = re.search(r"```([a-zA-Z0-9_+-]+)\s*(.*?)```", llm_response, re.DOTALL)
        if match:
            return match.group(1).strip(), match.group(2).strip()

        return None

    def parse_etl_response(self, llm_response: str, expected_format: str) -> dict:
        code = self.extract_code_block(llm_response, expected_format)
        language = expected_format

        if code is not None:
            return {
                "code": code,
                "language": language,
                "raw_response": llm_response,
                "success": True,
            }

        any_block = self.extract_any_code_block(llm_response)
        if any_block is not None:
            detected_language, detected_code = any_block
            return {
                "code": detected_code,
                "language": detected_language,
                "raw_response": llm_response,
                "success": True,
            }

        return {
            "code": "",
            "language": "",
            "raw_response": llm_response,
            "success": False,
        }
