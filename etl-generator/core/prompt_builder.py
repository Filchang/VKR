class PromptBuilder:
    def build_system_prompt(self, output_format: str | None = None) -> str:
        prompt = (
            "Ты — эксперт по разработке ETL-процессов. Твоя задача — "
            "генерировать готовый, рабочий код ETL-пайплайнов на основе "
            "описания задачи и схемы базы данных.\n"
            "Правила:\n"
            "1. Всегда возвращай код внутри блоков ```sql, ```python или ```yaml "
            "в зависимости от запрошенного формата.\n"
            "2. Код должен быть полным и готовым к запуску без изменений.\n"
            "3. Используй только таблицы и колонки из предоставленной схемы — "
            "не придумывай несуществующие.\n"
            "4. Добавляй комментарии к ключевым шагам.\n"
            "5. При трансформации данных всегда обрабатывай NULL-значения.\n"
            "6. Если задача неоднозначна — укажи допущения в комментарии в начале кода."
        )

        if output_format == "sql":
            prompt += (
                "\n"
                "Дополнительные правила для SQL:\n"
                "1. Строго возвращай только SQL-запрос.\n"
                "2. Не добавляй пояснения, markdown, комментарии, заголовки и любой текст вне SQL.\n"
                "3. Верни только один завершенный SQL-запрос, который можно выполнить как есть.\n"
                "4. Не используй блоки кода с ``` и не оборачивай ответ в markdown."
            )

        return prompt

    def build_user_prompt(
        self,
        task_description: str,
        schema: dict,
        output_format: str,
    ) -> str:
        schema_lines = []

        for table_name, columns in schema.items():
            schema_lines.append(f"Таблица: {table_name}")
            for column in columns:
                nullable = "NULL" if column.get("nullable", False) else "NOT NULL"
                schema_lines.append(
                    f"- {column.get('column')}: {column.get('type')} ({nullable})"
                )

        schema_text = "\n".join(schema_lines) if schema_lines else "Схема не предоставлена."

        prompt = (
            f"Описание задачи:\n{task_description}\n\n"
            f"Схема базы данных:\n{schema_text}\n\n"
            f"Требуемый формат вывода:\n{output_format}"
        )

        if output_format == "sql":
            prompt += (
                "\n\n"
                "Сгенерируй только чистый SQL-запрос. "
                "Не добавляй пояснения, markdown, кодовые блоки и текст вне SQL."
            )

        return prompt

    def get_correction_prompt(self, original_code: str, error_message: str) -> str:
        return (
            "В следующем коде обнаружена ошибка. Исправь её и верни полный "
            "исправленный код.\n"
            f"Код: {original_code}\n"
            f"Ошибка: {error_message}\n"
            "Если исправляешь SQL, верни только чистый SQL-запрос без пояснений, "
            "без markdown и без любого дополнительного текста."
        )
