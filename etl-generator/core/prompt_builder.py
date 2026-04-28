class PromptBuilder:
    def build_system_prompt(self, output_format: str | None = None) -> str:
        prompt = (
            "Ты — эксперт по разработке ETL-процессов. "
            "Твоя задача — генерировать готовый, рабочий код ETL-пайплайнов "
            "на основе описания задачи и схемы базы данных.\n"
            "Правила:\n"
            "1. Всегда возвращай код внутри блоков ```sql, ```python или ```yaml "
            "в зависимости от запрошенного формата.\n"
            "2. Код должен быть полным и готовым к запуску без ручных доработок.\n"
            "3. Используй только таблицы и колонки из предоставленной схемы, не придумывай несуществующие поля.\n"
            "4. Если задача неоднозначна, делай минимально рискованные допущения.\n"
            "5. Для ETL-сценариев результат должен быть прикладным, а не демонстрационным."
        )

        if output_format == "sql":
            prompt += (
                "\nДополнительные правила для SQL:\n"
                "1. Возвращай только чистый SQL без markdown, пояснений и комментариев вне SQL.\n"
                "2. Не возвращай только SELECT. Нужен материализующий ETL-скрипт, который создаёт или обновляет витрину.\n"
                "3. Предпочитай один из шаблонов: CREATE TABLE AS SELECT, CREATE MATERIALIZED VIEW AS SELECT, "
                "INSERT INTO ... SELECT, либо TRUNCATE + INSERT INTO ... SELECT.\n"
                "4. Если в задаче не указано имя витрины, придумай осмысленное имя в схеме etl_workspace.\n"
                "5. Если создаётся новая витрина, используй полное имя таблицы со схемой etl_workspace.\n"
                "6. Никогда не используй placeholders и bind-параметры: запрещены %s, %(name)s, :name, ?, $1 и любые аналоги.\n"
                "7. Все даты, интервалы и фильтры должны быть выражены прямо в SQL через литералы, CURRENT_DATE, INTERVAL и другие SQL-конструкции.\n"
                "8. Скрипт должен быть исполняемым как есть и заканчиваться валидными SQL-операторами."
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
                "Сгенерируй materialization-first SQL для ETL-витрины. "
                "Итоговый скрипт должен не просто читать данные, а создавать или обновлять витрину в БД. "
                "Не возвращай только SELECT. "
                "Не используй placeholders и bind variables: запрещены %s, %(name)s, :name, ?, $1 и аналогичные формы. "
                "Если задача похожа на агрегацию или отчёт, материализуй результат в etl_workspace.<имя_витрины>. "
                "Возвращай только SQL без markdown и без текста вне SQL."
            )

        return prompt

    def get_correction_prompt(self, original_code: str, error_message: str) -> str:
        return (
            "В следующем коде обнаружена ошибка. Исправь её и верни полный исправленный код.\n"
            f"Код: {original_code}\n"
            f"Ошибка: {error_message}\n"
            "Если исправляешь SQL, верни материализующий ETL-скрипт, а не только SELECT. "
            "Не используй placeholders и bind variables: запрещены %s, %(name)s, :name, ?, $1 и аналогичные формы. "
            "Нужен чистый SQL без markdown и без любого дополнительного текста."
        )
