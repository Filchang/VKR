import re
from dataclasses import dataclass, field


@dataclass
class ETLClassification:
    pattern: str
    load_strategy: str
    description: str
    target_table_hint: str | None = None
    keywords_matched: list[str] = field(default_factory=list)


class ETLClassifier:
    """Rule-based classifier that maps a task description to an ETL pattern."""

    _RULES = [
        {
            "pattern": "scd2",
            "load_strategy": "upsert",
            "description": "Slowly Changing Dimension Type 2 — версионирование исторических изменений",
            "keywords": [
                "scd", "scd2", "scd type 2", "история изменений", "историческ",
                "версионирован", "медленно меняющ", "slowly changing",
            ],
        },
        {
            "pattern": "deduplication",
            "load_strategy": "create_replace",
            "description": "Дедупликация — удаление дубликатов из набора данных",
            "keywords": [
                "дублик", "дедупликац", "dedup", "duplicate", "уникальн запис",
                "distinct", "row_number", "убрать повтор",
            ],
        },
        {
            "pattern": "incremental",
            "load_strategy": "insert_only",
            "description": "Инкрементальная загрузка — добавление только новых/изменённых записей",
            "keywords": [
                "инкрементальн", "incremental", "новые записи", "приращени",
                "delta", "дельта", "changed since", "за последн", "новых данн",
                "обновлённ", "только новые", "append",
            ],
        },
        {
            "pattern": "merge",
            "load_strategy": "upsert",
            "description": "Merge/Upsert — вставка новых и обновление существующих записей",
            "keywords": [
                "merge", "upsert", "insert or update", "вставить или обновить",
                "обновить существующ", "on conflict", "при совпадени",
            ],
        },
        {
            "pattern": "aggregation",
            "load_strategy": "truncate_insert",
            "description": "Агрегация — вычисление сводных метрик и построение витрин",
            "keywords": [
                "витрин", "агрегац", "суммарн", "статистик", "метрик",
                "отчёт", "отчет", "report", "сгруппир", "group by",
                "sum", "count", "avg", "average", "топ", "top-n",
                "ежедневн", "еженедельн", "ежемесячн", "weekly", "monthly",
                "daily", "продаж", "выручк",
            ],
        },
        {
            "pattern": "archive",
            "load_strategy": "insert_only",
            "description": "Архивация — перенос старых данных в архивные таблицы",
            "keywords": [
                "архив", "archive", "перенести старые", "устаревш",
                "старше", "older than", "перемести",
            ],
        },
        {
            "pattern": "full_refresh",
            "load_strategy": "truncate_insert",
            "description": "Полная перезагрузка — замена всего содержимого целевой таблицы",
            "keywords": [
                "полная перезагрузк", "full refresh", "пересоздать", "truncate",
                "полностью обновить", "перезалить", "overwrite",
            ],
        },
    ]

    _TARGET_PATTERNS = [
        r"(?:в таблицу|в витрину|создать таблицу|таблица результат)\s+['\"]?([a-z_][a-z0-9_.]*)['\"]?",
        r"(?:target|целевая)\s*[=:]\s*['\"]?([a-z_][a-z0-9_.]*)['\"]?",
        r"(?:into|в)\s+['\"]?(etl_workspace\.[a-z_][a-z0-9_]*)['\"]?",
        r"(?:создай|создать|create)\s+(?:таблицу|витрину)\s+([a-z_][a-z0-9_]*)",
    ]

    def classify(self, task_description: str) -> ETLClassification:
        text = task_description.lower()

        best_rule: dict | None = None
        best_score = 0
        best_keywords: list[str] = []

        for rule in self._RULES:
            matched = [kw for kw in rule["keywords"] if kw in text]
            if len(matched) > best_score:
                best_score = len(matched)
                best_rule = rule
                best_keywords = matched

        if best_rule is None:
            best_rule = {
                "pattern": "aggregation",
                "load_strategy": "truncate_insert",
                "description": "Агрегация — паттерн по умолчанию для ETL-витрин",
            }

        target_hint: str | None = None
        for pat in self._TARGET_PATTERNS:
            m = re.search(pat, text)
            if m:
                target_hint = m.group(1)
                break

        return ETLClassification(
            pattern=best_rule["pattern"],
            load_strategy=best_rule["load_strategy"],
            description=best_rule["description"],
            target_table_hint=target_hint,
            keywords_matched=best_keywords,
        )
