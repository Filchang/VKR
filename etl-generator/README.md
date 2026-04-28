# ETL Generator

Сервис генерирует SQL, Python ETL-скрипты и Airflow DAG по текстовому описанию задачи. В основе лежат FastAPI, SQLAlchemy и GigaChat, поверх которых добавлены валидация кода, хранение истории генераций и запуск артефактов.

## Что изменено

- Служебные таблицы приложения вынесены в отдельную схему БД.
- Рабочие таблицы и исполняемый SQL используют отдельную рабочую схему.
- SQL-валидатор теперь поддерживает многооператорные скрипты и DDL/DML, а не только `SELECT`.
- Генерация Airflow DAG нормализует идентификаторы, рендерит валидный Python и ждёт регистрации DAG в Airflow перед запуском.
- Ошибки генерации и запуска стали информативнее в API и UI.

## Структура БД

По умолчанию используются две схемы:

- `service` для таблиц приложения: `etl_tasks`, `generated_artifacts`, `execution_logs`
- `public` для рабочих таблиц ETL

Это поведение настраивается через переменные окружения:

- `APP_DB_SCHEMA` или старое имя `SERVICE_SCHEMA`
- `WORK_DB_SCHEMA` или старое имя `WORKSPACE_SCHEMA`

При старте приложения нужные схемы создаются автоматически, если СУБД это поддерживает.

## Основные переменные окружения

- `GIGACHAT_API_KEY` — ключ доступа к GigaChat
- `GIGACHAT_SCOPE` — scope для GigaChat
- `DB_URL` — строка подключения к основной БД
- `APP_DB_SCHEMA` — схема служебных таблиц, по умолчанию `service`
- `WORK_DB_SCHEMA` — схема рабочих таблиц, по умолчанию `public`
- `AIRFLOW_URL` — URL Airflow API
- `AIRFLOW_USER` — пользователь Airflow
- `AIRFLOW_PASSWORD` — пароль Airflow
- `DAGS_FOLDER` — каталог, куда приложение пишет сгенерированные DAG

## Запуск

```bash
docker-compose up -d
```

После старта:

- UI: `http://localhost:8000`
- Swagger: `http://localhost:8000/docs`
- ReDoc: `http://localhost:8000/redoc`
- Airflow: `http://localhost:8080`

## Как работает SQL-валидация

- Скрипт разбивается на отдельные SQL-операторы.
- `SELECT`, `INSERT`, `UPDATE`, `DELETE` проверяются через `EXPLAIN`.
- Остальные операторы, включая `CREATE TABLE`, `ALTER`, `TRUNCATE`, выполняются в транзакции с последующим `ROLLBACK`.
- Перед валидацией и реальным запуском для PostgreSQL выставляется `search_path` с приоритетом рабочей схемы.

Это позволяет пропускать типовые ETL-скрипты, которые создают временные или итоговые таблицы.

## Airflow DAG

Сгенерированный DAG:

- получает безопасные Python-идентификаторы для функций и task variables
- использует `PythonOperator`
- рендерится через Jinja-шаблон в валидный Python-код
- после деплоя ожидает появления DAG в Airflow API и снимает паузу перед запуском

## Тесты

```bash
pytest tests/unit/test_validator_and_dag.py tests/integration/test_etl_scenarios.py -q
```

Покрыты:

- SQL-сценарии генерации и валидации
- DAG-рендеринг и нормализация идентификаторов
- поддержка DDL/DML в SQL-валидаторе

## Структура проекта

```text
etl-generator/
├── api/            # FastAPI, ORM-модели, роуты
├── core/           # конфиг, интеграции, инспекция схемы, деплой DAG
├── generators/     # генераторы SQL/Python/DAG и валидатор
├── tests/          # unit и integration тесты
├── ui/             # статический web UI
├── Dockerfile
├── docker-compose.yml
└── README.md
```
