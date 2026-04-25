# ETL Generator — Интеллектуальная система генерации ETL-процессов

## Описание
ETL Generator — это сервис, который принимает описание ETL-задачи на русском языке, анализирует доступную схему базы данных и генерирует готовые артефакты для реализации пайплайна. Система умеет создавать SQL-запросы, Python-скрипты и Airflow DAG, а затем валидировать результат и сохранять историю генерации и запусков.

В основе проекта лежит интеграция с GigaChat, FastAPI и SQLAlchemy. Поверх LLM-генерации добавлены парсинг ответов, повторные попытки с автоисправлением, валидация кода, REST API для работы с задачами и минималистичный веб-интерфейс для запуска сценариев и просмотра результата.

## Стек технологий
| Компонент | Технология | Версия |
| --- | --- | --- |
| Backend API | FastAPI | 0.136.1 |
| ASGI-сервер | Uvicorn | 0.43.0 |
| ORM / DB access | SQLAlchemy | 2.0.49 |
| PostgreSQL driver | psycopg2-binary | 2.9.11 |
| LLM SDK | gigachat | 0.2.0 |
| Config management | pydantic-settings | 2.13.1 |
| Data validation | pydantic | 2.13.3 |
| Templates | Jinja2 | 3.1.6 |
| SQL parsing | sqlparse | 0.5.5 |
| Data processing | pandas | 3.0.2 |
| Migrations | alembic | 1.18.4 |
| HTTP client | httpx | 0.28.1 |
| Tests | pytest / pytest-asyncio | 9.0.3 / 1.3.0 |
| Containers | Docker / Docker Compose | compose file 3.9 |
| Scheduler | Apache Airflow | 2.8.0 |

## Быстрый старт
1. `git clone <repo-url>`
2. `cd etl-generator`
3. `cp .env.example .env`
4. Заполнить в `.env` переменную `GIGACHAT_API_KEY` и при необходимости остальные параметры.
5. `docker-compose up -d`
6. Открыть `http://localhost:8000`

## Структура проекта
```text
etl-generator/
├── api/                 # FastAPI-приложение, роуты, ORM-модели и схемы API
│   ├── database.py      # Подключение к БД, SessionLocal, init_db
│   ├── main.py          # Точка входа FastAPI, CORS, статика, startup
│   ├── models.py        # SQLAlchemy ORM-модели задач, артефактов и логов
│   ├── routes.py        # REST API для генерации, запуска и мониторинга
│   ├── schemas.py       # Pydantic-схемы запросов и ответов
│   └── __init__.py      # Python-пакет API
├── core/                # Базовая бизнес-логика и интеграции
│   ├── airflow_deployer.py  # Деплой и запуск DAG через Airflow REST API
│   ├── config.py            # Настройки проекта через pydantic-settings
│   ├── gigachat_client.py   # Обёртка над SDK GigaChat
│   ├── prompt_builder.py    # Построение системных и пользовательских промптов
│   ├── response_parser.py   # Извлечение кода из ответов LLM
│   ├── retry_pipeline.py    # Пайплайн автоисправления и повторных попыток
│   ├── schema_inspector.py  # Интроспекция схемы БД через SQLAlchemy
│   └── __init__.py          # Python-пакет core
├── dags/                # Каталог для сгенерированных Airflow DAG
├── generators/          # Генераторы кода и валидатор
│   ├── dag_generator.py     # Генерация Airflow DAG по YAML-структуре
│   ├── dag_template.j2      # Jinja2-шаблон DAG
│   ├── python_generator.py  # Генерация Python ETL-скриптов
│   ├── sql_generator.py     # Генерация SQL ETL-сценариев
│   ├── validator.py         # Валидация SQL, Python и DAG-кода
│   └── __init__.py          # Python-пакет generators
├── tests/               # Unit- и integration-тесты
│   ├── integration/         # Интеграционные ETL-сценарии
│   ├── unit/                # Модульные тесты компонентов
│   └── __init__.py          # Python-пакет tests
├── ui/                  # Одностраничный веб-интерфейс
│   └── index.html           # Frontend без фреймворков, CodeMirror UI
├── .env.example         # Пример переменных окружения
├── .gitignore           # Исключения Git
├── docker-compose.yml   # Локальная инфраструктура: app, postgres, airflow
├── Dockerfile           # Образ приложения FastAPI
├── requirements.txt     # Python-зависимости
├── setup.cfg            # Конфигурация pytest
└── README.md            # Документация проекта
```

## API документация
| Метод | Путь | Описание | Пример запроса |
| --- | --- | --- | --- |
| `POST` | `/api/generate` | Создать задачу генерации ETL-артефакта | `{"task_description":"Посчитай сумму заказов по пользователям","output_format":"sql","source_tables":["orders","users"]}` |
| `GET` | `/api/tasks/{task_id}` | Получить статус задачи и связанные артефакты | `GET /api/tasks/1` |
| `GET` | `/api/tasks` | Получить последние 20 задач | `GET /api/tasks` |
| `POST` | `/api/run/{artifact_id}` | Запустить SQL или Airflow DAG, либо получить инструкцию для Python | `POST /api/run/5` |
| `GET` | `/api/logs/{artifact_id}` | Получить историю запусков артефакта | `GET /api/logs/5` |

OpenAPI и Swagger UI доступны после запуска приложения:
- `http://localhost:8000/docs`
- `http://localhost:8000/redoc`

## Примеры использования
```bash
curl -X POST http://localhost:8000/api/generate \
  -H "Content-Type: application/json" \
  -d '{
    "task_description": "Посчитай сумму заказов по каждому пользователю",
    "output_format": "sql",
    "source_tables": ["orders", "users"]
  }'
```

```bash
curl -X POST http://localhost:8000/api/generate \
  -H "Content-Type: application/json" \
  -d '{
    "task_description": "Сгенерируй Python ETL-скрипт для загрузки заказов в витрину",
    "output_format": "python",
    "source_tables": ["orders", "users"]
  }'
```

```bash
curl -X POST http://localhost:8000/api/generate \
  -H "Content-Type: application/json" \
  -d '{
    "task_description": "Сгенерируй Airflow DAG для ежедневной загрузки заказов",
    "output_format": "airflow_dag",
    "source_tables": ["orders", "users", "products"]
  }'
```

## Запуск тестов
```bash
docker-compose exec app pytest tests/ -v
```

## Конфигурация
| Переменная | Описание | Пример значения |
| --- | --- | --- |
| `GIGACHAT_API_KEY` | API-ключ для доступа к GigaChat | `eyJhbGciOi...` |
| `GIGACHAT_SCOPE` | Scope для авторизации в GigaChat | `GIGACHAT_API_PERS` |
| `DB_URL` | Строка подключения к рабочей БД приложения | `postgresql://etl:etl@postgres:5432/etldb` |
| `AIRFLOW_URL` | Базовый URL Airflow REST API | `http://localhost:8080` |
| `AIRFLOW_USER` | Пользователь Airflow | `airflow` |
| `AIRFLOW_PASSWORD` | Пароль Airflow | `airflow` |

---

Проект можно расширять новыми генераторами, схемами выполнения и стратегиями валидации без изменения базового API-контракта.
