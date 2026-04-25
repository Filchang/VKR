import importlib

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.pool import StaticPool


SCENARIO_SQL = {
    "Выбери все заказы за последние 30 дней": """
SELECT *
FROM orders
WHERE created_at >= DATE('now', '-30 day')
""".strip(),
    "Посчитай сумму заказов по каждому пользователю": """
SELECT user_id, SUM(amount) AS total_amount
FROM orders
GROUP BY user_id
""".strip(),
    "Получи список заказов с именами пользователей": """
SELECT o.id, o.user_id, u.name, o.amount, o.created_at, o.status
FROM orders AS o
JOIN users AS u ON u.id = o.user_id
""".strip(),
    "Выбери только выполненные заказы (status = completed)": """
SELECT *
FROM orders
WHERE status = 'completed'
""".strip(),
    "Рассчитай средний чек по категориям продуктов": """
SELECT category, AVG(price) AS avg_check
FROM products
GROUP BY category
""".strip(),
    "Скопируй данные из orders в архивную таблицу orders_archive": """
INSERT INTO orders_archive (id, user_id, amount, created_at, status)
SELECT id, user_id, amount, created_at, status
FROM orders
""".strip(),
    "Обнови статус заказов старше 90 дней на expired": """
UPDATE orders
SET status = 'expired'
WHERE created_at < DATE('now', '-90 day')
""".strip(),
    "Сгруппируй заказы по месяцам, посчитай количество и сумму": """
SELECT strftime('%Y-%m', created_at) AS order_month,
       COUNT(*) AS orders_count,
       SUM(amount) AS total_amount
FROM orders
GROUP BY strftime('%Y-%m', created_at)
ORDER BY order_month
""".strip(),
    "Найди топ-10 пользователей по сумме заказов": """
SELECT user_id, SUM(amount) AS total_amount
FROM orders
GROUP BY user_id
ORDER BY total_amount DESC
LIMIT 10
""".strip(),
    "Найди пользователей, которые заказывали продукты категории Electronics": """
SELECT DISTINCT u.id, u.name, u.email, u.city
FROM users AS u
JOIN orders AS o ON o.user_id = u.id
JOIN products AS p ON p.id = o.id
WHERE p.category = 'Electronics'
""".strip(),
}


def _build_schema() -> dict:
    return {
        "orders": [
            {"column": "id", "type": "INT", "nullable": True},
            {"column": "user_id", "type": "INT", "nullable": True},
            {"column": "amount", "type": "DECIMAL", "nullable": True},
            {"column": "created_at", "type": "DATE", "nullable": True},
            {"column": "status", "type": "VARCHAR(20)", "nullable": True},
        ],
        "users": [
            {"column": "id", "type": "INT", "nullable": True},
            {"column": "name", "type": "VARCHAR(100)", "nullable": True},
            {"column": "email", "type": "VARCHAR(100)", "nullable": True},
            {"column": "city", "type": "VARCHAR(50)", "nullable": True},
        ],
        "products": [
            {"column": "id", "type": "INT", "nullable": True},
            {"column": "name", "type": "VARCHAR(100)", "nullable": True},
            {"column": "price", "type": "DECIMAL", "nullable": True},
            {"column": "category", "type": "VARCHAR(50)", "nullable": True},
        ],
    }


@pytest.fixture
def test_db():
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    connection = engine.connect()

    connection.execute(
        text(
            """
            CREATE TABLE orders (
                id INT,
                user_id INT,
                amount DECIMAL,
                created_at DATE,
                status VARCHAR(20)
            )
            """
        )
    )
    connection.execute(
        text(
            """
            CREATE TABLE users (
                id INT,
                name VARCHAR(100),
                email VARCHAR(100),
                city VARCHAR(50)
            )
            """
        )
    )
    connection.execute(
        text(
            """
            CREATE TABLE products (
                id INT,
                name VARCHAR(100),
                price DECIMAL,
                category VARCHAR(50)
            )
            """
        )
    )

    connection.execute(
        text(
            """
            INSERT INTO orders (id, user_id, amount, created_at, status) VALUES
            (1, 1, 120.50, '2026-04-10', 'completed'),
            (2, 2, 55.00, '2026-03-15', 'pending'),
            (3, 1, 220.00, '2025-12-10', 'completed')
            """
        )
    )
    connection.execute(
        text(
            """
            INSERT INTO users (id, name, email, city) VALUES
            (1, 'Alice', 'alice@example.com', 'Moscow'),
            (2, 'Bob', 'bob@example.com', 'Berlin'),
            (3, 'Carol', 'carol@example.com', 'Paris')
            """
        )
    )
    connection.execute(
        text(
            """
            INSERT INTO products (id, name, price, category) VALUES
            (1, 'Laptop', 1200.00, 'Electronics'),
            (2, 'Chair', 150.00, 'Furniture'),
            (3, 'Phone', 800.00, 'Electronics')
            """
        )
    )
    connection.commit()

    yield connection

    connection.close()
    engine.dispose()


def _run_scenario(test_db, monkeypatch, task_description: str, setup_sql: str | None = None):
    monkeypatch.setenv("GIGACHAT_API_KEY", "test-key")
    monkeypatch.setenv("DB_URL", "sqlite:///:memory:")

    if setup_sql:
        test_db.execute(text(setup_sql))
        test_db.commit()

    import generators.validator as validator_module

    monkeypatch.setattr(validator_module, "create_engine", lambda _db_url: test_db.engine)

    import generators.sql_generator as sql_generator_module

    sql_generator_module = importlib.reload(sql_generator_module)

    class FakeGigaChatClient:
        def send_message_with_retry(self, system_prompt: str, user_message: str) -> str:
            sql_code = SCENARIO_SQL[task_description]
            return f"```sql\n{sql_code}\n```"

    monkeypatch.setattr(sql_generator_module, "GigaChatClient", FakeGigaChatClient)
    monkeypatch.setattr(sql_generator_module.settings, "db_url", "sqlite:///:memory:")

    generator = sql_generator_module.SQLGenerator()
    result = generator.generate(task_description, _build_schema())

    assert result["success"] is True
    assert result["sql"].strip() != ""

    validator = validator_module.Validator("sqlite:///:memory:")
    validation_result = validator.validate_sql(result["sql"])

    assert validation_result["valid"] is True

    execution = test_db.execute(text(result["sql"]))
    if execution.returns_rows:
        execution.fetchall()


def test_simple_select_last_30_days(test_db, monkeypatch):
    _run_scenario(test_db, monkeypatch, "Выбери все заказы за последние 30 дней")


def test_aggregation_sum_by_user(test_db, monkeypatch):
    _run_scenario(test_db, monkeypatch, "Посчитай сумму заказов по каждому пользователю")


def test_join_orders_with_user_names(test_db, monkeypatch):
    _run_scenario(test_db, monkeypatch, "Получи список заказов с именами пользователей")


def test_filter_completed_orders(test_db, monkeypatch):
    _run_scenario(test_db, monkeypatch, "Выбери только выполненные заказы (status = completed)")


def test_transformation_average_check_by_category(test_db, monkeypatch):
    _run_scenario(test_db, monkeypatch, "Рассчитай средний чек по категориям продуктов")


def test_insert_into_archive_table(test_db, monkeypatch):
    _run_scenario(
        test_db,
        monkeypatch,
        "Скопируй данные из orders в архивную таблицу orders_archive",
        setup_sql="""
        CREATE TABLE orders_archive (
            id INT,
            user_id INT,
            amount DECIMAL,
            created_at DATE,
            status VARCHAR(20)
        )
        """,
    )


def test_update_old_orders_to_expired(test_db, monkeypatch):
    _run_scenario(test_db, monkeypatch, "Обнови статус заказов старше 90 дней на expired")


def test_group_orders_by_month(test_db, monkeypatch):
    _run_scenario(test_db, monkeypatch, "Сгруппируй заказы по месяцам, посчитай количество и сумму")


def test_top_10_users_by_order_sum(test_db, monkeypatch):
    _run_scenario(test_db, monkeypatch, "Найди топ-10 пользователей по сумме заказов")


def test_complex_join_users_with_electronics_orders(test_db, monkeypatch):
    _run_scenario(
        test_db,
        monkeypatch,
        "Найди пользователей, которые заказывали продукты категории Electronics",
    )
