import pytest

from core.etl_classifier import ETLClassification, ETLClassifier


@pytest.fixture
def classifier():
    return ETLClassifier()


def test_classifies_aggregation_from_keywords(classifier):
    result = classifier.classify("Построй ежедневную витрину продаж с агрегацией по регионам")
    assert result.pattern == "aggregation"
    assert result.load_strategy == "truncate_insert"


def test_classifies_incremental_load(classifier):
    result = classifier.classify(
        "Загрузи инкрементально новые записи заказов за последние сутки"
    )
    assert result.pattern == "incremental"
    assert result.load_strategy == "insert_only"


def test_classifies_scd2(classifier):
    result = classifier.classify(
        "Реализуй SCD2 для таблицы клиентов — храни историю изменений адреса"
    )
    assert result.pattern == "scd2"
    assert result.load_strategy == "upsert"


def test_classifies_deduplication(classifier):
    result = classifier.classify("Удали дубликаты из таблицы пользователей, оставь уникальные записи")
    assert result.pattern == "deduplication"


def test_classifies_merge_upsert(classifier):
    result = classifier.classify(
        "Merge данные: вставить новые строки или обновить существующие по ключу"
    )
    assert result.pattern == "merge"
    assert result.load_strategy == "upsert"


def test_classifies_archive(classifier):
    result = classifier.classify("Перенеси старые заказы в архив orders_archive")
    assert result.pattern == "archive"


def test_classifies_full_refresh(classifier):
    result = classifier.classify("Полная перезагрузка справочника продуктов из источника")
    assert result.pattern == "full_refresh"


def test_default_fallback_to_aggregation(classifier):
    result = classifier.classify("Сделай что-нибудь с данными")
    assert result.pattern == "aggregation"


def test_returns_etl_classification_dataclass(classifier):
    result = classifier.classify("Витрина заказов")
    assert isinstance(result, ETLClassification)
    assert result.pattern
    assert result.load_strategy
    assert result.description
    assert isinstance(result.keywords_matched, list)


def test_no_target_table_hint_by_default(classifier):
    result = classifier.classify("Агрегируй заказы по дням")
    assert result.target_table_hint is None


def test_extract_target_table_hint(classifier):
    result = classifier.classify("Создай таблицу mart_daily_sales из заказов за сегодня")
    assert result.target_table_hint == "mart_daily_sales"


def test_keywords_matched_populated(classifier):
    result = classifier.classify("Построй витрину с агрегацией и суммарными метриками")
    assert len(result.keywords_matched) > 0
