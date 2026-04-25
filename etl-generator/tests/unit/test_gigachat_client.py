import importlib

from unittest.mock import patch

from core.response_parser import ResponseParser

import core.gigachat_client as gigachat_client_module

def _load_gigachat_client_module(monkeypatch):
    monkeypatch.setenv("GIGACHAT_API_KEY", "test-api-key")
    monkeypatch.setenv("DB_URL", "postgresql://etl:etl@localhost:5432/etldb")

    

    return importlib.reload(gigachat_client_module)


def test_send_message_success(monkeypatch):
    gigachat_client_module = _load_gigachat_client_module(monkeypatch)

    with patch("core.gigachat_client.GigaChat") as mock_gigachat:
        mock_gigachat.return_value.chat.return_value.choices = [
            type(
                "Choice",
                (),
                {
                    "message": type(
                        "Message",
                        (),
                        {"content": "SELECT 1"},
                    )()
                },
            )()
        ]

        client = gigachat_client_module.GigaChatClient()
        result = client.send_message("system", "user")

        assert result == "SELECT 1"


def test_send_message_retry_on_failure(monkeypatch):
    gigachat_client_module = _load_gigachat_client_module(monkeypatch)

    success_response = type(
        "Response",
        (),
        {
            "choices": [
                type(
                    "Choice",
                    (),
                    {
                        "message": type(
                            "Message",
                            (),
                            {"content": "SELECT 1"},
                        )()
                    },
                )()
            ]
        },
    )()

    with patch("core.gigachat_client.GigaChat") as mock_gigachat, patch(
        "core.gigachat_client.time.sleep"
    ) as mock_sleep:
        mock_gigachat.return_value.chat.side_effect = [
            Exception("temporary error 1"),
            Exception("temporary error 2"),
            success_response,
        ]

        client = gigachat_client_module.GigaChatClient()
        result = client.send_message_with_retry("system", "user", max_retries=3)

        assert result == "SELECT 1"
        assert mock_gigachat.return_value.chat.call_count == 3
        assert mock_sleep.call_count == 2


def test_response_parser_extract_sql():
    llm_response = "```sql\nSELECT * FROM orders\n```"

    result = ResponseParser().extract_code_block(llm_response, "sql")

    assert result == "SELECT * FROM orders"


def test_response_parser_no_block():
    llm_response = "Ответ без блока кода"
    parser = ResponseParser()

    code_result = parser.extract_code_block(llm_response, "sql")
    parse_result = parser.parse_etl_response(llm_response, "sql")

    assert code_result is None
    assert parse_result["success"] is False
