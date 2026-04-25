import logging
import time

from gigachat import GigaChat
from gigachat.exceptions import GigaChatException as GigaChatError
from gigachat.models import Chat, Messages, MessagesRole

from core.config import settings


logger = logging.getLogger(__name__)


class SystemMessage(Messages):
    def __init__(self, content: str) -> None:
        super().__init__(role=MessagesRole.SYSTEM, content=content)


class HumanMessage(Messages):
    def __init__(self, content: str) -> None:
        super().__init__(role=MessagesRole.USER, content=content)


class GigaChatClient:
    def __init__(self) -> None:
        self.client = GigaChat(
            credentials=settings.gigachat_api_key,
            scope=settings.gigachat_scope,
            verify_ssl_certs=False,
            model='Gigachat-2'
        )

    def send_message(self, system_prompt: str, user_message: str) -> str:
        messages = [
            SystemMessage(content=system_prompt),
            HumanMessage(content=user_message),
        ]

        logger.info("Sending request to GigaChat")

        try:
            response = self.client.chat(Chat(messages=messages))
            return response.choices[0].message.content
        except Exception as exc:
            logger.error("Failed to send request to GigaChat: %s", exc)
            raise GigaChatError(str(exc)) from exc

    def send_message_with_retry(
        self,
        system_prompt: str,
        user_message: str,
        max_retries: int = 3,
    ) -> str:
        last_error = None

        for attempt in range(max_retries):
            try:
                return self.send_message(system_prompt, user_message)
            except Exception as exc:
                last_error = exc
                logger.error(
                    "GigaChat request failed on attempt %s/%s: %s",
                    attempt + 1,
                    max_retries,
                    exc,
                )
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)

        raise last_error or RuntimeError("All retries failed")
