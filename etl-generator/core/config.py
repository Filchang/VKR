from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    gigachat_api_key: str
    gigachat_scope: str = "GIGACHAT_API_PERS"
    db_url: str
    app_db_schema: str = Field(
        default="service",
        validation_alias=AliasChoices("APP_DB_SCHEMA", "SERVICE_SCHEMA", "service_schema"),
    )
    work_db_schema: str = Field(
        default="public",
        validation_alias=AliasChoices("WORK_DB_SCHEMA", "WORKSPACE_SCHEMA", "workspace_schema"),
    )
    airflow_url: str = "http://localhost:8080"
    airflow_user: str = "airflow"
    airflow_password: str = "airflow"
    dags_folder: str = "./dags"

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


settings = Settings()  # pyright: ignore[reportCallIssue]
