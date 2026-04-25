from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from api.database import init_db
from api.routes import router


app = FastAPI(
    title="ETL Generator API",
    description="Интеллектуальная система автоматической генерации ETL-процессов c использованием LLM",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router)


@app.on_event("startup")
def on_startup() -> None:
    init_db()


ui_dir = Path(__file__).resolve().parent.parent / "ui"
app.mount("/", StaticFiles(directory=str(ui_dir), html=True), name="ui")
