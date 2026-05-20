"""
CSV ingestion pipeline: parse → clean → normalise → load into etl_workspace.
"""
from __future__ import annotations

import io
import re
import unicodedata
from dataclasses import dataclass, field

import pandas as pd
from sqlalchemy import create_engine, text


# ── helpers ──────────────────────────────────────────────────────────────────

_ENCODINGS_TO_TRY = ("utf-8-sig", "utf-8", "cp1251", "latin-1")

_DATE_COLUMN_HINTS = re.compile(
    r"(date|дата|time|время|dt|created|updated|at|on|period|timestamp)",
    re.IGNORECASE,
)

_SAFE_NAME = re.compile(r"[^a-z0-9_]")


def _to_snake(name: str) -> str:
    """Convert an arbitrary string to a safe snake_case identifier."""
    # Transliterate Cyrillic and other Unicode to ASCII where possible
    name = unicodedata.normalize("NFKD", name)
    name = name.encode("ascii", "ignore").decode("ascii")
    name = name.lower().strip()
    name = re.sub(r"[\s\-/]+", "_", name)
    name = _SAFE_NAME.sub("", name)
    name = re.sub(r"_+", "_", name).strip("_")
    return name or "col"


def _make_unique_names(names: list[str]) -> list[str]:
    seen: dict[str, int] = {}
    result: list[str] = []
    for n in names:
        if n in seen:
            seen[n] += 1
            result.append(f"{n}_{seen[n]}")
        else:
            seen[n] = 0
            result.append(n)
    return result


def _read_csv(data: bytes) -> pd.DataFrame:
    """Try multiple encodings; return the first that parses cleanly."""
    for enc in _ENCODINGS_TO_TRY:
        try:
            df = pd.read_csv(io.BytesIO(data), encoding=enc, dtype=str, sep=None, engine="python")
            return df
        except Exception:
            continue
    raise ValueError(
        "Не удалось прочитать CSV-файл. "
        "Поддерживаемые кодировки: UTF-8, UTF-8-BOM, CP1251, Latin-1."
    )


def _clean_dataframe(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    warnings: list[str] = []

    # ── 1. drop fully empty rows/columns ────────────────────────────────────
    df = df.dropna(how="all")
    before_cols = len(df.columns)
    df = df.dropna(axis=1, how="all")
    if len(df.columns) < before_cols:
        warnings.append(f"Удалено {before_cols - len(df.columns)} полностью пустых столбца(-ов).")

    # ── 2. normalise column names to snake_case ──────────────────────────────
    df.columns = _make_unique_names([_to_snake(str(c)) for c in df.columns])

    # ── 3. strip whitespace in string cells ─────────────────────────────────
    for col in df.columns:
        df[col] = df[col].apply(lambda v: v.strip() if isinstance(v, str) else v)

    # ── 4. replace empty strings with NaN ───────────────────────────────────
    df = df.replace({"": pd.NA})

    # ── 5. try to infer numeric columns ─────────────────────────────────────
    for col in df.columns:
        if _DATE_COLUMN_HINTS.search(col):
            try:
                df[col] = pd.to_datetime(df[col], infer_datetime_format=True, errors="coerce")
                continue
            except Exception:
                pass
        try:
            df[col] = pd.to_numeric(df[col], errors="raise")
        except (ValueError, TypeError):
            pass  # keep as string

    # ── 6. drop duplicate rows ───────────────────────────────────────────────
    n_before = len(df)
    df = df.drop_duplicates()
    n_removed = n_before - len(df)
    if n_removed:
        warnings.append(f"Удалено {n_removed} дублирующихся строк.")

    return df, warnings


def _pandas_dtype_to_sql(dtype) -> str:
    dtype_str = str(dtype)
    if "int" in dtype_str:
        return "BIGINT"
    if "float" in dtype_str:
        return "DOUBLE PRECISION"
    if "datetime" in dtype_str:
        return "TIMESTAMP"
    if "bool" in dtype_str:
        return "BOOLEAN"
    return "TEXT"


# ── public API ────────────────────────────────────────────────────────────────

@dataclass
class CSVImportResult:
    table_name: str
    schema_name: str
    rows_loaded: int
    columns: list[dict]  # [{name, sql_type, nullable}]
    warnings: list[str] = field(default_factory=list)
    replaced_existing: bool = False


class CSVProcessor:
    def __init__(self, db_url: str, work_schema: str = "public"):
        self.db_url = db_url
        self.work_schema = work_schema
        self.engine = create_engine(db_url)

    def _qualified(self, table_name: str) -> str:
        is_sqlite = self.engine.dialect.name == "sqlite"
        if is_sqlite or not self.work_schema or self.work_schema == "public":
            return table_name
        return f"{self.work_schema}.{table_name}"

    def process_and_load(
        self,
        file_data: bytes,
        desired_table_name: str | None = None,
        original_filename: str = "upload.csv",
        replace_if_exists: bool = True,
    ) -> CSVImportResult:
        df_raw = _read_csv(file_data)
        df, warnings = _clean_dataframe(df_raw)

        if df.empty:
            raise ValueError("CSV-файл не содержит данных после очистки.")

        # derive table name
        if desired_table_name:
            table_name = _to_snake(desired_table_name)
        else:
            stem = re.sub(r"\.csv$", "", original_filename, flags=re.IGNORECASE)
            table_name = _to_snake(stem) or "csv_import"

        qualified = self._qualified(table_name)
        replaced = False

        with self.engine.begin() as conn:
            # check existence
            if replace_if_exists:
                if self.engine.dialect.name == "sqlite":
                    conn.execute(text(f"DROP TABLE IF EXISTS {qualified}"))
                else:
                    conn.execute(text(f"DROP TABLE IF EXISTS {qualified}"))
                replaced = True

        # write via pandas (handles DDL automatically)
        is_sqlite = self.engine.dialect.name == "sqlite"
        schema_arg = None if is_sqlite or self.work_schema == "public" else self.work_schema
        df.to_sql(
            table_name,
            con=self.engine,
            schema=schema_arg,
            if_exists="replace",
            index=False,
            method="multi",
            chunksize=500,
        )

        columns = [
            {
                "name": col,
                "sql_type": _pandas_dtype_to_sql(df[col].dtype),
                "nullable": bool(df[col].isna().any()),
            }
            for col in df.columns
        ]

        return CSVImportResult(
            table_name=table_name,
            schema_name=self.work_schema,
            rows_loaded=len(df),
            columns=columns,
            warnings=warnings,
            replaced_existing=replaced,
        )

    def list_csv_tables(self) -> list[dict]:
        """Return all tables in the work schema with basic metadata."""
        from sqlalchemy import inspect as sa_inspect

        inspector = sa_inspect(self.engine)
        schema = None if self.engine.dialect.name == "sqlite" else self.work_schema
        tables = inspector.get_table_names(schema=schema)
        result = []
        for tbl in tables:
            cols = inspector.get_columns(tbl, schema=schema)
            result.append({
                "table_name": tbl,
                "column_count": len(cols),
                "columns": [
                    {"name": c["name"], "type": str(c["type"])}
                    for c in cols
                ],
            })
        return result
