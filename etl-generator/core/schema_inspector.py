from sqlalchemy import create_engine, inspect


class SchemaInspector:
    def __init__(self, db_url: str, schema: str | None = None):
        self.engine = create_engine(db_url)
        self.inspector = inspect(self.engine)
        self.schema = None if self.engine.dialect.name == "sqlite" else schema

    def get_all_tables(self) -> list[str]:
        return self.inspector.get_table_names(schema=self.schema)

    def get_table_schema(self, table_name: str) -> list[dict]:
        schema_name = self.schema
        resolved_table = table_name
        if "." in table_name:
            schema_name, resolved_table = table_name.split(".", 1)

        columns = self.inspector.get_columns(resolved_table, schema=schema_name)
        return [
            {
                "column": col["name"],
                "type": str(col["type"]),
                "nullable": col["nullable"],
            }
            for col in columns
        ]

    def get_full_schema(self, tables: list[str] | None = None) -> dict:
        target_tables = tables if tables is not None else self.get_all_tables()
        return {
            table_name: self.get_table_schema(table_name)
            for table_name in target_tables
        }

    def get_foreign_keys(self, table_name: str) -> list[dict]:
        schema_name = self.schema
        resolved_table = table_name
        if "." in table_name:
            schema_name, resolved_table = table_name.split(".", 1)

        foreign_keys = self.inspector.get_foreign_keys(
            resolved_table,
            schema=schema_name,
        )
        return [
            {
                "referred_table": fk["referred_table"],
                "constrained_columns": fk["constrained_columns"],
                "referred_columns": fk["referred_columns"],
            }
            for fk in foreign_keys
        ]

    def format_schema_for_prompt(self, schema: dict) -> str:
        lines = []

        for table_name, columns in schema.items():
            formatted_columns = []
            for column in columns:
                nullable = "NULL" if column["nullable"] else "NOT NULL"
                formatted_columns.append(
                    f"{column['column']} ({column['type']}, {nullable})"
                )
            lines.append(f"Таблица {table_name}: {', '.join(formatted_columns)}")

        return "\n".join(lines)
