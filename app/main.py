from pathlib import Path
from typing import Any
from urllib.parse import quote_plus
from datetime import datetime, date
from decimal import Decimal, InvalidOperation
import os

import pyodbc
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from .db import get_connection


app = FastAPI(title="SQL Server CRUD Viewer")
templates = Jinja2Templates(directory=str(Path(__file__).resolve().parent / "templates"))


def quote_ident(name: str) -> str:
    if not name:
        raise HTTPException(status_code=400, detail="Invalid identifier")
    return f"[{name.replace(']', ']]')}]"


def list_tables() -> list[dict[str, str]]:
    query = """
    SELECT TABLE_SCHEMA, TABLE_NAME
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_TYPE = 'BASE TABLE'
    ORDER BY TABLE_SCHEMA, TABLE_NAME
    """
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(query)
        return [{"schema": row[0], "name": row[1]} for row in cur.fetchall()]


def table_columns(schema: str, table: str) -> list[dict[str, Any]]:
    query = """
    SELECT
      c.COLUMN_NAME,
      c.DATA_TYPE,
      c.IS_NULLABLE,
      COLUMNPROPERTY(
        OBJECT_ID(c.TABLE_SCHEMA + '.' + c.TABLE_NAME),
        c.COLUMN_NAME,
        'IsIdentity'
      ) AS IS_IDENTITY
    FROM INFORMATION_SCHEMA.COLUMNS AS c
    WHERE c.TABLE_SCHEMA = ? AND c.TABLE_NAME = ?
    ORDER BY c.ORDINAL_POSITION
    """
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(query, schema, table)
        rows = cur.fetchall()
    return [
        {
            "name": row[0],
            "data_type": row[1],
            "nullable": row[2] == "YES",
            "is_identity": bool(row[3]),
        }
        for row in rows
    ]


def _error_text(exc: Exception) -> str:
    if isinstance(exc, pyodbc.Error) and exc.args:
        return str(exc.args[1] if len(exc.args) > 1 else exc.args[0])
    return str(exc)


def current_connection_info() -> dict[str, str]:
    query = """
    SELECT
      CAST(@@SERVERNAME AS nvarchar(255)) AS server_name,
      CAST(DB_NAME() AS nvarchar(255)) AS database_name,
      CAST(COALESCE(CAST(SERVERPROPERTY('InstanceName') AS nvarchar(255)), 'MSSQLSERVER') AS nvarchar(255)) AS instance_name
    """
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(query)
        row = cur.fetchone()
    return {
        "server_name": row[0],
        "database_name": row[1],
        "instance_name": row[2],
        "target_server": os.getenv("DB_SERVER", "").strip(),
    }


def table_count(schema: str, table: str) -> int:
    table_sql = f"{quote_ident(schema)}.{quote_ident(table)}"
    query = f"SELECT COUNT(1) FROM {table_sql}"
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(query)
        row = cur.fetchone()
    return int(row[0] or 0)


def _parse_datetime(value: str) -> datetime:
    candidates = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M",
    ]
    for fmt in candidates:
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    raise ValueError("datetime format must be YYYY-MM-DD HH:MM[:SS]")


def _parse_date(value: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError("date format must be YYYY-MM-DD") from exc


def normalize_input_value(raw: str, data_type: str) -> Any:
    text = raw.strip()
    dt = (data_type or "").lower()

    if dt in {"int", "bigint", "smallint", "tinyint"}:
        return int(text)
    if dt in {"decimal", "numeric", "money", "smallmoney"}:
        normalized = text.replace(",", "")
        try:
            return Decimal(normalized)
        except InvalidOperation as exc:
            raise ValueError("numeric field must be a number") from exc
    if dt in {"float", "real"}:
        return float(text)
    if dt == "bit":
        lower = text.lower()
        if lower in {"1", "true", "yes", "y"}:
            return True
        if lower in {"0", "false", "no", "n"}:
            return False
        raise ValueError("bit field must be true/false or 1/0")
    if dt == "date":
        return _parse_date(text)
    if dt in {"datetime", "datetime2", "smalldatetime"}:
        return _parse_datetime(text)
    return text


def table_primary_key(schema: str, table: str) -> str | None:
    query = """
    SELECT KU.COLUMN_NAME
    FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC
    JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KU
      ON TC.CONSTRAINT_NAME = KU.CONSTRAINT_NAME
     AND TC.TABLE_SCHEMA = KU.TABLE_SCHEMA
    WHERE TC.CONSTRAINT_TYPE = 'PRIMARY KEY'
      AND TC.TABLE_SCHEMA = ?
      AND TC.TABLE_NAME = ?
    ORDER BY KU.ORDINAL_POSITION
    """
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(query, schema, table)
        rows = [r[0] for r in cur.fetchall()]
    if not rows:
        return None
    if len(rows) > 1:
        raise HTTPException(
            status_code=400,
            detail="Composite primary key is not supported yet.",
        )
    return rows[0]


def table_rows(schema: str, table: str, limit: int = 200) -> list[dict[str, Any]]:
    cols = table_columns(schema, table)
    if not cols:
        return []
    table_sql = f"{quote_ident(schema)}.{quote_ident(table)}"
    query = f"SELECT TOP ({int(limit)}) * FROM {table_sql}"
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(query)
        rows = cur.fetchall()
    col_names = [c["name"] for c in cols]
    return [dict(zip(col_names, row)) for row in rows]


@app.get("/", response_class=HTMLResponse)
def home(request: Request, schema: str | None = None, table: str | None = None):
    tables = list_tables()
    connection = current_connection_info()
    selected_schema = schema
    selected_table = table
    columns: list[dict[str, Any]] = []
    rows: list[dict[str, Any]] = []
    total_rows = 0
    pk_column: str | None = None
    error_message: str | None = request.query_params.get("error")

    if schema and table:
        try:
            columns = table_columns(schema, table)
            if not columns:
                raise HTTPException(status_code=404, detail="Table not found")
            pk_column = table_primary_key(schema, table)
            rows = table_rows(schema, table, limit=200)
            total_rows = table_count(schema, table)
        except Exception as exc:  # pragma: no cover
            error_message = str(exc)

    return templates.TemplateResponse(
        request,
        "index.html",
        {
            "tables": tables,
            "selected_schema": selected_schema,
            "selected_table": selected_table,
            "connection": connection,
            "columns": columns,
            "rows": rows,
            "total_rows": total_rows,
            "pk_column": pk_column,
            "error_message": error_message,
        },
    )


@app.post("/table/{schema}/{table}/insert")
async def insert_row(schema: str, table: str, request: Request):
    form = await request.form()
    cols = table_columns(schema, table)
    if not cols:
        raise HTTPException(status_code=404, detail="Table not found")

    values: dict[str, Any] = {}
    for col in cols:
        name = col["name"]
        if col.get("is_identity"):
            continue
        if name in form:
            raw = str(form.get(name, "")).strip()
            if raw == "":
                continue
            try:
                values[name] = normalize_input_value(raw, str(col.get("data_type", "")))
            except Exception as exc:
                return RedirectResponse(
                    url=f"/?schema={schema}&table={table}&error={quote_plus(f'Invalid value for {name}: {exc}')}",
                    status_code=303,
                )

    if not values:
        raise HTTPException(status_code=400, detail="No values to insert")

    columns_sql = ", ".join(quote_ident(k) for k in values.keys())
    placeholders = ", ".join("?" for _ in values)
    table_sql = f"{quote_ident(schema)}.{quote_ident(table)}"
    query = f"INSERT INTO {table_sql} ({columns_sql}) VALUES ({placeholders})"

    try:
        with get_connection() as conn:
            cur = conn.cursor()
            cur.execute(query, *values.values())
            conn.commit()
    except Exception as exc:
        return RedirectResponse(
            url=f"/?schema={schema}&table={table}&error={quote_plus(_error_text(exc))}",
            status_code=303,
        )

    return RedirectResponse(
        url=f"/?schema={schema}&table={table}",
        status_code=303,
    )


@app.post("/table/{schema}/{table}/update/{pk_value}")
async def update_row(schema: str, table: str, pk_value: str, request: Request):
    cols = table_columns(schema, table)
    pk_col = table_primary_key(schema, table)
    if not cols or not pk_col:
        raise HTTPException(status_code=400, detail="Table or primary key not found")

    form = await request.form()
    allowed_cols = {c["name"] for c in cols}
    set_values: dict[str, Any] = {}
    col_map = {c["name"]: c for c in cols}
    for key, value in form.items():
        if key in allowed_cols and key != pk_col:
            raw = str(value).strip()
            if raw == "":
                set_values[key] = None
                continue
            try:
                set_values[key] = normalize_input_value(raw, str(col_map[key].get("data_type", "")))
            except Exception as exc:
                return RedirectResponse(
                    url=f"/?schema={schema}&table={table}&error={quote_plus(f'Invalid value for {key}: {exc}')}",
                    status_code=303,
                )

    if not set_values:
        raise HTTPException(status_code=400, detail="No values to update")

    set_sql = ", ".join(f"{quote_ident(k)} = ?" for k in set_values.keys())
    table_sql = f"{quote_ident(schema)}.{quote_ident(table)}"
    query = (
        f"UPDATE {table_sql} "
        f"SET {set_sql} "
        f"WHERE {quote_ident(pk_col)} = ?"
    )

    try:
        with get_connection() as conn:
            cur = conn.cursor()
            cur.execute(query, *set_values.values(), pk_value)
            conn.commit()
    except Exception as exc:
        return RedirectResponse(
            url=f"/?schema={schema}&table={table}&error={quote_plus(_error_text(exc))}",
            status_code=303,
        )

    return RedirectResponse(
        url=f"/?schema={schema}&table={table}",
        status_code=303,
    )


@app.post("/table/{schema}/{table}/delete/{pk_value}")
def delete_row(schema: str, table: str, pk_value: str):
    pk_col = table_primary_key(schema, table)
    if not pk_col:
        raise HTTPException(status_code=400, detail="Primary key not found")

    table_sql = f"{quote_ident(schema)}.{quote_ident(table)}"
    query = f"DELETE FROM {table_sql} WHERE {quote_ident(pk_col)} = ?"
    try:
        with get_connection() as conn:
            cur = conn.cursor()
            cur.execute(query, pk_value)
            conn.commit()
    except Exception as exc:
        return RedirectResponse(
            url=f"/?schema={schema}&table={table}&error={quote_plus(_error_text(exc))}",
            status_code=303,
        )

    return RedirectResponse(
        url=f"/?schema={schema}&table={table}",
        status_code=303,
    )


@app.get("/health")
def health():
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.fetchone()
    return {"status": "ok"}
