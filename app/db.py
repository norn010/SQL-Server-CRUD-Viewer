import os
from pathlib import Path

import pyodbc
from dotenv import load_dotenv


BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")


def _require_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def _normalize_server(server: str) -> str:
    # Accept values copied from .env with escaped backslashes.
    return server.replace("\\\\", "\\").strip()


def build_connection_string() -> str:
    auth_mode = os.getenv("DB_AUTH_MODE", "trusted").strip().lower()
    server = _normalize_server(_require_env("DB_SERVER"))
    database = _require_env("DB_DATABASE")
    driver = os.getenv("DB_DRIVER", "ODBC Driver 17 for SQL Server").strip()

    parts = [
        f"DRIVER={{{driver}}}",
        f"SERVER={server}",
        f"DATABASE={database}",
        "TrustServerCertificate=yes",
    ]

    if auth_mode == "trusted":
        parts.append("Trusted_Connection=yes")
    elif auth_mode == "sql_auth":
        username = _require_env("DB_USERNAME")
        password = _require_env("DB_PASSWORD")
        parts.append(f"UID={username}")
        parts.append(f"PWD={password}")
    else:
        raise ValueError("DB_AUTH_MODE must be 'trusted' or 'sql_auth'")

    return ";".join(parts)


def get_connection() -> pyodbc.Connection:
    return pyodbc.connect(build_connection_string(), timeout=10)
