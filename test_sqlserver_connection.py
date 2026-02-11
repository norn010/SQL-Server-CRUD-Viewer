import os
from dotenv import load_dotenv
import pyodbc


def get_required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def build_connection_string() -> str:
    auth_mode = os.getenv("DB_AUTH_MODE", "trusted").strip().lower()
    server = get_required_env("DB_SERVER")
    database = get_required_env("DB_DATABASE")
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
        username = get_required_env("DB_USERNAME")
        password = get_required_env("DB_PASSWORD")
        parts.append(f"UID={username}")
        parts.append(f"PWD={password}")
    else:
        raise ValueError("DB_AUTH_MODE must be 'trusted' or 'sql_auth'")

    return ";".join(parts)


def main() -> None:
    load_dotenv()

    schema = os.getenv("DB_SAMPLE_SCHEMA", "dbo").strip()
    table = os.getenv("DB_SAMPLE_TABLE", "sales_records").strip()
    top_n = int(os.getenv("DB_TOP_N", "5"))
    conn_str = build_connection_string()

    print("Connecting to SQL Server...")
    with pyodbc.connect(conn_str, timeout=10) as conn:
        cursor = conn.cursor()
        query = (
            f"SELECT TOP ({top_n}) * "
            f"FROM [{schema}].[{table}] "
            "ORDER BY 1"
        )
        cursor.execute(query)
        columns = [col[0] for col in cursor.description]
        rows = cursor.fetchall()

    print("Connection successful.")
    print(f"Read success from [{schema}].[{table}]")
    print(f"Columns: {columns}")
    print(f"Row count fetched: {len(rows)}")

    for index, row in enumerate(rows, start=1):
        print(f"{index}. {tuple(row)}")


if __name__ == "__main__":
    main()
