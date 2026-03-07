from pathlib import Path

SQL_PATH = Path(__file__).resolve().parents[2] / "sql"

_cache = {}


def load_sql(layer: str, file_name: str) -> str:
    """
    Load SQL file from DW layer.
    Example:
        load_sql("raw", "insert_raw_bank_metrics.sql")
    """

    key = f"{layer}/{file_name}"

    if key in _cache:
        return _cache[key]

    sql_file = SQL_PATH / layer / file_name

    if not sql_file.exists():
        raise FileNotFoundError(f"SQL file not found: {sql_file}")

    query = sql_file.read_text(encoding="utf-8")

    _cache[key] = query

    return query
