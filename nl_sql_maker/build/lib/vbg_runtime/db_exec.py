# vbg_runtime/db_exec.py
from __future__ import annotations

import sqlite3
from typing import Any, List, Sequence, Tuple


def run_sqlite(db_path: str, sql: str, *, limit: int | None = None, timeout: int = 30) -> Tuple[List[str] | None, List[tuple] | None, str | None]:
    """
    Execute a (SELECT) SQL statement against a SQLite database.
    Returns (columns, rows, error). On error, (None, None, error_message).

    Safety notes:
      - This helper is intended for SELECTs that your emitter produced.
      - It does not interpolate user values. For dynamic values, use parameters.
    """
    if not isinstance(db_path, str) or not db_path:
        return None, None, "Invalid db_path"
    if not isinstance(sql, str) or not sql.strip():
        return None, None, "Invalid SQL"

    q = sql.strip().rstrip(";")
    if isinstance(limit, int) and limit > 0:
        q = f"{q} LIMIT {int(limit)}"

    try:
        conn = sqlite3.connect(db_path, timeout=timeout)
        try:
            cur = conn.cursor()
            cur.execute(q)
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description] if cur.description else []
            return cols, rows, None
        finally:
            conn.close()
    except Exception as e:
        return None, None, str(e)
