#!/usr/bin/env python3
# vbg_tools/sql_helpers.py
from __future__ import annotations

import datetime as _dt
from decimal import Decimal as _Decimal
import sqlite3
from typing import Any, Dict, List, Optional, Tuple


# ---------- JSON-safe coercion ----------
def json_safe(v: Any) -> Any:
    """Coerce SQLite/SpatiaLite values to JSON-safe types."""
    if isinstance(v, (bytes, bytearray, memoryview)):
        return "0x" + bytes(v).hex()
    if isinstance(v, (_dt.datetime, _dt.date, _dt.time)):
        return v.isoformat()
    if isinstance(v, _Decimal):
        try:
            return float(v)
        except Exception:
            return str(v)
    if isinstance(v, list):
        return [json_safe(x) for x in v]
    if isinstance(v, tuple):
        return [json_safe(x) for x in v]
    if isinstance(v, dict):
        return {k: json_safe(val) for k, val in v.items()}
    return v


# ---------- Binder helpers ----------
def table_columns_from_binder(binder_yaml: Dict[str, Any], table: str) -> List[str]:
    """Return base column names for a given table from the binder."""
    catalogs = (binder_yaml.get("catalogs") or {})
    columns = catalogs.get("columns") or {}
    cols: List[str] = []
    for fqn, cinfo in columns.items():
        if isinstance(cinfo, dict) and cinfo.get("table") == table:
            base = cinfo.get("name")
            if isinstance(base, str) and base:
                cols.append(base)
    return sorted(set(cols))


# ---------- SQL assembly ----------
def build_select_sql_from_slots(
    slots: Dict[str, Any],
    binder_yaml: Dict[str, Any],
    limit: int = 50
) -> str:
    """
    Build a simple SELECT for SQLite from resolved slots:
    - SELECT <explicit columns if provided, else all table columns, else *>
    - FROM "<table>"
    - LIMIT <limit>
    """
    table = (slots or {}).get("table")
    if not table:
        raise ValueError("Cannot build SQL: no table was resolved.")

    # Extract base column names from FQNs if present
    resolved_cols: List[str] = []
    for fqn in (slots or {}).get("columns", []) or []:
        base = fqn.split(".", 1)[1] if "." in fqn else fqn
        if base:
            resolved_cols.append(base)

    if not resolved_cols:
        resolved_cols = table_columns_from_binder(binder_yaml, table)

    if not resolved_cols:
        col_sql = "*"
    else:
        col_sql = ", ".join(f'"{c}"' for c in resolved_cols)

    sql = f'SELECT {col_sql} FROM "{table}"'
    if isinstance(limit, int) and limit > 0:
        sql += f" LIMIT {limit}"
    return sql


# ---------- SQL execution ----------
def execute_sqlite(
    db_path: str,
    sql: str,
    max_rows: Optional[int] = None
) -> Dict[str, Any]:
    """
    Execute SQL against SQLite and return a JSON-safe result dict:
      { "columns": [...], "rows": [ {col:val,...}, ... ], "rowcount": N }
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.execute(sql)
        colnames = [d[0] for d in (cur.description or [])]
        if max_rows is None:
            rows_raw = cur.fetchall()
        else:
            rows_raw = cur.fetchmany(max_rows)
        rows = [{k: json_safe(r[k]) for k in colnames} for r in rows_raw]
        return {"columns": colnames, "rows": rows, "rowcount": len(rows)}
    finally:
        try:
            conn.close()
        except Exception:
            pass
