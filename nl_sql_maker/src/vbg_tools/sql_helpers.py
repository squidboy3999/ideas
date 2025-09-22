#!/usr/bin/env python3
# vbg_tools/sql_helpers.py
from __future__ import annotations

import datetime as _dt
from decimal import Decimal as _Decimal
import sqlite3
from typing import Any, Dict, List, Optional, Tuple, Set
from collections import Counter as _Counter
import re as _re


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
            # Prefer explicit 'name'; fallback to base part of FQN
            base = cinfo.get("name")
            if not (isinstance(base, str) and base):
                base = str(fqn).split(".", 1)[-1]
            cols.append(base)
    return sorted(set(cols))


# ---------- Small SQL helpers ----------
def _quote_ident(name: str) -> str:
    return f'"{name}"'

def _quote_fqn_col(fqn: str) -> str:
    """Quote a column FQN -> "table"."column" (or bare quoted if table missing)."""
    if "." in str(fqn):
        t, c = str(fqn).split(".", 1)
        return f'{_quote_ident(t)}.{_quote_ident(c)}'
    return _quote_ident(str(fqn))

def _looks_numeric(s: Any) -> bool:
    if isinstance(s, (int, float)):
        return True
    # allow optional sign and decimals
    return bool(_re.fullmatch(r"[+-]?\d+(\.\d+)?", str(s)))

def _sql_lit(v: Any) -> str:
    """Render a Python value as an SQL literal."""
    if _looks_numeric(v):
        return str(v)
    s = str(v).replace("'", "''")
    return f"'{s}'"

def _catalog(d: Dict[str, Any], *path: str) -> Dict[str, Any]:
    cur = d
    for k in path:
        cur = (cur or {}).get(k, {})
    return cur

def _placeholders(tmpl: str) -> List[str]:
    # e.g., {column}, {value1}, {condition}, {to_type}, {geom}, {point}
    return _re.findall(r"\{([A-Za-z0-9_]+)\}", tmpl or "")

def _base_col(fqn: str) -> str:
    return fqn.split(".", 1)[1] if "." in fqn else fqn

def _classify_action(tmpl: str) -> Tuple[str, int]:
    """
    Returns (kind, phase_index)
      kind ∈ {"projection", "clause"}
      phase_index: only meaningful for "clause"
    Uses binder-provided phase_index if present; otherwise infers from template prefix.
    """
    if not isinstance(tmpl, str) or not tmpl.strip():
        return ("projection", 0)
    t = tmpl.strip().upper()
    # Clause keywords -> default phasing
    # 10: GROUP BY, 20: HAVING, 30: ORDER BY, 40: LIMIT
    if t.startswith("GROUP BY"):
        return ("clause", 10)
    if t.startswith("HAVING"):
        return ("clause", 20)
    if t.startswith("ORDER BY"):
        return ("clause", 30)
    if t.startswith("LIMIT"):
        return ("clause", 40)
    # Otherwise treat as projection expression (COUNT(...), SUM(...), DISTINCT ..., etc.)
    return ("projection", 0)

def _select_list_from_columns(resolved_cols_fqn: List[str]) -> str:
    """Render a SELECT list from FQNs (fallback path)."""
    if not resolved_cols_fqn:
        return "*"
    return ", ".join(_quote_fqn_col(c) for c in resolved_cols_fqn)


# ---------- Constraint → SQL WHERE helpers ----------
# Canonical comparator → rendering function
def _render_comparator_sql(column_fqn: str, op: str, values: List[Any]) -> str:
    col = _quote_fqn_col(column_fqn)
    op = str(op or "").lower()

    if op in ("equal",):
        if len(values) < 1: raise ValueError("equal requires 1 value")
        return f"{col} = {_sql_lit(values[0])}"

    if op in ("not_equal",):
        if len(values) < 1: raise ValueError("not_equal requires 1 value")
        return f"{col} != {_sql_lit(values[0])}"

    if op in ("greater_than",):
        if len(values) < 1: raise ValueError("greater_than requires 1 value")
        return f"{col} > {_sql_lit(values[0])}"

    if op in ("less_than",):
        if len(values) < 1: raise ValueError("less_than requires 1 value")
        return f"{col} < {_sql_lit(values[0])}"

    if op in ("greater_than_or_equal",):
        if len(values) < 1: raise ValueError("greater_than_or_equal requires 1 value")
        return f"{col} >= {_sql_lit(values[0])}"

    if op in ("less_than_or_equal",):
        if len(values) < 1: raise ValueError("less_than_or_equal requires 1 value")
        return f"{col} <= {_sql_lit(values[0])}"

    if op in ("between",):
        if len(values) < 2: raise ValueError("between requires 2 values")
        return f"{col} BETWEEN {_sql_lit(values[0])} AND {_sql_lit(values[1])}"

    if op in ("in",):
        if len(values) < 1: raise ValueError("in requires >=1 value")
        inner = ", ".join(_sql_lit(v) for v in values)
        return f"{col} IN ({inner})"

    if op in ("like",):
        if len(values) < 1: raise ValueError("like requires 1 value")
        return f"{col} LIKE {_sql_lit(values[0])}"

    if op in ("is_null",):
        return f"{col} IS NULL"

    if op in ("is_not_null",):
        return f"{col} IS NOT NULL"

    # Unknown comparator canonical
    raise ValueError(f"Unsupported comparator op '{op}'")

def _constraints_where_sql(constraints: List[Dict[str, Any]]) -> Tuple[str, List[Any]]:
    """
    Convert constraints to a single WHERE clause string (without the 'WHERE' keyword).
    Returns (where_sql, consumed_values_in_order).
    - Honors 'negated' and optional 'join_next' ('and'/'or'); default join is AND.
    """
    if not constraints:
        return "", []

    pieces: List[str] = []
    consumed: List[Any] = []

    for i, c in enumerate(constraints):
        col = c.get("column")
        op = c.get("op")
        vals = list(c.get("values") or [])
        neg = bool(c.get("negated"))
        join = str(c.get("join_next") or "and").lower()

        # Render the atomic predicate
        atom = _render_comparator_sql(col, op, vals)
        consumed.extend(vals)

        if neg:
            atom = f"NOT ({atom})"
        pieces.append(atom)

        # Append connector (but not after the last one)
        if i < len(constraints) - 1:
            if join not in ("and", "or"):
                join = "and"
            pieces.append(join.upper())

    return " ".join(pieces).strip(), consumed

def _strip_consumed_values(all_values: List[Any], consumed: List[Any]) -> List[Any]:
    """
    Remove consumed values (multiset subtraction), preserving original order of remaining.
    """
    ctr = _Counter(consumed)
    out: List[Any] = []
    for v in all_values:
        if ctr.get(v, 0) > 0:
            ctr[v] -= 1
            continue
        out.append(v)
    return out


# ---------- Placeholder role inference ----------
_COLUMN_NAME_HINTS: Tuple[str, ...] = (
    "column", "geom", "point", "geom1", "geom2", "geom_collection"
)

def _infer_placeholder_roles(
    action_meta: Dict[str, Any],
    placeholders: List[str]
) -> Tuple[Set[str], Set[str]]:
    """
    Decide which placeholders are column-like vs value-like.

    Heuristics:
      - Any name starting with 'value' or equal to 'values' is value-like.
      - Any name starting with one of _COLUMN_NAME_HINTS is column-like.
      - If a placeholder name appears in applicable_types, prefer:
          * column-like if its name starts with a column hint,
          * otherwise leave it as "other" (must be provided via extra_args).
      - This covers PostGIS shapes: {geom}, {geom1}, {geom2}, and multi-column actions like {column1}, {column2}.
    """
    col_like: Set[str] = set()
    val_like: Set[str] = set()

    app = (action_meta or {}).get("applicable_types") or {}

    for ph in placeholders:
        lname = ph.lower()
        if lname.startswith("value") or lname == "values":
            val_like.add(ph)
            continue
        if any(lname.startswith(h) for h in _COLUMN_NAME_HINTS):
            col_like.add(ph)
            continue
        # Fall back to binder knowledge: if arg exists there and looks like a columnish key, treat as column
        if ph in app and any(ph.lower().startswith(h) for h in _COLUMN_NAME_HINTS):
            col_like.add(ph)
            continue
        # else: leave for extra_args (e.g., {to_type}, {part}, {condition})
    return col_like, val_like


# ---------- Action rendering helpers ----------
def _render_action(
    *,
    action_name: str,
    tmpl: str,
    required: List[str],
    table: str,  # not used directly now but kept for future extensions
    resolved_cols_fqn: List[str],
    values: List[Any],
    column_placeholders: Set[str],
    value_placeholders: Set[str],
    extra_args: Dict[str, Any] | None = None,
) -> str:
    """
    Substitute placeholders in the action template.
    - column placeholders (e.g., column, column1, geom, geom1) use resolved_cols_fqn (FQNs) with proper quoting
    - value placeholders use values (literals rendered)
    - other placeholders must be supplied in extra_args (rendered literally)
    Raises ValueError if a required arg cannot be satisfied.
    """
    extra_args = extra_args or {}

    def _nth(arr: List[Any], name: str) -> Any:
        m = _re.match(r"^([A-Za-z_]+)(\d+)?$", name)
        idx = (int(m.group(2)) - 1) if (m and m.group(2)) else 0
        if idx < 0 or idx >= len(arr):
            raise ValueError(f"Action '{action_name}' requires '{name}' but only {len(arr)} provided")
        return arr[idx]

    subs: Dict[str, str] = {}
    for ph in required:
        if ph in column_placeholders:
            if not resolved_cols_fqn:
                raise ValueError(f"Action '{action_name}' requires a column but none were resolved")
            col_fqn = str(_nth(resolved_cols_fqn, ph))
            subs[ph] = _quote_fqn_col(col_fqn)
        elif ph in value_placeholders:
            if not values:
                raise ValueError(f"Action '{action_name}' requires a value but none were provided")
            subs[ph] = _sql_lit(_nth(values, ph))
        else:
            if ph not in extra_args:
                raise ValueError(f"Action '{action_name}' requires '{ph}' and it was not provided")
            subs[ph] = str(extra_args[ph])

    out = tmpl
    for ph, val in subs.items():
        out = out.replace("{" + ph + "}", val)
    return out


# ---------- SQL assembly ----------
def build_select_sql_from_slots(
    slots: Dict[str, Any],
    binder_yaml: Dict[str, Any],
    limit: int = 50
) -> str:
    """
    Build a SELECT for SQLite from resolved slots + actions + constraints.

    Behavior:
      1) Determine table and columns (keep FQNs).
      2) Render constraints (slots['constraints']) to a WHERE clause and
         subtract those values from the free value pool.
      3) Partition actions into:
         - projection actions -> expressions rendered into SELECT list
         - clause actions     -> appended after FROM/WHERE in phase order
      4) For clause actions, sort by phase_index (binder-provided) or default phase order.
      5) Fall back to selecting explicit columns (FQNs), or *.
      6) Append default LIMIT if none present.
    """
    # ------- base table -------
    table = (slots or {}).get("table")
    if not table:
        raise ValueError("Cannot build SQL: no table was resolved.")

    # ------- columns (FQNs) -------
    resolved_cols_fqn: List[str] = []
    for fqn in ((slots or {}).get("columns") or []):
        # keep as FQN strings
        resolved_cols_fqn.append(str(fqn))
    if not resolved_cols_fqn:
        # fallback: all columns of table, qualify as FQNs
        for base in table_columns_from_binder(binder_yaml, table):
            resolved_cols_fqn.append(f"{table}.{base}")

    # ------- constraints -> WHERE (and value consumption) -------
    constraints = list((slots or {}).get("constraints") or [])
    where_sql, consumed_vals = _constraints_where_sql(constraints)

    # Values bank for actions (remove ones used by constraints first)
    all_values: List[Any] = list((slots or {}).get("values") or [])
    free_values: List[Any] = _strip_consumed_values(all_values, consumed_vals)

    # ------- functions catalog -------
    catalogs = _catalog(binder_yaml, "catalogs")
    fn_meta = catalogs.get("functions", {}) if isinstance(catalogs, dict) else {}

    # ------- actions: projections + clauses -------
    actions_all: List[str] = list((slots or {}).get("actions") or [])
    clause_only: List[str] = list((slots or {}).get("clause_actions") or [])

    projection_exprs: List[str] = []
    clause_pieces: List[Tuple[int, str]] = []  # (phase_index, clause_sql)

    def _render_known_action(act: str, values_bank: List[Any]) -> Optional[Tuple[str, int, str]]:
        meta = fn_meta.get(act) if isinstance(fn_meta, dict) else None
        if not isinstance(meta, dict):
            return None
        tmpl = meta.get("template")
        if not isinstance(tmpl, str) or not tmpl.strip():
            return None
        kind, default_phase = _classify_action(tmpl)
        phase_index = int(meta.get("phase_index")) if str(meta.get("phase_index") or "").isdigit() else default_phase
        req = _placeholders(tmpl)

        # Figure out which placeholders are column-like vs value-like
        col_phs, val_phs = _infer_placeholder_roles(meta, req)

        rendered = _render_action(
            action_name=act,
            tmpl=tmpl,
            required=req,
            table=table,
            resolved_cols_fqn=resolved_cols_fqn,
            values=values_bank,
            column_placeholders=col_phs,
            value_placeholders=val_phs,
            extra_args={},  # reserved for future mapping (e.g. {to_type}, {part}, {condition})
        )
        return (kind, phase_index, rendered)

    # First pass: projection-type actions from actions_all
    for act in actions_all:
        try:
            res = _render_known_action(act, free_values)
            if not res:
                continue
            kind, _phase, rendered = res
            if kind == "projection":
                projection_exprs.append(rendered)
            else:
                clause_pieces.append((_phase, rendered))
        except ValueError:
            # Skip bad projections; clauses should be well-formed or added via clause_actions
            pass

    # Second pass: explicit clause actions (LIMIT/ORDER/HAVING/GROUP)
    for act in clause_only:
        res = _render_known_action(act, free_values)
        if not res:
            continue
        kind, phase, rendered = res
        # Force clause placement even if template looked like projection
        if kind != "clause":
            phase = max(phase, 40)
        clause_pieces.append((phase, rendered))

    # ------- SELECT list -------
    if projection_exprs:
        select_list = ", ".join(projection_exprs)
    else:
        select_list = _select_list_from_columns(resolved_cols_fqn)

    sql = f'SELECT {select_list} FROM "{table}"'

    # ------- WHERE (before any other clause) -------
    if where_sql:
        sql += " WHERE " + where_sql

    # ------- Append remaining clauses in phase order -------
    if clause_pieces:
        clause_pieces.sort(key=lambda t: t[0])
        for _, clause in clause_pieces:
            if clause:
                sql += " " + clause.strip()

    # ------- Default LIMIT (only if no LIMIT clause present already) -------
    has_limit_clause = any(s for _, s in clause_pieces if s.strip().upper().startswith("LIMIT"))
    if not has_limit_clause and isinstance(limit, int) and limit > 0:
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
