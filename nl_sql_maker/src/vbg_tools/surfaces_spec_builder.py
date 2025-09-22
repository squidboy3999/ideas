from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, Any, Iterable, List, Tuple, Set
import yaml

# =========================
# Data model
# =========================

@dataclass(frozen=True)
class SQLSpec:
    func: str
    arg_key: str           # usually "column"
    table: str
    column: str            # fully-qualified, e.g. "users.age"
    expression_sql: str    # baseline SELECT … FROM … (no predicates)

# =========================
# Type mapping & normalization
# =========================

_EXPECTED_SLOT_TYPES = {
    "numeric", "date", "text", "boolean", "geometry", "geography",
    "geometry_point", "geography_point", "geometry_linestring",
    "geography_linestring", "geometry_polygon", "geography_polygon",
    "id"
}

_NUMERIC_HINTS = {
    "int", "integer", "bigint", "smallint", "decimal", "numeric", "float", "double", "real"
}
_DATE_HINTS = {"date", "timestamp", "timestamptz", "time", "datetime"}


def dbtype_to_slot_types(db_type: str) -> Set[str]:
    """
    Map a DB 'type' string — even if it's a stringified dict — to abstract slot types.
    If the string parses as a dict with a 'type' key, use that inner 'type'.
    """
    s = db_type or ""
    if isinstance(s, str) and ("{" in s and "}" in s):
        try:
            parsed = yaml.safe_load(s)
            if isinstance(parsed, dict) and "type" in parsed:
                s = str(parsed.get("type") or "")
        except Exception:
            # keep original s
            pass
    low = s.lower()
    if any(h in low for h in _NUMERIC_HINTS):
        return {"numeric"}
    if any(h in low for h in _DATE_HINTS):
        return {"date"}
    if "bool" in low:
        return {"boolean"}
    if "geom" in low or "geog" in low:
        return {"geometry"}
    if low == "id" or low.endswith("_id"):
        return {"id"}
    return {"text"}


def _normalize_slot_types(st_raw: Any, db_type: str) -> Set[str]:
    """
    Normalize a 'slot_types' field that might be:
      - a valid string ("numeric"), or list of strings,
      - a stringified yaml/json dict or list,
      - or missing/garbage (fallback to db type).
    """
    if st_raw is None:
        return dbtype_to_slot_types(db_type)

    # Single string?
    if isinstance(st_raw, str):
        s = st_raw.strip()
        low = s.lower()
        if low in _EXPECTED_SLOT_TYPES:
            return {low}
        # try to parse as yaml/json
        try:
            parsed = yaml.safe_load(s)
        except Exception:
            parsed = None
        if isinstance(parsed, dict):
            inner_t = str(parsed.get("type") or "")
            types = dbtype_to_slot_types(inner_t)
            return types if types else dbtype_to_slot_types(db_type)
        if isinstance(parsed, list):
            cleaned = {str(x).lower() for x in parsed if isinstance(x, str)}
            cleaned &= _EXPECTED_SLOT_TYPES
            return cleaned if cleaned else dbtype_to_slot_types(db_type)
        return dbtype_to_slot_types(db_type)

    # List?
    if isinstance(st_raw, list):
        cleaned = {str(x).lower() for x in st_raw if isinstance(x, str)}
        cleaned &= _EXPECTED_SLOT_TYPES
        return cleaned if cleaned else dbtype_to_slot_types(db_type)

    # Fallback
    return dbtype_to_slot_types(db_type)


def column_slot_types(binder: Dict[str, Any], fqcol: str) -> Set[str]:
    """
    Return normalized abstract slot types for a fully-qualified column name.
    Robust to junky/serialized 'slot_types' and 'type'.
    (Signature matches tests: (binder, fqcol))
    """
    cats = binder.get("catalogs") or {}
    cols = cats.get("columns") or {}
    meta = cols.get(fqcol) or {}
    st_raw = meta.get("slot_types")
    db_t   = meta.get("type", "")
    return _normalize_slot_types(st_raw, db_t)


# =========================
# Helpers to walk binder & vocab
# =========================

def _iter_table_columns(binder: Dict[str, Any]) -> Iterable[Tuple[str, str]]:
    """Yield (table, fqcol) for each fully-qualified column present."""
    cats = binder.get("catalogs") or {}
    cols = cats.get("columns") or {}
    for fq, meta in cols.items():
        table = (meta or {}).get("table")
        name = (meta or {}).get("name")
        if not table or not name:
            continue
        yield table, fq


def _projection_actions_from_vocab(vocab: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """
    Return actions with 'placement: projection'.
    Robust: prefer top-level 'sql_actions', fallback to 'keywords.sql_actions'.
    """
    actions = vocab.get("sql_actions")
    if not isinstance(actions, dict) or not actions:
        actions = ((vocab.get("keywords") or {}).get("sql_actions") or {})
    if not isinstance(actions, dict):
        actions = {}
    return {
        k: v for k, v in actions.items()
        if isinstance(v, dict) and (v.get("placement") or "").lower() == "projection"
    }


def _required_types(vmeta: Dict[str, Any], arg_key: str) -> List[str]:
    at = vmeta.get("applicable_types") or {}
    req = at.get(arg_key)
    if isinstance(req, str):
        return [req]
    if isinstance(req, list):
        return req
    return []


def _applicable(col_slots: Set[str], required: List[str]) -> bool:
    if not required:
        # strict: no applicable_types -> not applicable
        return False
    if "any" in {r.lower() for r in required}:
        return True
    return any(r in col_slots for r in required)


def _qi(s: str) -> str:
    return '"' + s.replace('"', '""') + '"'


def _baseline_sql(func: str, table: str, fqcol: str) -> str:
    tbl, col = fqcol.split(".", 1)
    if func.lower() == "distinct":
        return f'SELECT DISTINCT {_qi(tbl)}.{_qi(col)} FROM {_qi(table)}'
    return f'SELECT {func.upper()}({_qi(tbl)}.{_qi(col)}) FROM {_qi(table)}'


# =========================
# Public API
# =========================

def enumerate_specs(
    binder: Dict[str, Any],
    vocab: Dict[str, Any],
    *,
    arg_key: str = "column",
    max_specs: int = 1000,
) -> List[SQLSpec]:
    """
    Map (binder × vocab) -> list of SQLSpec, only when 'applicable_types' agrees with the column's slot types.
    Numeric/date columns are enumerated first to bias toward constraints downstream.
    """
    actions = _projection_actions_from_vocab(vocab)
    cols = list(_iter_table_columns(binder))

    def _prio(item: Tuple[str, str]) -> int:
        _, fq = item
        st = column_slot_types(binder, fq)
        if "numeric" in st: return 0
        if "date" in st:    return 1
        return 2

    cols.sort(key=_prio)

    out: List[SQLSpec] = []
    for table, fqcol in cols:
        slots = column_slot_types(binder, fqcol)
        for func, meta in actions.items():
            req = _required_types(meta, arg_key)
            if not _applicable(slots, req):
                continue
            out.append(SQLSpec(
                func=func,
                arg_key=arg_key,
                table=table,
                column=fqcol,
                expression_sql=_baseline_sql(func, table, fqcol),
            ))
            if len(out) >= max_specs:
                return out
    return out
