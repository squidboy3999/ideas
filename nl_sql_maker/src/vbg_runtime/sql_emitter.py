# vbg_runtime/sql_emitter.py
from __future__ import annotations

import re
from typing import Any, Dict, List, Sequence, Tuple

# Canonical core (serialize only; we tolerate either a bound object or a canonical string)
try:
    from vbg_generate.canonical_core import serialize_binding  # type: ignore
except Exception:  # pragma: no cover
    from src.vbg_generate.canonical_core import serialize_binding  # type: ignore


# -------------------------
# Quoting & helpers
# -------------------------

def quote_ident(ident: str, engine: str = "sqlite") -> str:
    """
    Quote an identifier for the target engine.
    - If dotted (table.column), quote each part individually.
    - SQLite uses double quotes by default.
    """
    if ident is None:
        return '""' if engine == "sqlite" else ""
    s = str(ident)
    if "." in s:
        return ".".join(quote_ident(p, engine) for p in s.split(".", 1))
    if engine == "sqlite":
        return f'"{s}"'
    # Fallback: ANSI-style double quotes
    return f'"{s}"'


def _get_fn_meta(binder_artifact: Dict[str, Any], fn: str) -> Dict[str, Any]:
    catalogs = binder_artifact.get("catalogs") if isinstance(binder_artifact, dict) else {}
    fns = catalogs.get("functions") if isinstance(catalogs, dict) else {}
    return (fns or {}).get(fn, {}) or {}


def _is_postgis_fn(fn: str) -> bool:
    return isinstance(fn, str) and fn.lower().startswith("st_")


def _normalize_oxford_and(text: str) -> str:
    """
    Turn 'A and B' -> 'A, B' and 'A, B, and C' -> 'A, B, C' at the *token* level,
    without touching 'of'/'from' or function names.
    """
    s = re.sub(r"\s*,\s*and\s+", ", ", text)
    s = re.sub(r"\s+and\s+", ", ", s)
    s = re.sub(r"\s{2,}", " ", s).strip()
    return s


# -------------------------
# Canonical parser (simple)
# -------------------------

_WORD_OR_DOT = r"[A-Za-z_][A-Za-z0-9_]*"
COL_RE = re.compile(fr"^{_WORD_OR_DOT}\.{_WORD_OR_DOT}$")


class _ParseError(ValueError):
    pass


def _tokenize(canonical: str) -> List[str]:
    return re.findall(r",|\.|[A-Za-z0-9_]+", canonical or "")


def _read_column(tokens: Sequence[str], i: int) -> Tuple[str, int]:
    """
    Read a dotted column (table.column) starting at tokens[i].
    Returns (column_id, next_index).
    """
    if i + 2 < len(tokens) and re.fullmatch(_WORD_OR_DOT, tokens[i]) and tokens[i + 1] == "." and re.fullmatch(_WORD_OR_DOT, tokens[i + 2]):
        col = f"{tokens[i]}.{tokens[i+2]}"
        return col, i + 3
    raise _ParseError(f"expected dotted column at token {i}: {tokens[i:i+3]}")


def _read_function(tokens: Sequence[str], i: int) -> Tuple[str, List[str], int]:
    """
    Read 'fn [of <col> (, <col>)*]' starting at tokens[i].
    Only treat commas as *argument* separators if what follows clearly starts a dotted column.
    Otherwise, stop and let the outer select-item loop handle the comma (i.e., next item).
    """
    fn = tokens[i]
    j = i + 1
    args: List[str] = []

    # Optional 'of'
    if j < len(tokens) and tokens[j].lower() == "of":
        j += 1

        # First column argument (required if 'of' is present)
        col, j = _read_column(tokens, j)
        args.append(col)

        # Additional arguments: only if comma is followed by a *dotted column* start
        while j < len(tokens) and tokens[j] == ",":
            # Lookahead to see if the next tokens look like <ident> . <ident>
            if (j + 3) <= len(tokens) and \
               re.fullmatch(_WORD_OR_DOT, tokens[j + 1] or "") and \
               tokens[j + 2] == "." and \
               (j + 3) < len(tokens) and re.fullmatch(_WORD_OR_DOT, tokens[j + 3] or ""):
                j += 1  # consume ','
                col, j = _read_column(tokens, j)
                args.append(col)
            else:
                # Comma likely separates *select items*, not more function args.
                # Do NOT consume it; let the outer loop see it.
                break

    return fn, args, j



def _split_select_from(canonical: str) -> Tuple[str, str]:
    s = canonical.strip()
    low = s.lower()
    if not low.startswith("select "):
        raise _ParseError("canonical must start with 'select '")
    # Find the last ' from ' to avoid false positives in names (rare)
    idx = low.rfind(" from ")
    if idx <= 0:
        raise _ParseError("canonical must contain ' from '")
    return s[len("select "):idx], s[idx + 6 :].strip()


def _parse_items(canonical_items: str, binder_artifact: Dict[str, Any]) -> List[Tuple[str, Any]]:
    """
    Return a list of items:
      - ('column', 'table.column')
      - ('function', fn_name, [args...])
    Uses binder metadata to decide function arity; defaults to 1 arg if unknown.
    """
    toks = _tokenize(canonical_items)
    out: List[Tuple[str, Any]] = []
    i = 0
    while i < len(toks):
        # Skip list separators first (support both ',' and any stray 'and')
        if toks[i] == "," or toks[i].lower() == "and":
            i += 1
            continue

        # Try column first
        try:
            col, j = _read_column(toks, i)
            out.append(("column", col))
            i = j
            continue
        except _ParseError:
            pass

        # Otherwise, assume function
        fn, args_greedy, j = _read_function(toks, i)
        meta = _get_fn_meta(binder_artifact, fn)
        arg_spec = meta.get("args") or []

        # Determine arity: if explicitly declared use it; else use however many we actually parsed.
        if isinstance(arg_spec, list) and len(arg_spec) >= 1:
            arity = len(arg_spec)
        else:
            arity = len(args_greedy)

        args = args_greedy[:arity]
        out.append(("function", fn, args))
        i = j

    return out



def _parse_canonical(canonical: str, binder_artifact: Dict[str, Any]) -> Tuple[str, List[Tuple[str, Any]]]:
    """
    Parse a (possibly Oxford-AND) canonical string into (from_table, items).
    """
    s = _normalize_oxford_and(canonical or "")
    items_str, from_table = _split_select_from(s)
    items = _parse_items(items_str, binder_artifact)
    return from_table, items


# -------------------------
# Function formatting
# -------------------------

def format_fn_call(
    fn: str,
    arg_sqls: Sequence[str],
    binder_artifact: Dict[str, Any],
    engine: str = "sqlite",
    from_table: str | None = None,
) -> str:
    """
    Render a function using binder template when present. Supported placeholders:
      {column}, {columns}, {table}, {value}
    Fallback: `fn(arg1, arg2, ...)`.
    """
    meta = _get_fn_meta(binder_artifact, fn)
    tmpl = meta.get("template") if isinstance(meta, dict) else None

    # Environment for template
    env = {
        "column": arg_sqls[0] if arg_sqls else "",
        "columns": ", ".join(arg_sqls),
        "table": quote_ident(from_table or "", engine),
        "value": "__VALUE__",  # left as-is for MVP
    }

    if isinstance(tmpl, str) and "{" in tmpl:
        # Simple braces replacement; if formatting fails, fall back
        try:
            return tmpl.format(**env)
        except Exception:
            pass

    # Default: raw call
    return f"{fn}({', '.join(arg_sqls)})"


# -------------------------
# Public API
# -------------------------

def emit_select_with_warnings(
    bound_or_canonical: Any,
    *,
    binder_artifact: Dict[str, Any],
    engine: str = "sqlite",
) -> Tuple[str, List[str]]:
    """
    Emit SQL SELECT from a bound object *or* canonical string.
    Returns (sql, warnings).
    """
    # Canonical surface
    if isinstance(bound_or_canonical, str):
        canonical = bound_or_canonical
    else:
        canonical = serialize_binding(bound_or_canonical)

    from_table, items = _parse_canonical(canonical, binder_artifact)

    sel_sqls: List[str] = []
    warnings: List[str] = []

    for item in items:
        if not item:
            continue
        if item[0] == "column":
            col = str(item[1])
            # Expect dotted; if not, qualify with FROM table
            if "." not in col and from_table:
                col = f"{from_table}.{col}"
            sel_sqls.append(quote_ident(col.split(".", 1)[0], engine) + "." + quote_ident(col.split(".", 1)[1], engine))
        elif item[0] == "function":
            _, fn, cols = item
            if _is_postgis_fn(fn) and engine == "sqlite":
                warnings.append(f"Function '{fn}' looks PostGIS; SQLite may not support it.")
            arg_sqls: List[str] = []
            for c in (cols or []):
                # Qualify & quote
                if "." not in c and from_table:
                    c = f"{from_table}.{c}"
                arg_sqls.append(
                    quote_ident(c.split(".", 1)[0], engine) + "." + quote_ident(c.split(".", 1)[1], engine)
                )
            sel_sqls.append(format_fn_call(fn, arg_sqls, binder_artifact, engine=engine, from_table=from_table))
        else:
            # Unknown item — skip safely
            continue

    sql = f'SELECT {", ".join(sel_sqls)} FROM {quote_ident(from_table, engine)};'
    return sql, warnings


def emit_select(
    bound_or_canonical: Any,
    *,
    binder_artifact: Dict[str, Any],
    engine: str = "sqlite",
) -> str:
    """
    Convenience wrapper that drops warnings.
    """
    sql, _ = emit_select_with_warnings(bound_or_canonical, binder_artifact=binder_artifact, engine=engine)
    return sql
