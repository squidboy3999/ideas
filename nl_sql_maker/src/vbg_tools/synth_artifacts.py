# vbg_tools/synth_artifacts.py
from __future__ import annotations

from typing import Any, Dict, List, Set

from .artifact_helpers import (
    extract_keywords_root,
    normalize_aliases,
    connectors_from_keywords,
    select_template_from_keywords,
    collect_table_rows,
    collect_column_rows,
    collect_functions_from_schema,
    ensure_core_connectors,
)


# =========================
# Vocabulary synthesis
# =========================

def _collect_select_verbs(kw: dict) -> dict:
    out = {}
    for can, meta in (kw.get("select_verbs") or {}).items():
        out[str(can)] = {"aliases": normalize_aliases(meta.get("aliases", []))}
    return out


def _collect_comparators(kw: dict) -> dict:
    out = {}
    for can, meta in (kw.get("comparison_operators") or {}).items():
        out[str(can)] = {"aliases": normalize_aliases(meta.get("aliases", []))}
    return out


def _collect_filler_words(kw: dict) -> dict:
    aliases = (((kw.get("filler_words") or {}).get("_skip") or {}).get("aliases")) or []
    return {"_skip": {"aliases": normalize_aliases(aliases)}}


def _normalize_action_entry(name: str, meta: dict) -> dict:
    """
    Normalize a single action entry from top-level sql_actions (input YAML).
    Produces a dict that includes:
      - template (str)
      - aliases (sorted/deduped)
      - placement (default 'projection')
      - bind_style (default 'of' if template hints {column}/{value}, else 'to')
      - applicable_types (arg -> [slot_type,...]) as strings
      - reqs (list of {arg, st}) flattened from applicable_types
    """
    aliases = normalize_aliases(meta.get("aliases", []))
    template = str(meta.get("template", "")).strip()

    placement = meta.get("placement") or "projection"
    bind_style = meta.get("bind_style")
    if not bind_style:
        # Heuristic default: 'of' if template references an arg; otherwise 'to'
        bind_style = "of" if ("{column}" in template or "{value}" in template or "{" in template) else "to"

    applicable_types = meta.get("applicable_types") or {}
    # Normalize applicable_types → map[str] -> list[str]
    app_norm: dict[str, List[str]] = {}
    for arg, types in applicable_types.items():
        if isinstance(types, list):
            app_norm[str(arg)] = [str(t) for t in types if str(t).strip()]
        elif types is None:
            app_norm[str(arg)] = []
        else:
            app_norm[str(arg)] = [str(types)]

    # Flatten to reqs
    reqs: List[dict] = []
    for arg, types in app_norm.items():
        if types:
            for st in types:
                reqs.append({"arg": arg, "st": st})
        else:
            # If no explicit slot types, keep a generic 'any' requirement (optional)
            pass

    return {
        "template": template,
        "aliases": aliases,
        "placement": placement,
        "bind_style": bind_style,
        "applicable_types": app_norm,
        "reqs": reqs,
    }


def _collect_sql_actions_combined(keywords_yaml: dict) -> dict:
    """
    New contract: all actions (incl. postgis) are at top-level 'sql_actions'.
    We still emit them under vocabulary['keywords']['sql_actions'] for runtime/tests.
    If the legacy nested 'keywords.sql_actions' also exists, we merge (top-level wins).
    """
    top = keywords_yaml.get("sql_actions") or {}
    if not isinstance(top, dict):
        top = {}

    nested_kw = extract_keywords_root(keywords_yaml)
    legacy = (nested_kw.get("sql_actions") or {}) if isinstance(nested_kw, dict) else {}

    merged_names = set(top.keys()) | set(legacy.keys())
    out: dict = {}
    for name in merged_names:
        # Prefer top-level definition if present
        src = top.get(name) if name in top else legacy.get(name, {})
        if not isinstance(src, dict):
            src = {}
        out[str(name)] = _normalize_action_entry(str(name), src)
    return out


def build_vocabulary(keywords_yaml: dict) -> dict:
    kw = extract_keywords_root(keywords_yaml)

    select_verbs = _collect_select_verbs(kw)
    comparators = _collect_comparators(kw)
    filler_words = _collect_filler_words(kw)
    connectors = connectors_from_keywords(kw)  # ensures core AND/OR/FROM/OF/COMMA present
    select_template = select_template_from_keywords(kw)

    # NEW: gather actions from top-level sql_actions (and merge with legacy if present)
    sql_actions = _collect_sql_actions_combined(keywords_yaml)

    vocabulary = {
        "keywords": {
            "select_verbs": select_verbs,
            "comparison_operators": comparators,
            "filler_words": filler_words,
            "connectors": connectors,
            "global_templates": {"select_template": select_template},
            # Keep actions under 'keywords' in the emitted vocabulary
            "sql_actions": sql_actions,
        }
    }
    return vocabulary


# =========================
# Binder synthesis
# =========================

def _functions_from_actions(sql_actions: dict) -> dict:
    """
    Derive function signatures directly from sql_actions (arity = number of unique arg names in 'reqs').
    Carry placement and bind_style through so clause actions can be modeled later.
    """
    out = {}
    for name, meta in (sql_actions or {}).items():
        reqs = meta.get("reqs") or []
        arity = len({(r.get("arg") or "") for r in reqs})
        out[name] = {
            "arity": arity,
            "template": meta.get("template", ""),
            "requirements": reqs,
            "placement": meta.get("placement", "projection"),
            "bind_style": meta.get("bind_style", "of"),
        }
    return out


def _ensure_ordering_functions(functions: dict) -> dict:
    has_desc = any(n in functions for n in ("order_by_desc", "orderby_desc"))
    has_asc = any(n in functions for n in ("order_by", "orderby"))

    if not has_desc:
        functions["order_by_desc"] = {
            "arity": 1,
            "template": "{column}",
            "requirements": [{"arg": "column", "st": "any"}],
            "placement": "clause",
            "bind_style": "of",
        }

    if not has_asc:
        functions["order_by"] = {
            "arity": 1,
            "template": "{column}",
            "requirements": [{"arg": "column", "st": "any"}],
            "placement": "clause",
            "bind_style": "of",
        }
    return functions


def _normalize_db_type_str(t: str | None) -> str | None:
    if not t:
        return None
    s = str(t).strip()
    # If someone accidentally passed a dict-as-string, ignore it.
    if s.startswith("{") and s.endswith("}"):
        return None
    return s


def _slot_types_from_types_list(types: list[str]) -> list[str]:
    """
    Convert a list that may contain DB types (INTEGER, VARCHAR(50), DECIMAL...)
    or abstract types (numeric, text, date, timestamp, geometry_*) into a
    deduped set of abstract slot types understood by the runtime.
    """
    out: set[str] = set()
    for raw in (types or []):
        if not raw:
            continue
        s = str(raw).strip()
        if not s:
            continue
        # If it's a dict-as-string (bad), skip
        if s.startswith("{") and s.endswith("}"):
            continue
        lo = s.lower()

        # Already-abstract types pass through
        if lo in {"any", "numeric", "integer", "float", "boolean", "text", "date", "timestamp",
                  "geometry", "geography",
                  "geometry_point", "geometry_linestring", "geometry_polygon",
                  "geography_point", "geography_linestring", "geography_polygon"}:
            # Normalize numeric family
            if lo in {"integer", "float"}:
                out.add("numeric")
            else:
                out.add(lo)
            continue

        # DB → abstract mapping
        if any(k in lo for k in ("int", "decimal", "numeric", "real", "double", "money", "number")):
            out.add("numeric")
        elif any(k in lo for k in ("char", "text", "varchar", "string", "uuid", "json")):
            out.add("text")
        elif "bool" in lo:
            out.add("boolean")
        elif lo == "date":
            out.add("date")
        elif "timestamp" in lo or "datetime" in lo:
            out.add("timestamp")
        elif "polygon" in lo:
            out.add("geometry_polygon")
        elif "linestring" in lo or "line_string" in lo:
            out.add("geometry_linestring")
        elif "point" in lo:
            out.add("geometry_point")
        elif "geometry" in lo:
            out.add("geometry")
        elif "geography" in lo:
            out.add("geography")
        else:
            # Unknown → don't invent; leave it out
            pass

    return sorted(out)


def build_binder(schema_yaml: dict, vocabulary: dict) -> dict:
    """
    Build binder with normalized tables/columns/functions/connectors.
    Columns:
      - name: string (no dict-like)
      - table: string
      - type: DB type string or None (clean; no dict-like)
      - slot_types: list[str] abstract types (numeric/text/date/timestamp/geometry_*)
    """
    table_rows = collect_table_rows(schema_yaml)
    column_rows = collect_column_rows(schema_yaml)  # now returns clean fqn/table/name/types

    tables = {r["n"]: {} for r in table_rows}
    columns: Dict[str, Dict[str, Any]] = {}

    def _normalize_db_type_str(t: str | None) -> str | None:
        if not t:
            return None
        s = str(t).strip()
        return None if (s.startswith("{") and s.endswith("}")) else s

    def _slot_types_from_list(types: List[str]) -> List[str]:
        out: set[str] = set()
        for raw in (types or []):
            if not raw:
                continue
            s = str(raw).strip()
            if not s or (s.startswith("{") and s.endswith("}")):
                continue
            lo = s.lower()

            # Already-abstract
            if lo in {"any", "numeric", "integer", "float", "boolean", "text", "date", "timestamp",
                      "geometry", "geography",
                      "geometry_point", "geometry_linestring", "geometry_polygon",
                      "geography_point", "geography_linestring", "geography_polygon"}:
                if lo in {"integer", "float"}:
                    out.add("numeric")
                else:
                    out.add(lo)
                continue

            # DB → abstract
            if any(k in lo for k in ("int", "decimal", "numeric", "real", "double", "money", "number")):
                out.add("numeric")
            elif any(k in lo for k in ("char", "text", "varchar", "string", "uuid", "json")):
                out.add("text")
            elif "bool" in lo:
                out.add("boolean")
            elif lo == "date":
                out.add("date")
            elif "timestamp" in lo or "datetime" in lo:
                out.add("timestamp")
            elif "polygon" in lo:
                out.add("geometry_polygon")
            elif "linestring" in lo or "line_string" in lo:
                out.add("geometry_linestring")
            elif "point" in lo:
                out.add("geometry_point")
            elif "geometry" in lo:
                out.add("geometry")
            elif "geography" in lo:
                out.add("geography")
        return sorted(out)

    for r in column_rows:
        fqn = r["fqn"]                             # guaranteed "table.col"
        table = r["table"]
        name = r.get("name") or fqn.split(".", 1)[-1]
        raw_types: List[str] = r.get("types") or []

        # Choose one clean DB type if present
        db_type: str | None = None
        for t in raw_types:
            tnorm = _normalize_db_type_str(t)
            if tnorm:
                db_type = tnorm
                break

        slot_types = _slot_types_from_list(raw_types)

        columns[fqn] = {
            "name": name,
            "table": table,
            "type": db_type,
            "slot_types": slot_types,
        }

    # functions
    schema_fns = collect_functions_from_schema(schema_yaml)
    if schema_fns:
        functions: Dict[str, Any] = {}
        for r in schema_fns:
            name = r["name"]
            arity = len({(x.get("arg") or "") for x in (r.get("reqs") or [])})
            functions[name] = {
                "arity": arity,
                "template": r.get("template", ""),
                "requirements": r.get("reqs") or [],
                "placement": r.get("placement") or "projection",
                "bind_style": r.get("bind_style") or "of",
            }
    else:
        # Legacy path from vocabulary
        actions = (vocabulary.get("keywords") or {}).get("sql_actions") or {}
        functions = _functions_from_actions(actions)

    functions = _ensure_ordering_functions(functions)

    connectors = ensure_core_connectors(((vocabulary.get("keywords") or {}).get("connectors") or {}))

    return {
        "catalogs": {
            "tables": tables,
            "columns": columns,
            "functions": functions,
            "connectors": connectors,
        }
    }


# =========================
# Grammar synthesis
# =========================

def _emit_terminal_lines(connectors: dict) -> list[str]:
    """
    Emit case-insensitive terminals for words; COMMA uses ','.
    Avoid ever quoting the lowercase placeholders 'table', 'columns', 'value'.
    """
    lines = []
    seen = set()

    def add(name: str, literal: str):
        nonlocal lines, seen
        if name in seen:
            return
        if literal.isalpha():
            lines.append(f'{name}: "{literal}"i')
        else:
            lines.append(f'{name}: "{literal}"')
        seen.add(name)

    add("SELECT", "select")

    # connectors may include AND/OR/NOT/FROM/OF/COMMA (+ others)
    for k, v in (connectors or {}).items():
        name = str(k).upper()
        lit = str(v)
        if name == "SELECT":
            continue
        if lit.lower() in ("table", "columns", "value"):
            continue
        add(name, lit)

    if "FROM" not in seen:
        add("FROM", "from")
    if "COMMA" not in seen:
        add("COMMA", ",")

    return lines


def _collect_action_names(vocabulary: dict, binder: dict) -> set[str]:
    # Prefer actions from vocabulary (keywords.sql_actions)
    kw = (vocabulary.get("keywords") or {})
    names: set[str] = set((kw.get("sql_actions") or {}).keys())
    # If none, fall back to binder functions
    if not names:
        funcs = ((binder.get("catalogs") or {}).get("functions") or {})
        names.update(funcs.keys())
    # Never include placeholders
    return {n for n in names if n.lower() not in {"table", "columns", "value"}}


def _emit_action_rule(names: set[str]) -> str:
    if not names:
        return 'action: "count"i | "avg"i | "sum"i | "min"i | "max"i'
    alts = " | ".join(f'"{n}"i' for n in sorted(names))
    return f"action: {alts}"


def build_grammar(vocabulary: dict, binder: dict) -> str:
    connectors = (vocabulary.get("keywords") or {}).get("connectors") or {}
    connectors = ensure_core_connectors(connectors)

    parts: list[str] = []
    parts.append("// Auto-generated Lark grammar (offline synthesis)")

    # terminals
    parts.extend(_emit_terminal_lines(connectors))
    parts.append("")
    parts.append("start: query")

    # action rule
    action_names = _collect_action_names(vocabulary, binder)
    parts.append(_emit_action_rule(action_names))

    # VALUE token (uppercase token; this is not the banned lowercase literal)
    parts.append('VALUE: "VALUE"')

    # projection and query paths
    parts.append("projection: action [OF] VALUE")
    # IMPORTANT: single rule with alternatives (avoid duplicate rule definition)
    parts.append("query: SELECT FROM | SELECT projection FROM")

    parts.append("")
    parts.append("%import common.WS")
    parts.append("%ignore WS")

    grammar = "\n".join(parts) + "\n"
    return grammar
