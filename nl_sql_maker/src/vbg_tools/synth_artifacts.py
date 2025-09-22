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


def build_binder(schema_yaml: dict, vocabulary: dict) -> dict:
    # tables & columns
    table_rows = collect_table_rows(schema_yaml)
    column_rows = collect_column_rows(schema_yaml)

    tables = {r["n"]: {} for r in table_rows}
    columns = {}
    for r in column_rows:
        fqn = r["fqn"]
        columns[fqn] = {
            "name": fqn.split(".")[-1],
            "table": r["table"],
            "type": (r.get("types") or [None])[0],
            "slot_types": r.get("types") or [],
        }

    # functions: prefer explicit schema.functions; else derive from vocabulary.keywords.sql_actions
    schema_functions_rows = collect_functions_from_schema(schema_yaml)
    functions: dict = {}
    if schema_functions_rows:
        for r in schema_functions_rows:
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
        sql_actions = (vocabulary.get("keywords") or {}).get("sql_actions") or {}
        functions = _functions_from_actions(sql_actions)

    functions = _ensure_ordering_functions(functions)

    # connectors mirror
    connectors = (vocabulary.get("keywords") or {}).get("connectors") or {}
    connectors = ensure_core_connectors(connectors)

    binder = {
        "catalogs": {
            "tables": tables,
            "columns": columns,
            "functions": functions,
            "connectors": connectors,
        }
    }
    return binder


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
