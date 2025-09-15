#!/usr/bin/env python3
# vbg_tools/make_surfaces.py
from __future__ import annotations
import os, sys, re, yaml
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Tuple, Optional, Iterable, Set

from .create_cli_test import generate_cli_test

# --- Reuse runtime pieces (no shell-outs) ---
from .graph_runtime import (
    must_load_yaml, must_load_text,
    build_lexicon_and_connectors, build_index, match_aliases,
    build_schema_indices, tokenize,
    map_text,  # for parse validation
)

# --- Use the same SQL builder as runtime ---
from .sql_helpers import build_select_sql_from_slots

# Optional direct graph pull
try:
    from .cypher_helper import get_driver, with_session
    _HAS_NEO4J = True
except Exception:
    _HAS_NEO4J = False

# ---------- Config ----------
ART_DIR = Path(os.environ.get("ARTIFACTS_DIR", "out"))

DEFAULT_OUT_GOLD = ART_DIR / "gold_surfaces.yml"
DEFAULT_OUT_MULTI = ART_DIR / "valid_multipath.yml"
DEFAULT_OUT_INVALID = ART_DIR / "invalid_surfaces.yml"

# Limit how many surfaces we generate per (func, table, column) spec
MAX_SURFACES_PER_SPEC = int(os.environ.get("MAX_SURFACES_PER_SPEC", "6"))
MAX_FUNCS = int(os.environ.get("MAX_FUNCS", "50"))  # safety bound
MAX_SPECS = int(os.environ.get("MAX_SPECS", "200"))

# Cap predicates per surface (0..2); we also validate with the grammar
MAX_PREDICATES_PER_SURFACE = int(os.environ.get("MAX_PREDICATES_PER_SURFACE", "2"))

# ---------- Data classes ----------
@dataclass(frozen=True)
class SQLSpec:
    func: str             # function canonical name (e.g., "count", "st_area")
    arg_key: str          # template argument key (e.g., "column", "geom")
    table: str            # table name
    column: str           # base column name (not FQN)
    expression_sql: str   # SELECT <func("table"."col")> FROM "table"

# ---------- Small utils ----------
def _lower_strip(s: str) -> str:
    return str(s or "").strip().lower()

def _quote_ident(name: str) -> str:
    # simple ANSI-ish quoting; adjust if you use backticks etc.
    return '"' + name.replace('"', '""') + '"'

def _normalize_sql(s: str) -> str:
    """
    Very light normalization for equality checks:
    - lowercase
    - strip double quotes
    - collapse whitespace
    - drop trailing 'limit <n>' if present
    """
    t = _lower_strip(s)
    t = t.replace('"', "")
    t = re.sub(r"\s+", " ", t)
    # remove trailing ' limit N'
    t = re.sub(r"\s+limit\s+\d+\s*$", "", t)
    return t.strip()

def _iter_applicable_columns(
    binder: Dict[str, Any],
    required_types: List[str],
) -> Iterable[Tuple[str, str]]:  # yields (table, base_col)
    """
    Yield columns whose types intersect required_types.
    """
    catalogs = binder.get("catalogs") or {}
    columns = catalogs.get("columns") or {}
    for _fqn, meta in (columns or {}).items():
        if not isinstance(meta, dict): continue
        table = meta.get("table")
        name  = meta.get("name")
        types = [str(t) for t in (meta.get("slot_types") or [])]
        if not table or not name: continue
        if not required_types:
            yield (table, name)
        else:
            if any(_lower_strip(t) in {_lower_strip(x) for x in required_types} for t in types):
                yield (table, name)

def _functions_from_binder(binder: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    return (binder.get("catalogs") or {}).get("functions") or {}

def _connectors_from_binder(binder: Dict[str, Any]) -> Dict[str, str]:
    return (binder.get("catalogs") or {}).get("connectors") or {}

def _select_aliases_from_vocab(vocab: Dict[str, Any]) -> List[str]:
    out: List[str] = []
    sv = ((vocab.get("keywords") or {}).get("select_verbs") or {})
    if isinstance(sv, dict):
        for _can, ent in sv.items():
            als = ent.get("aliases")
            if isinstance(als, list):
                out.extend([str(a) for a in als if isinstance(a, str)])
    return sorted({a for a in out if a})

def _func_aliases_from_vocab(vocab: Dict[str, Any], func: str) -> List[str]:
    """
    Pull aliases from BOTH sql_actions and postgis_actions.
    """
    out: List[str] = []
    for sec_name in ("sql_actions", "postgis_actions"):
        sec = vocab.get(sec_name) or {}
        ent = sec.get(func) or {}
        als = ent.get("aliases")
        if isinstance(als, list):
            out.extend([str(a) for a in als if isinstance(a, str)])
    return out

def _first_aliases(xs: List[str], k: int) -> List[str]:
    if not xs: return []
    if len(xs) <= k: return xs
    return xs[:k]  # deterministic

def _trim_trailing_of(s: str) -> str:
    """Remove a trailing 'of' (or ', of', etc.) in a function alias to avoid 'of of'."""
    s2 = s.strip()
    toks = re.findall(r"[A-Za-z0-9_]+|[,]", s2.lower())
    if toks and toks[-1] == "of":
        s2 = s2[: s2.lower().rfind(" of")].rstrip()
    return s2

def _render_func_expr(func_template: str, arg_key: str, table: str, col: str) -> str:
    fqn = f'{_quote_ident(table)}.{_quote_ident(col)}'
    return func_template.replace("{" + arg_key + "}", fqn)

def _sql_for_spec(func_template: str, arg_key: str, table: str, col: str) -> str:
    expr = _render_func_expr(func_template, arg_key, table, col)
    return f"SELECT {expr} FROM {_quote_ident(table)}"

def _action_kind_from_template(tmpl: Optional[str]) -> str:
    if not isinstance(tmpl, str) or not tmpl.strip():
        return "projection"
    t = tmpl.strip().upper()
    if t.startswith("ORDER BY") or t.startswith("GROUP BY") or t.startswith("HAVING") or t.startswith("LIMIT"):
        return "clause"
    return "projection"

# ---------- Comparator catalog (type rules & arity shape) ----------
_COMPARATOR_TYPE_RULES: Dict[str, Dict[str, Any]] = {
    "equal":                 {"column": {"any"},                        "value": {"any"},          "shape": "single"},
    "not_equal":             {"column": {"any"},                        "value": {"any"},          "shape": "single"},
    "greater_than":          {"column": {"numeric","date","timestamp"},"value": {"numeric","date","timestamp"}, "shape": "single"},
    "less_than":             {"column": {"numeric","date","timestamp"},"value": {"numeric","date","timestamp"}, "shape": "single"},
    "greater_than_or_equal": {"column": {"numeric","date","timestamp"},"value": {"numeric","date","timestamp"}, "shape": "single"},
    "less_than_or_equal":    {"column": {"numeric","date","timestamp"},"value": {"numeric","date","timestamp"}, "shape": "single"},
    "between":               {"column": {"numeric","date","timestamp"},"value": {"numeric","date","timestamp"}, "shape": "between"},
    "in":                    {"column": {"any"},                        "value": {"any"},          "shape": "list"},
    "like":                  {"column": {"text"},                       "value": {"text"},         "shape": "single_like"},
    "is_null":               {"column": {"any"},                        "value": set(),            "shape": "no_value"},
    "is_not_null":           {"column": {"any"},                        "value": set(),            "shape": "no_value"},
}

def _comparator_aliases_from_vocab(vocab: Dict[str, Any]) -> Dict[str, List[str]]:
    out: Dict[str, List[str]] = {}
    comps = ((vocab.get("keywords") or {}).get("comparison_operators") or {})
    if not isinstance(comps, dict):
        return out
    for canonical, ent in comps.items():
        als = ent.get("aliases")
        if isinstance(als, list):
            # Keep aliases that contain letters (skip bare symbols; tokenizer/lexicon match them independently)
            textual = [a for a in als if re.search(r"[A-Za-z]", str(a))]
            if textual:
                out[str(canonical)] = [str(a) for a in textual]
    return out

def _logical_aliases_from_vocab(vocab: Dict[str, Any]) -> Dict[str, List[str]]:
    out: Dict[str, List[str]] = {}
    log = ((vocab.get("keywords") or {}).get("logical_operators") or {})
    if not isinstance(log, dict):
        return out
    for canonical, ent in log.items():
        als = ent.get("aliases")
        if isinstance(als, list):
            out[str(canonical)] = [str(a) for a in als if isinstance(a, str)]
    return out

def _columns_for_table(binder: Dict[str, Any], table: str) -> List[Tuple[str, List[str]]]:
    """Return [(base_col_name, slot_types), ...] for a given table."""
    cols: List[Tuple[str, List[str]]] = []
    catalogs = binder.get("catalogs") or {}
    columns = catalogs.get("columns") or {}
    for _fqn, meta in (columns or {}).items():
        if not isinstance(meta, dict): continue
        if meta.get("table") != table: continue
        base = meta.get("name")
        types = [str(t) for t in (meta.get("slot_types") or []) if t]
        if base:
            cols.append((base, sorted({t.lower() for t in types})))
    return cols

def _types_are_predicate_friendly(tset: Set[str]) -> bool:
    friendly = {"numeric", "text", "date", "timestamp", "id", "any"}
    return bool(tset & friendly)

# ---------- Value synthesis (deterministic) ----------
def _pick_numeric_values() -> Tuple[str, str]:
    return "5", "10"

def _pick_date_values() -> Tuple[str, str]:
    return "'2021-01-01'", "'2022-01-01'"

def _pick_timestamp_values() -> Tuple[str, str]:
    return "'2021-01-01 00:00:00'", "'2022-01-01 00:00:00'"

def _pick_text_value_like() -> str:
    return "'a%'"  # wildcard for LIKE

def _pick_text_value_eq() -> str:
    return "'alpha'"

def _value_pair_for_types(vkinds: Set[str]) -> Tuple[str, str]:
    if {"numeric"} & vkinds:
        return _pick_numeric_values()
    if {"date"} & vkinds:
        return _pick_date_values()
    if {"timestamp"} & vkinds:
        return _pick_timestamp_values()
    return _pick_numeric_values()

def _single_value_for_types(vkinds: Set[str], for_like: bool = False) -> str:
    if {"numeric"} & vkinds:
        return _pick_numeric_values()[0]
    if {"date"} & vkinds:
        return _pick_date_values()[0]
    if {"timestamp"} & vkinds:
        return _pick_timestamp_values()[0]
    if {"text"} & vkinds:
        return _pick_text_value_like() if for_like else _pick_text_value_eq()
    return _pick_text_value_eq()

# ---------- Predicate synthesis ----------
def _choose_valid_comparators_for_column(
    col_types: List[str],
    comp_aliases: Dict[str, List[str]],
) -> List[Tuple[str, str, str]]:
    col_tset = {t.lower() for t in col_types}
    out: List[Tuple[str, str, str]] = []
    for canonical, rules in _COMPARATOR_TYPE_RULES.items():
        if canonical not in comp_aliases:
            continue
        allowed_cols: Set[str] = set(rules.get("column") or [])
        if "any" not in allowed_cols and not (col_tset & allowed_cols):
            continue
        alias_list = comp_aliases[canonical]
        if not alias_list:
            continue
        alias = alias_list[0]  # deterministic alias
        shape = str(rules.get("shape") or "single")
        out.append((canonical, alias, shape))
    return out

def _render_predicate_text(
    column: str,
    comparator_canonical: str,
    alias_text: str,
    col_types: List[str],
) -> str:
    """Return a grammar-friendly predicate string for NL (no WHERE)."""
    rules = _COMPARATOR_TYPE_RULES.get(comparator_canonical, {})
    shape = rules.get("shape", "single")
    vtypes: Set[str] = set(rules.get("value") or [])

    if shape == "no_value":
        return f"{column} {alias_text}"
    elif shape == "between":
        v1, v2 = _value_pair_for_types(vtypes or set(col_types))
        return f"{column} {alias_text} {v1} and {v2}"
    elif shape == "list":
        v1 = _single_value_for_types(vtypes or set(col_types))
        v2 = _single_value_for_types(vtypes or set(col_types))
        return f"{column} {alias_text} {v1}, {v2}"
    elif shape == "single_like":
        v = _single_value_for_types(vtypes or set(col_types), for_like=True)
        return f"{column} {alias_text} {v}"
    else:
        v = _single_value_for_types(vtypes or set(col_types))
        return f"{column} {alias_text} {v}"

def _predicates_for_table(
    vocab: Dict[str, Any],
    binder: Dict[str, Any],
    table: str,
    max_predicates: int,
) -> List[Tuple[str, str]]:
    """
    Produce (predicate_text, canonical_op) up to max_predicates for `table`.
    """
    comp_aliases = _comparator_aliases_from_vocab(vocab)
    if not comp_aliases:
        return []

    pred_pairs: List[Tuple[str, str]] = []
    for base_col, col_types in _columns_for_table(binder, table):
        tset = {t.lower() for t in col_types}
        if not _types_are_predicate_friendly(tset):
            continue
        cand = _choose_valid_comparators_for_column(col_types, comp_aliases)
        for canonical, alias, _shape in cand[:2]:  # at most 2 per column
            pred_pairs.append(
                (_render_predicate_text(base_col, canonical, alias, col_types), canonical)
            )
            if len(pred_pairs) >= max_predicates:
                return pred_pairs
    return pred_pairs[:max_predicates]

# ---------- Sources for generating SQL specs ----------
def specs_from_artifacts(binder: Dict[str, Any], max_specs: int) -> List[SQLSpec]:
    out: List[SQLSpec] = []
    functions = _functions_from_binder(binder)
    if not isinstance(functions, dict):
        return out

    for fname, meta in functions.items():
        if not isinstance(meta, dict):
            continue
        template = meta.get("template")
        if not isinstance(template, str) or not template.strip():
            continue
        if _action_kind_from_template(template) == "clause":
            continue
        if int(meta.get("arity") or 0) != 1:
            continue

        app = meta.get("applicable_types") or {}
        if not isinstance(app, dict) or not app:
            continue

        arg_key = next(iter(app.keys()))
        required = app.get(arg_key) or []
        req_types = [str(t) for t in (required if isinstance(required, list) else [required])]

        for table, base_col in _iter_applicable_columns(binder, req_types):
            sql = _sql_for_spec(template, arg_key, table, base_col)
            out.append(SQLSpec(func=fname, arg_key=str(arg_key), table=str(table), column=str(base_col), expression_sql=sql))
            if len(out) >= max_specs:
                return out

    return out[:max_specs]

def specs_from_graph(uri: str, user: str, password: str, database: Optional[str], max_specs: int) -> List[SQLSpec]:
    if not _HAS_NEO4J:
        raise RuntimeError("Neo4j driver not available; install neo4j and ensure cypher_helper imports.")

    driver = get_driver(uri, user, password)
    out: List[SQLSpec] = []

    def _function_rows(sess) -> List[Dict[str, Any]]:
        return sess.execute_read(lambda tx: tx.run(
            """
            MATCH (f:Function)
            OPTIONAL MATCH (f)-[ar:ARG_REQUIRES]->(st:SlotType)
            WITH f, collect({arg:ar.arg, st:st.name}) AS reqs
            RETURN f.name AS name, f.template AS template, reqs
            ORDER BY name
            """
        ).data())

    def _columns(sess) -> List[Dict[str, Any]]:
        return sess.execute_read(lambda tx: tx.run(
            """
            MATCH (t:Table)-[:OWNS]->(c:Column)
            OPTIONAL MATCH (c)-[:HAS_TYPE]->(st:SlotType)
            RETURN t.name AS table, c.name AS name, collect(DISTINCT st.name) AS types
            ORDER BY t.name, c.name
            """
        ).data())

    with with_session(driver, database) as sess:
        frows = _function_rows(sess)
        crows = _columns(sess)

    cols_by_type: Dict[str, List[Tuple[str,str]]] = {}
    for r in crows:
        table, name = str(r["table"]), str(r["name"])
        types = [str(t) for t in (r["types"] or []) if t]
        for typ in types:
            cols_by_type.setdefault(_lower_strip(typ), []).append((table, name))

    for fr in frows:
        fname = str(fr["name"])
        template = fr.get("template")
        if not isinstance(template, str) or not template.strip(): continue

        reqs = [p for p in fr.get("reqs") or [] if p and p.get("arg")]
        if len({p["arg"] for p in reqs if p.get("arg")}) != 1:
            continue
        arg_key = str(reqs[0]["arg"])
        req_types = sorted({ _lower_strip(p["st"]) for p in reqs if p.get("st") })
        if not req_types: continue

        cand_cols: Set[Tuple[str,str]] = set()
        for t in req_types:
            cand_cols.update(cols_by_type.get(t, []))

        for table, base_col in sorted(cand_cols):
            sql = _sql_for_spec(template, arg_key, table, base_col)
            out.append(SQLSpec(func=fname, arg_key=arg_key, table=table, column=base_col, expression_sql=sql))
            if len(out) >= max_specs:
                return out

    driver.close()
    return out[:max_specs]

# ---------- Parse validation ----------
def _is_parseable(nl: str, vocab: Dict[str, Any], binder: Dict[str, Any], grammar_text: str) -> bool:
    try:
        rr = map_text(nl, vocab, binder, grammar_text, want_tree=False)
        return bool(rr.parse_ok)
    except Exception:
        return False

# ---------- Surface generation (predicate-aware) ----------
def surfaces_for_spec(
    vocab: Dict[str, Any],
    binder: Dict[str, Any],
    grammar_text: str,              # validate each candidate surface
    spec: SQLSpec,
    max_surfaces: int = MAX_SURFACES_PER_SPEC,
) -> List[str]:
    """
    Generate NL surfaces from aliases, optionally adding 0..2 validated predicates:
      "<select> <func_alias> <of> <column> <from> <table> [predicate] [(AND|OR) [NOT] predicate]"
    Clause actions are not used to generate surfaces (handled elsewhere).
    Only parseable candidates (under the current grammar) are returned.
    """
    # Safety guard for clause actions
    functions = _functions_from_binder(binder)
    fmeta = functions.get(spec.func) or {}
    if _action_kind_from_template(fmeta.get("template")) == "clause":
        return []

    selects = _first_aliases(_select_aliases_from_vocab(vocab), 2) or ["show"]
    raw_func_als = _first_aliases(_func_aliases_from_vocab(vocab, spec.func), 3) or [spec.func]
    func_als = [_trim_trailing_of(fa) for fa in raw_func_als]

    conns = _connectors_from_binder(binder)
    of_s   = conns.get("OF", "of")
    from_s = conns.get("FROM", "from")

    # Logical operator surfaces
    logical = _logical_aliases_from_vocab(vocab)
    and_s = (logical.get("and") or [conns.get("AND", "and")])[0]
    or_s  = (logical.get("or")  or [conns.get("OR", "or")])[0]
    not_s = (logical.get("not") or ["not"])[0]

    # Deterministic predicate pool
    pred_pool = _predicates_for_table(
        vocab=vocab,
        binder=binder,
        table=spec.table,
        max_predicates=MAX_PREDICATES_PER_SURFACE * 3
    )

    candidates_raw: List[str] = []
    for sel in selects:
        for fa in func_als:
            base = f"{sel} {fa} {of_s} {spec.column} {from_s} {spec.table}"
            candidates_raw.append(base)

            if pred_pool and MAX_PREDICATES_PER_SURFACE >= 1:
                p1_txt, _ = pred_pool[0]
                candidates_raw.append(f"{base} {p1_txt}")
                # NOT p1
                candidates_raw.append(f"{base} {not_s} {p1_txt}")

            if len(pred_pool) >= 2 and MAX_PREDICATES_PER_SURFACE >= 2:
                (p1_txt, _), (p2_txt, _) = pred_pool[0], pred_pool[1]
                # AND
                candidates_raw.append(f"{base} {p1_txt} {and_s} {p2_txt}")
                # OR
                candidates_raw.append(f"{base} {p1_txt} {or_s} {p2_txt}")
                # AND with NOT on second
                candidates_raw.append(f"{base} {p1_txt} {and_s} {not_s} {p2_txt}")

    # Dedup + parse-validate + trim to max_surfaces
    uniq: List[str] = []
    seen = set()
    for s in candidates_raw:
        k = s.strip().lower()
        if k in seen:
            continue
        if _is_parseable(s, vocab, binder, grammar_text):
            seen.add(k)
            uniq.append(s)
            if len(uniq) >= max_surfaces:
                break
    return uniq

# ---------- Resolver helpers (surface → slots with constraints) ----------
_VAL_RE = r"(?:'[^']*'|\d+(?:\.\d+)?)"

def _alias_regex_union(aliases: List[str]) -> str:
    parts = []
    for a in aliases:
        # turn "is greater than" -> r"is\s+greater\s+than"
        p = r"\s+".join(map(re.escape, a.strip().split()))
        parts.append(p)
    return "(?:" + "|".join(parts) + ")"

def _predicate_patterns_for_vocab(vocab: Dict[str, Any]) -> Dict[str, re.Pattern]:
    """Build compiled regex patterns per comparator canonical covering the shapes we emit."""
    comps = _comparator_aliases_from_vocab(vocab)
    pats: Dict[str, re.Pattern] = {}
    for canon, aliases in comps.items():
        alias_union = _alias_regex_union(aliases)
        if canon == "between":
            pat = re.compile(rf"^(?P<col>\w+)\s+{alias_union}\s+(?P<v1>{_VAL_RE})\s+and\s+(?P<v2>{_VAL_RE})\s*$", re.I)
        elif canon == "in":
            # support "x in v1, v2" (no parentheses)
            pat = re.compile(rf"^(?P<col>\w+)\s+{alias_union}\s+(?P<list>{_VAL_RE}(?:\s*,\s*{_VAL_RE})+)\s*$", re.I)
        elif canon in ("is_null", "is_not_null"):
            pat = re.compile(rf"^(?P<col>\w+)\s+{alias_union}\s*$", re.I)
        elif canon == "like":
            pat = re.compile(rf"^(?P<col>\w+)\s+{alias_union}\s+(?P<v>{_VAL_RE})\s*$", re.I)
        else:
            # single value
            pat = re.compile(rf"^(?P<col>\w+)\s+{alias_union}\s+(?P<v>{_VAL_RE})\s*$", re.I)
        pats[canon] = pat
    return pats

def _split_tail_by_logic(tail: str, vocab: Dict[str, Any]) -> List[Tuple[Optional[str], str]]:
    """
    Split a 'tail' like "price equals 'a' and not age > 5" into:
      [(None, "price equals 'a'"), ("and", "not age > 5")]
    """
    logical = _logical_aliases_from_vocab(vocab)
    and_alias = logical.get("and", ["and"])[0]
    or_alias  = logical.get("or",  ["or"])[0]

    # Greedy split on first AND/OR occurrence only twice (we cap at 2 preds)
    toks = re.split(rf"\s+({re.escape(and_alias)}|{re.escape(or_alias)})\s+", tail.strip(), maxsplit=1, flags=re.I)
    if len(toks) == 1:
        return [(None, toks[0])]
    if len(toks) == 3:
        return [(None, toks[0]), (toks[1].lower(), toks[2])]
    # Unexpected extra splits; fall back (shouldn’t happen with our synthesis)
    return [(None, tail.strip())]

def _strip_leading_not(s: str, vocab: Dict[str, Any]) -> Tuple[bool, str]:
    logical = _logical_aliases_from_vocab(vocab)
    not_alias = logical.get("not", ["not"])[0]
    s2 = s.strip()
    if re.match(rf"^{re.escape(not_alias)}\b", s2, flags=re.I):
        return True, s2[len(not_alias):].lstrip()
    return False, s2

def _parse_single_predicate(
    frag: str,
    table: str,
    vocab: Dict[str, Any],
    binder: Dict[str, Any],
    patterns: Dict[str, re.Pattern],
) -> Optional[Dict[str, Any]]:
    """
    Try to parse one predicate fragment into a constraint dict.
    """
    neg, body = _strip_leading_not(frag, vocab)
    for canon, pat in patterns.items():
        m = pat.match(body)
        if not m:
            continue
        col_base = (m.group("col") or "").strip()
        # Validate column belongs to table
        catalogs = binder.get("catalogs") or {}
        columns = catalogs.get("columns") or {}
        fqn = None
        for k, meta in (columns or {}).items():
            if not isinstance(meta, dict): continue
            if meta.get("table") == table and str(meta.get("name")).lower() == col_base.lower():
                fqn = f'{table}.{meta.get("name")}'
                break
        if not fqn:
            return None

        values: List[str] = []
        if canon == "between":
            values = [m.group("v1"), m.group("v2")]
        elif canon == "in":
            raw = m.group("list")
            values = [v.strip() for v in re.split(r"\s*,\s*", raw)]
        elif canon in ("is_null", "is_not_null"):
            values = []
        else:
            values = [m.group("v")]

        return {"column": fqn, "op": canon, "values": [v.strip("'") if v and v.startswith("'") and v.endswith("'") else v for v in values], "negated": bool(neg)}
    return None

def _extract_predicates_from_tail(
    tail: str,
    table: str,
    vocab: Dict[str, Any],
    binder: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """
    Convert the textual tail after 'from <table>' into constraints list.
    Preserves join order by setting 'join_next' on constraint 1 if two are present.
    """
    if not tail.strip():
        return []
    patterns = _predicate_patterns_for_vocab(vocab)
    parts = _split_tail_by_logic(tail, vocab)  # [(None, pred1), ('and'|'or', pred2)]
    constraints: List[Dict[str, Any]] = []
    for idx, (joiner, frag) in enumerate(parts):
        c = _parse_single_predicate(frag, table, vocab, binder, patterns)
        if not c:
            return []
        constraints.append(c)
        # set join_next on the first predicate only
        if idx == 0 and len(parts) > 1 and joiner:
            c["join_next"] = joiner.lower()
    return constraints[:2]  # safety cap

def _extract_base_parts(surface: str, vocab: Dict[str, Any], binder: Dict[str, Any]) -> Tuple[Optional[str], Optional[str], Optional[str], str]:
    """
    Return (func_canonical, table, column_base, tail_after_from_table).
    Tail may be empty string if no predicates were present.
    """
    # Lexicon & schema indices
    lexicon, _connectors = build_lexicon_and_connectors(vocab)
    by_len, max_len = build_index(lexicon)
    tables_by_lc, columns_by_lc, _coltypes = build_schema_indices(binder)

    toks = tokenize(surface)
    spans = match_aliases(toks, by_len, max_len)

    func: Optional[str] = None
    for s in spans:
        if s.role in ("sql_action", "postgis_action"):
            func = s.canonical
            break

    # crude table/column extraction from tokens
    table: Optional[str] = None
    col_base: Optional[str] = None
    for t in toks:
        if (not table) and t in tables_by_lc: table = tables_by_lc[t]
        if (not col_base) and t in columns_by_lc: col_base = columns_by_lc[t].split(".",1)[1]
    if not table:
        return None, None, None, ""

    # find "from <table>" tail (in original surface, case-insensitive)
    m = re.search(rf"\bfrom\s+{re.escape(table)}\b", surface, flags=re.I)
    tail = surface[m.end():].strip() if m else ""

    return func, table, col_base, tail

# ---------- Resolve NL surface -> SQL(s) ----------
def resolve_surface_to_sqls(
    surface: str,
    vocab: Dict[str, Any],
    binder: Dict[str, Any],
) -> List[str]:
    """
    Resolve NL surface to 0..N SQL strings using artifacts.
    Now emits:
      - projection-only SQL (baseline/spec)
      - constrained SQL if predicates were detected
    """
    func, table, col_base, tail = _extract_base_parts(surface, vocab, binder)
    if not (func and table):
        return []

    # Build baseline slots (projection only)
    slots_base: Dict[str, Any] = {
        "table": table,
        "columns": [f"{table}.{col_base}"] if col_base else [],
        "values": [],
        "actions": [func],
        # clause_actions optional
    }

    sqls: List[str] = []
    try:
        sqls.append(build_select_sql_from_slots(slots_base, binder_yaml=binder, limit=50))
    except Exception:
        pass

    # Try to parse predicates tail -> constraints
    constraints = _extract_predicates_from_tail(tail, table, vocab, binder)
    if constraints:
        slots_constrained = dict(slots_base)
        slots_constrained["constraints"] = constraints
        try:
            sql_constrained = build_select_sql_from_slots(slots_constrained, binder_yaml=binder, limit=50)
            if sql_constrained not in sqls:
                sqls.append(sql_constrained)
        except Exception:
            # If builder fails for constraints, we still return baseline
            pass

    return sqls

# ---------- Orchestration ----------
def generate_surfaces_and_classify(
    *,
    vocab_path: Path,
    binder_path: Path,
    out_gold: Path = DEFAULT_OUT_GOLD,
    out_multi: Path = DEFAULT_OUT_MULTI,
    out_invalid: Path = DEFAULT_OUT_INVALID,
    use_graph: bool = False,
    neo4j_uri: Optional[str] = None,
    neo4j_user: Optional[str] = None,
    neo4j_password: Optional[str] = None,
    neo4j_database: Optional[str] = None,
    max_specs: int = MAX_SPECS,
    max_surfaces_per_spec: int = MAX_SURFACES_PER_SPEC,
) -> Tuple[int, int, int]:
    """
    Returns counts: (n_gold, n_multi, n_invalid).
    """
    vocab = must_load_yaml(vocab_path)
    binder = must_load_yaml(binder_path)
    grammar_text = must_load_text(ART_DIR / "graph_grammar.lark")

    # 1) Build SQL targets (specs)
    if use_graph:
        specs = specs_from_graph(
            uri=neo4j_uri or os.environ.get("NEO4J_URI", "bolt://localhost:7687"),
            user=neo4j_user or os.environ.get("NEO4J_USER", "neo4j"),
            password=neo4j_password or os.environ.get("NEO4J_PASSWORD", "test"),
            database=neo4j_database or os.environ.get("NEO4J_DATABASE") or None,
            max_specs=max_specs,
        )
    else:
        specs = specs_from_artifacts(binder, max_specs=max_specs)

    if not specs:
        print("No specs generated; check artifacts/graph.", file=sys.stderr)
        return (0,0,0)

    # 2) For each SQLSpec, generate NL surfaces and resolve back
    gold_rows: List[Dict[str, str]] = []
    multi_rows: List[Dict[str, Any]] = []
    invalid_rows: List[Dict[str, Any]] = []

    seen_nl: Set[str] = set()

    for spec in specs:
        target_norm = _normalize_sql(spec.expression_sql)
        surfaces = surfaces_for_spec(
            vocab=vocab,
            binder=binder,
            grammar_text=grammar_text,
            spec=spec,
            max_surfaces=max_surfaces_per_spec,
        )
        for nl in surfaces:
            nl_key = nl.strip().lower()
            if nl_key in seen_nl:
                continue
            seen_nl.add(nl_key)

            resolved = resolve_surface_to_sqls(nl, vocab, binder)
            resolved_norm = [_normalize_sql(s) for s in resolved]
            resolved_norm_uniq = sorted({r for r in resolved_norm})

            if target_norm in resolved_norm_uniq:
                # If there's also a constrained SQL, classify as multipath; else gold.
                others = [s for s in resolved if _normalize_sql(s) != target_norm]
                if not others:
                    gold_rows.append({"natural_language": nl, "sql_expression": spec.expression_sql})
                else:
                    multi_rows.append({
                        "natural_language": nl,
                        "original_sql": spec.expression_sql,
                        "sql_expressions": others,
                    })
            else:
                invalid_rows.append({
                    "natural_language": nl,
                    "original_sql": spec.expression_sql,
                    "sql_expressions": resolved,
                })

    # 3) Write YAML outputs
    out_gold.parent.mkdir(parents=True, exist_ok=True)
    out_multi.parent.mkdir(parents=True, exist_ok=True)
    out_invalid.parent.mkdir(parents=True, exist_ok=True)

    with open(out_gold, "w", encoding="utf-8") as f:
        yaml.safe_dump(gold_rows, f, sort_keys=False, allow_unicode=True)
    with open(out_multi, "w", encoding="utf-8") as f:
        yaml.safe_dump(multi_rows, f, sort_keys=False, allow_unicode=True)
    with open(out_invalid, "w", encoding="utf-8") as f:
        yaml.safe_dump(invalid_rows, f, sort_keys=False, allow_unicode=True)

    print(f"Wrote {len(gold_rows)} gold surfaces -> {out_gold}")
    print(f"Wrote {len(multi_rows)} valid multipath -> {out_multi}")
    print(f"Wrote {len(invalid_rows)} invalid surfaces -> {out_invalid}")
    return (len(gold_rows), len(multi_rows), len(invalid_rows))

# ---------- CLI ----------
def main(argv: Optional[List[str]] = None) -> int:
    argv = argv or sys.argv[1:]

    import argparse
    p = argparse.ArgumentParser(
        description="Synthesize NL surfaces from SQL targets (via artifacts or graph), and classify by reversibility.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--artifacts-dir", default=str(ART_DIR), help="directory where graph_vocabulary.yaml & graph_binder.yaml live")
    p.add_argument("--use-graph", action="store_true", help="if set, enumerate specs from Neo4j instead of artifacts")
    p.add_argument("--neo4j-uri", default=os.environ.get("NEO4J_URI", "bolt://localhost:7687"))
    p.add_argument("--neo4j-user", default=os.environ.get("NEO4J_USER", "neo4j"))
    p.add_argument("--neo4j-password", default=os.environ.get("NEO4J_PASSWORD", "test"))
    p.add_argument("--neo4j-database", default=os.environ.get("NEO4J_DATABASE") or None)
    p.add_argument("--max-specs", type=int, default=MAX_SPECS, help="max number of SQL specs to seed surfaces from")
    p.add_argument("--max-surfaces-per-spec", type=int, default=MAX_SURFACES_PER_SPEC)
    p.add_argument("--out-gold", default=str(DEFAULT_OUT_GOLD))
    p.add_argument("--out-multipath", default=str(DEFAULT_OUT_MULTI))
    p.add_argument("--out-invalid", default=str(DEFAULT_OUT_INVALID))

    args = p.parse_args(argv)

    art_dir = Path(args.artifacts_dir)
    vocab_path = art_dir / "graph_vocabulary.yaml"
    binder_path = art_dir / "graph_binder.yaml"
    if not vocab_path.exists() or not binder_path.exists():
        print(f"Missing artifacts in {art_dir}. Run graph_to_artifacts.py first.", file=sys.stderr)
        return 2

    generate_surfaces_and_classify(
        vocab_path=vocab_path,
        binder_path=binder_path,
        out_gold=Path(args.out_gold),
        out_multi=Path(args.out_multipath),
        out_invalid=Path(args.out_invalid),
        use_graph=bool(args.use_graph),
        neo4j_uri=args.neo4j_uri,
        neo4j_user=args.neo4j_user,
        neo4j_password=args.neo4j_password,
        neo4j_database=args.neo4j_database,
        max_specs=int(args.max_specs),
        max_surfaces_per_spec=int(args.max_surfaces_per_spec),
    )
    generate_cli_test(
        art_dir=ART_DIR,
        gold_path=Path(args.out_gold),     # defaults to out/gold_surfaces.yml
        out_path=None,                     # defaults to out/cli_test.sh
        template_path=None,                # or pass a custom Jinja template file
        max_items=None                     # or cap e.g., 100
    )
    return 0

if __name__ == "__main__":
    sys.exit(main())
