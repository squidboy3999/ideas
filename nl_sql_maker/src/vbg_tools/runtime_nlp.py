#!/usr/bin/env python3
# vbg_tools/runtime_nlp.py
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

# ----------------- tokenization -----------------
_OPERATOR_PAT = r"(>=|<=|!=|==|<>|=|>|<)"
_PUNCT_PAT    = r"(\(|\)|,|%)"
_WORD_PAT     = r"([A-Za-z0-9_]+)"
TOKEN_RE = re.compile(rf"{_OPERATOR_PAT}|{_PUNCT_PAT}|{_WORD_PAT}")

def tokenize(text: str) -> List[str]:
    s = (text or "").strip().lower()
    if not s:
        return []
    return [m.group(0) for m in TOKEN_RE.finditer(s)]

def is_number(tok: str) -> bool:
    return bool(re.fullmatch(r"\d+(\.\d+)?", tok))

def is_quoted_string(raw: str) -> Optional[str]:
    raw = raw.strip()
    if len(raw) >= 2 and ((raw[0] == raw[-1] == '"') or (raw[0] == raw[-1] == "'")):
        return raw[1:-1]
    return None

# ----------------- data structures -----------------
@dataclass
class LexEntry:
    tokens: Tuple[str, ...]
    canonical: str
    role: Optional[str]
    surface: str

@dataclass
class MatchSpan:
    start: int
    end: int
    canonical: str
    role: Optional[str]
    surface: str

# ----------------- vocabulary loader -----------------
KW_SECTIONS_WITH_ALIASES = [
    ("select_verbs", "select_verb", True),
    ("prepositions", "preposition", False),
    ("logical_operators", "logical", False),
    ("comparison_operators", "comparator", False),
    ("filler_words", "filler", False),
    ("connectors", "connector", False),
]
ACTION_SECTIONS_WITH_ALIASES = [
    ("sql_actions", "sql_action"),
    ("postgis_actions", "postgis_action"),
]

def _to_alias_list(ent: Any) -> List[str]:
    if isinstance(ent, dict):
        aliases = ent.get("aliases")
        if aliases is None:
            surf = ent.get("surface")
            return [surf] if isinstance(surf, str) else []
        if isinstance(aliases, str):
            return [aliases]
        if isinstance(aliases, list):
            return [a for a in aliases if isinstance(a, str)]
        return []
    if isinstance(ent, list):
        return [a for a in ent if isinstance(a, str)]
    if isinstance(ent, str):
        return [ent]
    return []

def _is_clause_template(t: Any) -> bool:
    if not isinstance(t, str):
        return False
    s = re.sub(r"\s+", " ", t).strip().upper()
    return s.startswith("LIMIT") or s.startswith("ORDER BY") or s.startswith("GROUP BY") or s.startswith("HAVING")

def build_lexicon_and_connectors(vocab_yaml: Dict[str, Any]) -> Tuple[List[LexEntry], Dict[str, str]]:
    """
    Build the lexicon from vocabulary.
    - comparison_operators -> role 'comparator'
    - logical_operators    -> role 'logical'
    - sql/postgis actions  -> role 'sql_action' unless template is a clause, then 'clause_action'
    """
    lexicon: List[LexEntry] = []
    connectors_map: Dict[str, str] = {}

    kw = (vocab_yaml.get("keywords") or {})

    for section, role, is_nested in KW_SECTIONS_WITH_ALIASES:
        sec = kw.get(section) or {}
        if not isinstance(sec, dict):
            continue
        if section == "connectors":
            for cname, v in sec.items():
                can = str(cname)
                surface = v.get("surface") if isinstance(v, dict) else v
                if isinstance(surface, str) and surface.strip():
                    connectors_map[can] = surface.strip()
                    toks = tuple(tokenize(surface))
                    if toks:
                        lexicon.append(LexEntry(tokens=toks, canonical=can, role="connector", surface=surface))
        elif is_nested:
            for canonical, ent in sec.items():
                can = str(canonical)
                for surf in _to_alias_list(ent):
                    toks = tuple(tokenize(surf))
                    if toks:
                        lexicon.append(LexEntry(tokens=toks, canonical=can, role=role, surface=surf))
        else:
            for canonical, ent in sec.items():
                can = str(canonical)
                for surf in _to_alias_list(ent):
                    toks = tuple(tokenize(surf))
                    if toks:
                        lexicon.append(LexEntry(tokens=toks, canonical=can, role=role, surface=surf))

    # actions
    for section, default_role in ACTION_SECTIONS_WITH_ALIASES:
        sec = (vocab_yaml.get(section) or {})
        if not isinstance(sec, dict):
            continue
        for canonical, ent in sec.items():
            can = str(canonical)
            tmpl = ent.get("template") if isinstance(ent, dict) else None
            role_here = "clause_action" if _is_clause_template(tmpl) else default_role
            for surf in _to_alias_list(ent):
                toks = tuple(tokenize(surf))
                if toks:
                    lexicon.append(LexEntry(tokens=toks, canonical=can, role=role_here, surface=surf))

    return lexicon, connectors_map

# ----------------- binder loader -----------------
def iter_tables(tbls: Any):
    if isinstance(tbls, dict):
        for tname in tbls.keys():
            yield str(tname)
    elif isinstance(tbls, list):
        for tname in tbls:
            yield str(tname)

def infer_column_types(cinfo: Dict[str, Any], col_name: str) -> List[str]:
    """Map int/float/decimal → numeric unless 'id' is present in name/labels."""
    want: List[str] = []
    sl = cinfo.get("slot_types")
    if isinstance(sl, list):
        want.extend([str(x) for x in sl if isinstance(x, (str, int, float, bool))])
    hints: List[str] = []
    for k in ("types", "labels"):
        v = cinfo.get(k)
        if isinstance(v, list):
            hints.extend([str(x) for x in v if isinstance(x, (str, int, float, bool))])
    for k in ("type", "data_type", "category"):
        v = cinfo.get(k)
        if isinstance(v, str):
            hints.append(v)

    hints_norm = [x.strip().lower().replace(" ", "_") for x in hints]
    name_norm = (col_name or "").lower()
    looks_id = ("id" in name_norm) or any(h.lower() == "id" for h in hints)

    mapped: List[str] = []
    for h in hints_norm:
        if (not looks_id) and h in ("int","integer","bigint","smallint","float","double","real","decimal","numeric"):
            mapped.append("numeric")
        else:
            mapped.append(h)

    if looks_id:
        mapped.append("id")

    all_types = set([t.strip().lower().replace(" ", "_") for t in want + mapped if isinstance(t, str)])
    return sorted(all_types)

def build_schema_indices(binder_yaml: Dict[str, Any]) -> Tuple[Dict[str, str], Dict[str, str], Dict[str, List[str]]]:
    catalogs = (binder_yaml.get("catalogs") or {})
    tables = catalogs.get("tables") or {}
    columns = catalogs.get("columns") or {}

    tables_by_lc: Dict[str, str] = {t.lower(): t for t in iter_tables(tables)}
    columns_by_lc: Dict[str, str] = {}
    column_types: Dict[str, List[str]] = {}

    for k, cinfo in (columns or {}).items():
        if not isinstance(cinfo, dict):
            continue
        if "." in k:
            tname, base = k.split(".", 1)
            table = cinfo.get("table") or tname
            base_col = base
        else:
            table = cinfo.get("table") or ""
            base_col = k
        fqn = f"{table}.{base_col}" if table else base_col
        columns_by_lc[base_col.lower()] = fqn
        column_types[fqn] = infer_column_types(cinfo, base_col)
    return tables_by_lc, columns_by_lc, column_types

# ----------------- greedy matcher -----------------
def build_index(lexicon: List[LexEntry]) -> Tuple[Dict[int, Dict[Tuple[str, ...], List[LexEntry]]], int]:
    by_len: Dict[int, Dict[Tuple[str, ...], List[LexEntry]]] = {}
    max_len = 1
    for le in lexicon:
        L = len(le.tokens)
        if L == 0: continue
        max_len = max(max_len, L)
        by_len.setdefault(L, {}).setdefault(le.tokens, []).append(le)
    return by_len, max_len

ROLE_PRIORITY = {
    "connector": 5,
    "comparator": 4,
    "sql_action": 4,
    "postgis_action": 4,
    "clause_action": 4,
    "select_verb": 3,
    "preposition": 2,
    "filler": 1,
    None: 0,
}

def choose_best(entries: List[LexEntry]) -> LexEntry:
    best = entries[0]
    best_score = (ROLE_PRIORITY.get(best.role, 0), best.canonical.isupper())
    for e in entries[1:]:
        score = (ROLE_PRIORITY.get(e.role, 0), e.canonical.isupper())
        if score > best_score:
            best, best_score = e, score
    return best

def match_aliases(tokens: List[str], by_len: Dict[int, Dict[Tuple[str, ...], List[LexEntry]]], max_len: int) -> List[MatchSpan]:
    i = 0
    spans: List[MatchSpan] = []
    n = len(tokens)
    while i < n:
        chosen: Optional[MatchSpan] = None
        for L in range(min(max_len, n - i), 0, -1):
            window = tuple(tokens[i: i + L])
            cand = by_len.get(L, {}).get(window)
            if cand:
                best = choose_best(cand)
                chosen = MatchSpan(start=i, end=i + L, canonical=best.canonical, role=best.role, surface=best.surface)
                break
        if chosen:
            spans.append(chosen)
            i = chosen.end
        else:
            i += 1
    return spans

# ----------------- harvesting helpers -----------------
def extract_literal_values(raw: str, tokens: List[str]) -> List[str]:
    """Collect quoted strings (unquoted) + numeric tokens in surface order."""
    values: List[str] = []
    for m in re.finditer(r"(['\"]).*?\1", raw):
        inner = is_quoted_string(m.group(0))
        if inner is not None:
            values.append(inner)
    for t in tokens:
        if is_number(t):
            values.append(t)
    return values

def gather_tables_columns(tokens: List[str],
                          tables_by_lc: Dict[str, str],
                          columns_by_lc: Dict[str, str]) -> Tuple[List[str], List[str]]:
    found_tables: List[str] = []
    found_columns: List[str] = []
    for t in tokens:
        if t in tables_by_lc:
            found_tables.append(tables_by_lc[t])
        if t in columns_by_lc:
            found_columns.append(columns_by_lc[t])  # FQN
    return found_tables, found_columns

def collect_actions(spans: List[MatchSpan]) -> Tuple[List[str], List[str], List[str]]:
    """Return (projection_or_postgis actions, clause_actions, comparators)."""
    proj: List[str] = []
    clause: List[str] = []
    comparators: List[str] = []
    for s in spans:
        if s.role in ("sql_action", "postgis_action"):
            proj.append(s.canonical)
        elif s.role == "clause_action":
            clause.append(s.canonical)
        elif s.role == "comparator":
            comparators.append(s.canonical)
    return proj, clause, comparators

def nearest_column_fqn(before_idx: int,
                       tokens: List[str],
                       columns_by_lc: Dict[str, str],
                       default_table: Optional[str]) -> Optional[str]:
    """
    Find the nearest column mention to the left of a given token index.
    If the token is a base column not seen with a table, bind to default_table when available.
    """
    j = before_idx - 1
    while j >= 0:
        tok = tokens[j]
        if tok in columns_by_lc:
            fqn = columns_by_lc[tok]
            if "." in fqn or not default_table:
                return fqn
            return f"{default_table}.{tok}"
        j -= 1
    return None

def harvest_constraints(raw: str,
                        tokens: List[str],
                        spans: List[MatchSpan],
                        columns_by_lc: Dict[str, str],
                        default_table: Optional[str]) -> Tuple[List[Dict[str, Any]], List[str]]:
    """
    Build constraints from comparator spans and nearby values.
    Returns (constraints, warnings)
    """
    warnings: List[str] = []
    values_stream = extract_literal_values(raw, tokens)
    val_idx = 0

    # NOTE: normalize logical canonicals to lower-case for robust checks
    logic_by_pos: Dict[int, str] = {}
    for s in spans:
        if s.role == "logical":
            logic_by_pos[s.start] = s.canonical.lower()

    constraints: List[Dict[str, Any]] = []

    for s in spans:
        if s.role != "comparator":
            continue

        col_fqn = nearest_column_fqn(s.start, tokens, columns_by_lc, default_table)
        if not col_fqn:
            warnings.append(f"Comparator '{s.canonical}' without a resolvable column near token {s.start}")
            continue

        op = s.canonical
        want_vals = 0
        if op in ("is_null", "is_not_null"):
            want_vals = 0
        elif op in ("between",):
            want_vals = 2
        elif op in ("in",):
            want_vals = 1  # minimum; lists will be honored if there are more literals later
        else:
            want_vals = 1

        vals: List[str] = []
        for _ in range(want_vals):
            if val_idx < len(values_stream):
                vals.append(values_stream[val_idx])
                val_idx += 1
        if len(vals) < want_vals:
            warnings.append(f"Insufficient values for comparator '{op}' (needed {want_vals}, got {len(vals)})")

        negated = False
        if (s.start - 1) in logic_by_pos and logic_by_pos[s.start - 1] == "not":
            negated = True

        constraints.append({
            "column": col_fqn,
            "op": op,
            "values": vals,
            "negated": negated,
        })

        # logical join between predicates (attach to previous)
        if len(constraints) >= 2:
            if (s.start - 1) in logic_by_pos and logic_by_pos[s.start - 1] in ("and", "or"):
                constraints[-2]["join_next"] = logic_by_pos[s.start - 1]

    return constraints, warnings
