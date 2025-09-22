# vbg_tools/runtime_nlp.py
from __future__ import annotations

import re
from typing import Any, Dict, Iterable, List, Tuple, Optional
from dataclasses import dataclass

_VAL_RE = r"(?:'[^']*'|\d+(?:\.\d+)?)"  # quoted string or number

# --------------------------------------------------------------------------------------
# Public data shapes expected by graph_runtime.py and tests
# --------------------------------------------------------------------------------------

@dataclass(frozen=True)
class LexEntry:
    tokens: Tuple[str, ...]   # alias tokens (lowercased)
    canonical: str            # canonical key (e.g., "select", "count", "greater_than", "AND")
    role: str                 # e.g., "select_verb", "sql_action", "clause_action", "comparator", "connector"
    surface: str              # original surface phrase


@dataclass(frozen=True)
class MatchSpan:
    start: int               # inclusive start index in tokenized NL
    end: int                 # exclusive end index
    canonical: str           # canonical symbol matched (e.g., "select", "count", "AND")
    role: str                # role from LexEntry
    surface: str             # surface matched

# ---------------------------
# Misc Helpers
# ---------------------------
def _alias_regex_union(aliases: List[str]) -> str:
    parts = []
    for a in aliases:
        a = str(a).strip()
        if not a:
            continue
        parts.append(r"\s+".join(map(re.escape, a.split())))
    return "(?:" + "|".join(parts) + ")" if parts else "(?!)"

def _predicate_patterns_for_vocab(vocab: Dict[str, Any]) -> Dict[str, re.Pattern]:
    """
    Build compiled regex for each comparator using its aliases.
    Supports: between, in, like, is_null, is_not_null, and binary comparators.
    """
    comps = ((vocab.get("keywords") or {}).get("comparison_operators") or {})
    pats: Dict[str, re.Pattern] = {}
    for canon, ent in (comps or {}).items():
        als = ent.get("aliases") or []
        aliases = [a for a in als if isinstance(a, str) and a.strip()]
        if not aliases:
            continue
        union = _alias_regex_union(aliases)
        if canon == "between":
            pat = re.compile(rf"^(?P<col>\w+)\s+{union}\s+(?P<v1>{_VAL_RE})\s+and\s+(?P<v2>{_VAL_RE})\s*$", re.I)
        elif canon == "in":
            pat = re.compile(rf"^(?P<col>\w+)\s+{union}\s+(?P<list>{_VAL_RE}(?:\s*,\s*{_VAL_RE})+)\s*$", re.I)
        elif canon in ("is_null", "is_not_null"):
            pat = re.compile(rf"^(?P<col>\w+)\s+{union}\s*$", re.I)
        elif canon == "like":
            pat = re.compile(rf"^(?P<col>\w+)\s+{union}\s+(?P<v>{_VAL_RE})\s*$", re.I)
        else:
            pat = re.compile(rf"^(?P<col>\w+)\s+{union}\s+(?P<v>{_VAL_RE})\s*$", re.I)
        pats[canon] = pat
    return pats

def _strip_leading_not(s: str, vocab: Dict[str, Any]) -> Tuple[bool, str]:
    logical = ((vocab.get("keywords") or {}).get("logical_operators") or {})
    not_aliases = (logical.get("not") or {}).get("aliases") or ["not"]
    not_union = _alias_regex_union(not_aliases)
    s2 = s.strip()
    m = re.match(rf"^{not_union}\b", s2, flags=re.I)
    if m:
        return True, s2[m.end():].lstrip()
    return False, s2

def _split_tail_by_logic(tail: str, vocab: Dict[str, Any]) -> List[Tuple[Optional[str], str]]:
    """Split on the first logical joiner (AND/OR); returns list of (joiner, fragment)."""
    logical = ((vocab.get("keywords") or {}).get("logical_operators") or {})
    and_aliases = (logical.get("and") or {}).get("aliases") or ["and"]
    or_aliases  = (logical.get("or")  or {}).get("aliases") or ["or"]
    and_union = _alias_regex_union(and_aliases)
    or_union  = _alias_regex_union(or_aliases)
    toks = re.split(rf"\s+({and_union}|{or_union})\s+", tail.strip(), maxsplit=1, flags=re.I)
    if len(toks) == 1:
        return [(None, toks[0])]
    if len(toks) == 3:
        return [(None, toks[0]), (toks[1].strip().lower(), toks[2])]
    return [(None, tail.strip())]

def _parse_single_predicate(
    frag: str,
    table: str,
    vocab: Dict[str, Any],
    binder: Dict[str, Any],
    patterns: Dict[str, re.Pattern],
) -> Optional[Dict[str, Any]]:
    neg, body = _strip_leading_not(frag, vocab)
    catalogs = binder.get("catalogs") or {}
    columns  = catalogs.get("columns") or {}

    def _fqn_for_base(base: str) -> Optional[str]:
        base_lc = base.lower()
        for _k, meta in (columns or {}).items():
            if not isinstance(meta, dict):
                continue
            if (meta.get("table") or "").lower() == table.lower() and str(meta.get("name") or "").lower() == base_lc:
                return f"{meta.get('table')}.{meta.get('name')}"
        return None

    for canon, pat in patterns.items():
        m = pat.match(body)
        if not m:
            continue
        col_base = (m.group("col") or "").strip()
        fqn = _fqn_for_base(col_base)
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
        elif canon == "like":
            values = [m.group("v")]
        else:
            values = [m.group("v")]

        norm_vals = []
        for v in values:
            if isinstance(v, str) and len(v) >= 2 and v.startswith("'") and v.endswith("'"):
                norm_vals.append(v[1:-1])
            else:
                norm_vals.append(v)

        return {"column": fqn, "op": canon, "values": norm_vals, "negated": bool(neg)}
    return None
# --------------------------------------------------------------------------------------
# Basic NLP utilities (public API)
# --------------------------------------------------------------------------------------

_TOKEN_SPLIT_RE = re.compile(r"[^\w']+")  # split on non-word except apostrophes

def tokenize(text: str) -> List[str]:
    """
    Lowercase tokenizer that separates commas/most punctuation as standalone tokens.
    Keeps simple apostrophes inside words (e.g., don't).
    Examples:
      "Show me users, please" -> ["show","me","users",",","please"]
    """
    if not text:
        return []
    # Preserve commas explicitly, split others
    text = text.replace(",", " , ")
    parts = []
    for chunk in text.split():
        if chunk == ",":
            parts.append(",")
            continue
        # split further on non-word (except ')
        subs = [s for s in _TOKEN_SPLIT_RE.split(chunk) if s]
        if not subs:
            continue
        parts.extend(subs)
    return [p.lower() for p in parts if p]


_NUM_RE = re.compile(r"^[+-]?\d+(\.\d+)?$")

def is_number(s: str) -> bool:
    return bool(_NUM_RE.match(s.strip())) if isinstance(s, str) else False


def is_quoted_string(s: str) -> str | None:
    """
    Returns the inner text if s is a single- or double-quoted string; else None.
    """
    if not isinstance(s, str) or len(s) < 2:
        return None
    if (s[0] == s[-1]) and s[0] in ("'", '"'):
        inner = s[1:-1]
        # simple unescape for doubled quotes of same kind
        inner = inner.replace(s[0]*2, s[0])
        return inner
    return None


# --------------------------------------------------------------------------------------
# Vocabulary / connectors → Lexicon
# --------------------------------------------------------------------------------------

CORE_CONNECTORS = {
    "AND": "and",
    "OR": "or",
    "NOT": "not",
    "FROM": "from",
    "OF": "of",
    "COMMA": ",",
}

def _norm_str(s: Any) -> str:
    return str(s).strip()

def _normalize_aliases(aliases: Iterable[Any]) -> List[str]:
    out: List[str] = []
    for a in aliases or []:
        t = _norm_str(a)
        if t:
            out.append(t)
    # de-dup, preserve order case-insensitively
    seen = set()
    uniq = []
    for a in out:
        al = a.lower()
        if al in seen:
            continue
        seen.add(al)
        uniq.append(a)
    return uniq

def _alias_tokens(phrase: str) -> Tuple[str, ...]:
    return tuple(tokenize(_norm_str(phrase)))

def _is_clause_action(name: str, meta: Dict[str, Any]) -> bool:
    placement = (meta or {}).get("placement") or ""
    if str(placement).lower() == "clause":
        return True
    n = (name or "").lower()
    if n.startswith("order_by") or n in {"group_by", "having", "limit", "limit_one"}:
        return True
    return False

def _collect_actions(vocab: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """
    New contract: actions live at top-level sql_actions.
    If legacy keywords.sql_actions exists, merge (top-level wins).
    """
    actions: Dict[str, Dict[str, Any]] = {}
    top = (vocab.get("sql_actions") or {})
    if isinstance(top, dict):
        actions.update(top)
    kw = (vocab.get("keywords") or {})
    legacy = (kw.get("sql_actions") or {})
    if isinstance(legacy, dict):
        for k, v in legacy.items():
            actions.setdefault(k, v)
    return actions

def build_lexicon_and_connectors(vocabulary: Dict[str, Any]):
    """
    Build (lexicon, connectors_map).

    - Select verbs: role='select_verb', canonical='select'
    - Comparison operators: role='comparator', canonical=<name>
    - Actions: role='sql_action' unless placement=clause (or heuristic) → 'clause_action'
    - Connectors: ensure core present; also add them as lexicon entries with role='connector'
    """
    kw = (vocabulary.get("keywords") or {})

    # Connectors map with core fallbacks
    connectors_map: Dict[str, str] = dict(CORE_CONNECTORS)
    provided = (kw.get("connectors") or kw.get("CONNECTORS") or {})
    if isinstance(provided, dict):
        for k, v in provided.items():
            connectors_map[str(k).upper()] = _norm_str(v) or connectors_map.get(str(k).upper(), "")

    lex: List[LexEntry] = []

    # Select verbs
    for _, meta in (kw.get("select_verbs") or {}).items():
        aliases = _normalize_aliases((meta or {}).get("aliases") or [])
        for a in aliases:
            lex.append(LexEntry(tokens=_alias_tokens(a), canonical="select", role="select_verb", surface=a))

    # Comparison operators
    for can, meta in (kw.get("comparison_operators") or {}).items():
        aliases = _normalize_aliases((meta or {}).get("aliases") or [])
        for a in aliases:
            lex.append(LexEntry(tokens=_alias_tokens(a), canonical=str(can), role="comparator", surface=a))

    # Actions (top-level, merged with legacy)
    actions = _collect_actions(vocabulary)
    for name, meta in (actions or {}).items():
        aliases = _normalize_aliases((meta or {}).get("aliases") or [])
        role = "clause_action" if _is_clause_action(name, meta or {}) else "sql_action"
        if not aliases:
            aliases = [name]
        for a in aliases:
            lex.append(LexEntry(tokens=_alias_tokens(a), canonical=str(name), role=role, surface=a))

    # Connectors as lexicon entries (to match like other tokens)
    for cname, surf in connectors_map.items():
        if not surf:
            continue
        lex.append(LexEntry(tokens=_alias_tokens(surf), canonical=cname, role="connector", surface=surf))

    return lex, connectors_map


# --------------------------------------------------------------------------------------
# Schema helpers (public API)
# --------------------------------------------------------------------------------------

_NUMERIC_TYPES = {"int", "integer", "bigint", "smallint", "decimal", "numeric", "float", "double", "real"}
_TEXT_TYPES = {"text", "varchar", "char", "character varying", "string"}
_DATE_TYPES = {"date"}
_TIME_TYPES = {"timestamp", "timestamptz", "time", "datetime"}

def infer_column_types(cinfo: Dict[str, Any], colname: str) -> List[str]:
    """
    Infer slot types from DB type + column name.

    Rules:
    - Always include the base DB type (lowercased) when present (e.g., "integer").
    - If the column name is ID-like ("id" or endswith "_id"):
        * include "id"
        * DO NOT add the broad "numeric" slot type even if the DB type is numeric.
    - Otherwise:
        * add "numeric" for numeric DB types
        * add "text"   for text DB types
        * add "date"   for date/time types (basic coarse tag)
    """
    stypes: List[str] = []

    # Base DB type
    t = (cinfo or {}).get("type")
    t_l = t.lower() if isinstance(t, str) and t else None
    if t_l:
        stypes.append(t_l)

    # Column-name heuristic
    n = (colname or "").lower()
    is_id_like = (n == "id") or n.endswith("_id")

    # Type-derived tags (guarded by id policy)
    if t_l:
        # numeric?
        if any(x in t_l for x in _NUMERIC_TYPES):
            if not is_id_like:
                if "numeric" not in stypes:
                    stypes.append("numeric")
        # text?
        if any(x in t_l for x in _TEXT_TYPES):
            if "text" not in stypes:
                stypes.append("text")
        # date/time? (coarse)
        if any(x in t_l for x in (_DATE_TYPES | _TIME_TYPES)):
            if "date" not in stypes and "timestamp" not in stypes:
                stypes.append("date")

    # ID-like adds 'id' but not 'numeric'
    if is_id_like and "id" not in stypes:
        stypes.append("id")

    # Dedup preserve order
    seen = set()
    out: List[str] = []
    for s in stypes:
        if s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out


def build_schema_indices(binder_artifact: Dict[str, Any]):
    """
    Returns (tables_by_lc, columns_by_lc, types_by_fqn).
      - tables_by_lc:  "users" -> "users"
      - columns_by_lc: "age" -> "users.age" (first seen wins if ambiguous)
      - types_by_fqn:  "users.age" -> ["integer","numeric",...]
    """
    cats = (binder_artifact.get("catalogs") or {})
    tables = (cats.get("tables") or {})
    columns = (cats.get("columns") or {})

    tables_by_lc: Dict[str, str] = {}
    for tname in tables.keys():
        tl = str(tname).lower()
        tables_by_lc.setdefault(tl, str(tname))

    columns_by_lc: Dict[str, str] = {}
    types_by_fqn: Dict[str, List[str]] = {}

    for fqn, meta in columns.items():
        fqn_str = str(fqn)
        base = fqn_str.split(".")[-1]
        base_lc = base.lower()
        columns_by_lc.setdefault(base_lc, fqn_str)

        # types
        slot_types = (meta or {}).get("slot_types") or []
        if not isinstance(slot_types, list) or not slot_types:
            slot_types = infer_column_types(meta or {}, base)
        else:
            slot_types = [str(s) for s in slot_types if str(s).strip()]
        types_by_fqn[fqn_str] = slot_types

    return tables_by_lc, columns_by_lc, types_by_fqn

def gather_tables_columns(binder_artifact: Dict[str, Any]):
    """
    Convenience: return (list_of_tables, list_of_column_fqns)
    """
    cats = (binder_artifact.get("catalogs") or {})
    tables = list((cats.get("tables") or {}).keys())
    columns = list((cats.get("columns") or {}).keys())
    return tables, columns


# --------------------------------------------------------------------------------------
# N-gram index & greedy matcher (public API)
# --------------------------------------------------------------------------------------

def build_index(lexicon: List[LexEntry]):
    """
    Build an index keyed by n-gram length for greedy matching.
    Returns (by_len, max_len) where by_len[length] -> list[LexEntry].
    """
    by_len: Dict[int, List[LexEntry]] = {}
    max_len = 1
    for le in lexicon:
        L = len(le.tokens)
        if L <= 0:
            continue
        by_len.setdefault(L, []).append(le)
        if L > max_len:
            max_len = L
    return by_len, max_len

def match_aliases(tokens: List[str], by_len: Dict[int, List[LexEntry]], max_len: int) -> List[MatchSpan]:
    """
    Greedy, left-to-right longest-match for n-gram aliases.
    Non-overlapping by construction.
    """
    spans: List[MatchSpan] = []
    i = 0
    N = len(tokens)
    while i < N:
        matched = False
        # try longest first
        for L in range(min(max_len, N - i), 0, -1):
            window = tuple(tokens[i:i+L])
            entries = by_len.get(L, [])
            # exact token tuple match
            for le in entries:
                if window == le.tokens:
                    spans.append(MatchSpan(start=i, end=i+L, canonical=le.canonical, role=le.role, surface=" ".join(le.tokens)))
                    i += L
                    matched = True
                    break
            if matched:
                break
        if not matched:
            i += 1
    return spans


# --------------------------------------------------------------------------------------
# Actions & constraints helpers (public API)
# --------------------------------------------------------------------------------------

def collect_actions(vocabulary: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """
    Return a normalized mapping of action name -> metadata.
    (Top-level sql_actions merged with legacy if present.)
    """
    return _collect_actions(vocabulary)

def harvest_constraints(
    source_text: str,
    table: Optional[str],
    vocabulary: Dict[str, Any],
    binder: Dict[str, Any],
    *,
    max_predicates: int = 2
) -> Tuple[List[Dict[str, Any]], List[str]]:
    """
    Parse constraints from the NL tail after 'FROM <table>'.
    Returns (constraints, warnings).
    Strategy:
      1) Find tail after FROM <table>.
      2) Try parse whole tail as ONE predicate (handles 'between ... and ...').
      3) If that fails, try split once on AND/OR (outside the 'between' case),
         parse left and right. If both parse, set join_next on the first.
    """
    warnings: List[str] = []
    if not table:
        return ([], warnings)

    from_word = (((vocabulary.get("keywords") or {}).get("connectors") or {}).get("FROM") or "from")
    m = re.search(rf"\b{re.escape(str(from_word))}\b\s+{re.escape(str(table))}\b", source_text, flags=re.I)
    if not m:
        return ([], warnings)

    tail = source_text[m.end():].strip()
    if not tail:
        return ([], warnings)

    patterns = _predicate_patterns_for_vocab(vocabulary)
    if not patterns:
        return ([], warnings)

    # 2) Try whole tail as a single predicate (catches 'between ... and ...')
    single = _parse_single_predicate(tail, table, vocabulary, binder, patterns)
    if single:
        return ([single], warnings)

    # 3) Try a single AND/OR split and parse both sides
    logical = ((vocabulary.get("keywords") or {}).get("logical_operators") or {})
    and_aliases = (logical.get("and") or {}).get("aliases") or ["and"]
    or_aliases  = (logical.get("or")  or {}).get("aliases") or ["or"]
    and_union = _alias_regex_union(and_aliases)
    or_union  = _alias_regex_union(or_aliases)
    m2 = re.search(rf"\s+({and_union}|{or_union})\s+", tail, flags=re.I)

    if not m2:
        # nothing we understand
        return ([], warnings)

    joiner = m2.group(1).strip().lower()
    left   = tail[: m2.start()]
    right  = tail[m2.end() :]

    c1 = _parse_single_predicate(left, table, vocabulary, binder, patterns)
    c2 = _parse_single_predicate(right, table, vocabulary, binder, patterns)

    if not (c1 and c2):
        warnings.append(f"Unrecognized predicate fragment(s): {tail!r}")
        return ([], warnings)

    c1["join_next"] = joiner
    constraints = [c1, c2][:max_predicates]
    return (constraints, warnings)