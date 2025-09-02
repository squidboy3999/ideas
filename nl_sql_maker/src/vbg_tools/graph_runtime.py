#!/usr/bin/env python3
# vbg_tools/graph_runtime.py
from __future__ import annotations
import os, sys, re, json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Tuple, Optional
import sqlite3
from lark import Lark, UnexpectedInput

ART_DIR = Path(os.environ.get("ARTIFACTS_DIR", "out"))

# Strictly use graph_* artifacts (no h_* fallbacks)
VOCAB_PATH   = ART_DIR / "graph_vocabulary.yaml"
BINDER_PATH  = ART_DIR / "graph_binder.yaml"
GRAMMAR_PATH = ART_DIR / "graph_grammar.lark"
# ----------------- SQL compilation & execution -----------------


def table_columns_from_binder(binder_yaml: Dict[str, Any], table: str) -> List[str]:
    """Return base column names for a given table from the binder. Falls back to empty list."""
    catalogs = (binder_yaml.get("catalogs") or {})
    columns = catalogs.get("columns") or {}
    cols: List[str] = []
    for fqn, cinfo in columns.items():
        if isinstance(cinfo, dict) and cinfo.get("table") == table:
            base = cinfo.get("name")
            if isinstance(base, str) and base:
                cols.append(base)
    return sorted(set(cols))

def build_select_sql(res: RuntimeResult, binder_yaml: Dict[str, Any], limit: int = 50) -> str:
    """Build a simple SELECT for SQLite from resolved slots.
    - SELECT <explicit columns if provided, else all table columns, else *>
    - FROM "<table>"
    - LIMIT <limit>
    """
    table = (res.slots or {}).get("table")
    if not table:
        raise ValueError("Cannot build SQL: no table was resolved.")

    # Pick columns
    resolved_cols: List[str] = []
    for fqn in (res.slots or {}).get("columns", []):
        # our runtime stores FQNs in slots; base name is after the dot
        base = fqn.split(".", 1)[1] if "." in fqn else fqn
        if base:
            resolved_cols.append(base)

    if not resolved_cols:
        resolved_cols = table_columns_from_binder(binder_yaml, table)

    # If still nothing, fall back to * (unlikely if binder is present)
    if not resolved_cols:
        col_sql = "*"
    else:
        # Quote identifiers for SQLite
        col_sql = ", ".join(f'"{c}"' for c in resolved_cols)

    sql = f'SELECT {col_sql} FROM "{table}"'
    if isinstance(limit, int) and limit > 0:
        sql += f" LIMIT {limit}"
    return sql

def execute_sqlite(db_path: str, sql: str) -> Tuple[List[Dict[str, Any]], int]:
    """Execute SQL against SQLite and return rows as list of dicts + rowcount."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.execute(sql)
        rows_raw = cur.fetchall()
        rows = [dict(r) for r in rows_raw]
        return rows, len(rows)
    finally:
        conn.close()

# ----------------- IO helpers -----------------
def must_load_yaml(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise SystemExit(f"Missing required artifact: {path}")
    import yaml
    with open(path, "r", encoding="utf-8") as f:
        return (yaml.safe_load(f) or {})

def must_load_text(path: Path) -> str:
    if not path.exists():
        raise SystemExit(f"Missing required artifact: {path}")
    return path.read_text(encoding="utf-8")

# ----------------- tokenization -----------------
TOKEN_RE = re.compile(r"[A-Za-z0-9_]+|[,]")

def tokenize(text: str) -> List[str]:
    s = (text or "").strip().lower()
    if not s:
        return []
    return TOKEN_RE.findall(s)

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

@dataclass
class RuntimeResult:
    canonical_tokens: List[str]
    slots: Dict[str, Any]
    spans: List[Dict[str, Any]]
    parse_ok: bool
    parse_error: Optional[str] = None
    warnings: List[str] = field(default_factory=list)
    tree: Optional[str] = None  # textual tree on demand

# ----------------- vocabulary loader -----------------
KW_SECTIONS_WITH_ALIASES = [
    ("select_verbs", "select_verb", True),   # nested: {"select": {aliases: [...]}}
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

def build_lexicon_and_connectors(vocab_yaml: Dict[str, Any]) -> Tuple[List[LexEntry], Dict[str, str]]:
    lexicon: List[LexEntry] = []
    connectors_map: Dict[str, str] = {}  # Canonical (e.g., AND) -> surface (e.g., "and")

    kw = (vocab_yaml.get("keywords") or {})
    # keywords.* sections
    for section, role, is_nested in KW_SECTIONS_WITH_ALIASES:
        sec = kw.get(section) or {}
        if not isinstance(sec, dict):
            continue
        if section == "connectors":
            # {AND: "and"} or {AND: {surface:"and"}}
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
    # top-level actions
    for section, role in ACTION_SECTIONS_WITH_ALIASES:
        sec = (vocab_yaml.get(section) or {})
        if not isinstance(sec, dict):
            continue
        for canonical, ent in sec.items():
            can = str(canonical)
            for surf in _to_alias_list(ent):
                toks = tuple(tokenize(surf))
                if toks:
                    lexicon.append(LexEntry(tokens=toks, canonical=can, role=role, surface=surf))
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
    """Map int/float/decimal → numeric unless 'id' is present in name/labels.
    If it looks like an id, explicitly include 'id' and do not coerce numerics."""
    # collect hints
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
        mapped.append("id")  # <-- ensure 'id' is present when it looks like an id

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

# ----------------- slots & canonicalization -----------------
def harvest_and_canonicalize(raw: str,
                             tokens: List[str],
                             spans: List[MatchSpan],
                             tables_by_lc: Dict[str, str],
                             columns_by_lc: Dict[str, str],
                             connectors_map: Dict[str, str]) -> RuntimeResult:
    # Canonical tokens (drop fillers)
    canonicals: List[str] = []
    for s in spans:
        if s.role == "filler":
            continue
        # Normalize connectors & SELECT to uppercase tokens for grammar terminals
        if s.role == "connector" or s.canonical == "select":
            canon = s.canonical.upper()
        else:
            canon = s.canonical
        canonicals.append(canon)

    # Simple slots (for future SQL assembly)
    values: List[str] = []
    for m in re.finditer(r"(['\"]).*?\1", raw):
        inner = is_quoted_string(m.group(0))
        if inner is not None:
            values.append(inner)
    for t in tokens:
        if is_number(t):
            values.append(t)

    found_tables: List[str] = []
    found_columns: List[str] = []
    for t in tokens:
        if t in tables_by_lc:
            found_tables.append(tables_by_lc[t])
        if t in columns_by_lc:
            found_columns.append(columns_by_lc[t])

    slots = {
        "table": (found_tables[0] if found_tables else None),
        "columns": sorted(set(found_columns)),
        "values": values,
    }

    # Grammar nudge: ensure SELECT ... FROM ... minimal shape
    has_select = any(c == "SELECT" for c in canonicals)
    has_from   = any(c == "FROM" for c in canonicals)
    if has_select and not has_from:
        canonicals.append("FROM")

    # Unmapped tokens (for debugging)
    covered = set()
    for s in spans:
        covered.update(range(s.start, s.end))
    unmapped = [t for i, t in enumerate(tokens) if i not in covered and not is_number(t)]
    warnings: List[str] = []
    if unmapped:
        warnings.append(f"Unmapped tokens: {unmapped}")

    return RuntimeResult(
        canonical_tokens=canonicals,
        slots=slots,
        spans=[{"start": s.start, "end": s.end, "surface": s.surface, "canonical": s.canonical, "role": s.role} for s in spans],
        parse_ok=False,
        parse_error=None,
        warnings=warnings,
    )

# ----------------- Lark parse -----------------
def try_parse_with_lark(grammar_text: str, canonical_tokens: List[str], want_tree: bool) -> Tuple[bool, Optional[str], Optional[str]]:
    # Feed a whitespace-separated stream of canonical tokens to the grammar.
    # Assumes grammar terminals are compatible (e.g., SELECT, FROM, AND...) and
    # it has terminals/nonterminals for identifiers where needed.
    text = " ".join(canonical_tokens).strip()
    try:
        parser = Lark(grammar_text, parser="earley", lexer="dynamic_complete")
        tree = parser.parse(text)
        return True, None, (tree.pretty() if want_tree else None)
    except UnexpectedInput as e:
        return False, str(e), None
    except Exception as e:
        return False, f"{type(e).__name__}: {e}", None

# ----------------- core mapping -----------------
def map_text(text: str,
             vocab_yaml: Dict[str, Any],
             binder_yaml: Dict[str, Any],
             grammar_text: str,
             want_tree: bool) -> RuntimeResult:
    lexicon, connectors_map = build_lexicon_and_connectors(vocab_yaml)
    tables_by_lc, columns_by_lc, _column_types = build_schema_indices(binder_yaml)

    toks = tokenize(text)
    by_len, max_len = build_index(lexicon)
    spans = match_aliases(toks, by_len, max_len)

    res = harvest_and_canonicalize(text, toks, spans, tables_by_lc, columns_by_lc, connectors_map)

    ok, err, tree = try_parse_with_lark(grammar_text, res.canonical_tokens, want_tree)
    res.parse_ok = ok
    res.parse_error = err
    res.tree = tree
    if not ok and err:
        res.warnings.append(f"Grammar parse failed: {err}")
    return res

# ----------------- CLI -----------------
def main(argv: Optional[List[str]] = None) -> int:
    argv = argv or sys.argv[1:]
    if not argv:
        print("Usage: vbg_graph_runtime \"natural language query\" [--json] [--tree] [--sql] [--db PATH] [--limit N]\n", file=sys.stderr)
        return 2

    as_json = "--json" in argv
    want_tree = "--tree" in argv
    want_sql = "--sql" in argv

    # parse limit
    limit = 50
    if "--limit" in argv:
        try:
            i = argv.index("--limit")
            limit = int(argv[i+1])
            argv = argv[:i] + argv[i+2:]
        except Exception:
            print("Invalid --limit value", file=sys.stderr)
            return 2

    # parse db
    db_path: Optional[str] = None
    if "--db" in argv:
        try:
            i = argv.index("--db")
            db_path = argv[i+1]
            argv = argv[:i] + argv[i+2:]
        except Exception:
            print("Missing path after --db", file=sys.stderr)
            return 2

    argv = [a for a in argv if a not in ("--json","--tree","--sql")]
    if not argv:
        print("No input text provided.", file=sys.stderr)
        return 2
    text = " ".join(argv)

    vocab_yaml   = must_load_yaml(VOCAB_PATH)
    binder_yaml  = must_load_yaml(BINDER_PATH)
    grammar_text = must_load_text(GRAMMAR_PATH)

    res = map_text(text, vocab_yaml, binder_yaml, grammar_text, want_tree)

    payload = {
        "canonical_tokens": res.canonical_tokens,
        "slots": res.slots,
        "spans": res.spans,
        "parse_ok": res.parse_ok,
        "warnings": res.warnings,
    }

    sql_stmt = None
    rows = None
    rowcount = None

    # Optionally build SQL (only if we resolved a table and parse_ok)
    if want_sql and res.parse_ok and (res.slots or {}).get("table"):
        try:
            sql_stmt = build_select_sql(res, binder_yaml, limit=limit)
        except Exception as e:
            res.warnings.append(f"SQL build error: {e}")

    # Optionally execute SQL if db path provided
    if db_path and res.parse_ok and (res.slots or {}).get("table"):
        # Build if not already built
        if not sql_stmt:
            try:
                sql_stmt = build_select_sql(res, binder_yaml, limit=limit)
            except Exception as e:
                res.warnings.append(f"SQL build error: {e}")
        if sql_stmt:
            try:
                rows, rowcount = execute_sqlite(db_path, sql_stmt)
            except Exception as e:
                res.warnings.append(f"DB exec error: {e}")

    # Output
    if sql_stmt is not None:
        payload["sql"] = sql_stmt
    if rows is not None:
        payload["rows"] = rows
        payload["rowcount"] = rowcount
    if db_path:
        payload["db_path"] = db_path

    if not as_json:
        print("CANONICAL TOKENS:", " ".join(res.canonical_tokens))
        print("PARSE:", "OK" if res.parse_ok else "FAIL")
        print("SLOTS:")
        print(json.dumps(res.slots, indent=2))
        if sql_stmt:
            print("SQL:", sql_stmt)
        if rows is not None:
            print(f"ROWS ({rowcount}):")
            print(json.dumps(rows, indent=2))
        if res.warnings:
            print("WARNINGS:", *res.warnings, sep="\n- ")
        if want_tree and res.tree:
            print("\nPARSE TREE:\n")
            print(res.tree)
    else:
        if want_tree and res.tree:
            payload["tree"] = res.tree
        print(json.dumps(payload, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
