#!/usr/bin/env python3
# vbg_tools/graph_runtime.py
from __future__ import annotations
import os, sys, re, json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Tuple, Optional

from lark import Lark, UnexpectedInput

# --- SQL helpers ---
from .sql_helpers import (
    build_select_sql_from_slots,
    execute_sqlite,
)

# --- runtime helpers (all Python, no bash) ---
from .runtime_helper import (
    resolve_artifact_paths,
    ensure_artifacts,
    execute_parse,
    attach_sql_if_requested,
    CaseExpectations,
    StepResult,
    validate_parse_case_payload,
    validate_sql_case_payload,
)

# --- NLP / harvesting moved out ---
from .runtime_nlp import (
    tokenize, is_number, is_quoted_string,
    LexEntry, MatchSpan,
    build_lexicon_and_connectors,
    infer_column_types, build_schema_indices,
    build_index, match_aliases,
    gather_tables_columns, collect_actions, harvest_constraints
)

ART_DIR = Path(os.environ.get("ARTIFACTS_DIR", "out"))

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

# ----------------- data structures -----------------
@dataclass
class RuntimeResult:
    canonical_tokens: List[str]
    slots: Dict[str, Any]
    spans: List[Dict[str, Any]]
    parse_ok: bool
    parse_error: Optional[str] = None
    warnings: List[str] = field(default_factory=list)
    tree: Optional[str] = None  # textual tree on demand

# ----------------- slots & canonicalization -----------------
def harvest_and_canonicalize(raw: str,
                             tokens: List[str],
                             spans: List[MatchSpan],
                             tables_by_lc: Dict[str, str],
                             columns_by_lc: Dict[str, str],
                             connectors_map: Dict[str, str]) -> RuntimeResult:
    """
    Build canonical tokens and slots.
    - Keeps previous behavior for SELECT/FROM + projection actions.
    - Does NOT inject constraint tokens; the grammar can stay permissive for now.
    - Adds slots['constraints'] with comparator-derived predicates (builder renders WHERE).
    """
    canonicals: List[str] = []
    for s in spans:
        if s.role == "filler":
            continue
        if s.role == "connector" or s.canonical == "select":
            canon = s.canonical.upper()
        else:
            canon = s.canonical
        canonicals.append(canon)

    found_tables, found_columns = gather_tables_columns(tokens, tables_by_lc, columns_by_lc)
    table = (found_tables[0] if found_tables else None)

    slots: Dict[str, Any] = {
        "table": table,
        "columns": sorted(set(found_columns)),
        "values": [],  # values are only needed by actions; constraints supply their own
    }

    proj_actions, clause_actions, _comparators = collect_actions(spans)
    if proj_actions:
        slots["actions"] = proj_actions
    if clause_actions:
        slots["clause_actions"] = clause_actions

    # Ensure FROM if SELECT present
    has_select = any(c == "SELECT" for c in canonicals)
    has_from   = any(c == "FROM" for c in canonicals)
    if has_select and not has_from:
        canonicals.append("FROM")
        has_from = True

    # Insert VALUE only for projection actions (never for plain SELECT FROM)
    if proj_actions:
        try:
            from_idx = canonicals.index("FROM")
        except ValueError:
            from_idx = len(canonicals)
        insert_at: Optional[int] = None
        if "OF" in canonicals:
            of_idx = canonicals.index("OF")
            if of_idx < from_idx:
                insert_at = of_idx + 1
        if insert_at is None:
            first_action = proj_actions[0]
            try:
                act_idx = canonicals.index(first_action)
            except ValueError:
                act_idx = from_idx
            insert_at = min(act_idx + 1, from_idx)
        if "VALUE" not in canonicals:
            canonicals.insert(insert_at, "VALUE")

    # Constraints harvesting (now with normalized logicals inside the helper)
    constraints, c_warnings = harvest_constraints(raw, tokens, spans, columns_by_lc, table)
    if constraints:
        slots["constraints"] = constraints

    # Unmapped tokens (informational)
    covered = set()
    for s in spans:
        covered.update(range(s.start, s.end))
    unmapped = [t for i, t in enumerate(tokens) if i not in covered and not is_number(t)]
    warnings: List[str] = []
    if unmapped:
        warnings.append(f"Unmapped tokens: {unmapped}")
    warnings.extend(c_warnings)

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

# ----------------- Public test APIs (unchanged) -----------------
@dataclass
class ParseCase:
    label: str
    utterance: str
    tokens_regex: str
    want_parse_ok: bool

@dataclass
class SQLCase:
    label: str
    utterance: str
    tokens_regex: str
    min_rows: int

def _normalize_regex(rx: str) -> str:
    s = (rx or "").strip()
    if len(s) >= 3 and s[0] in ("r", "R") and s[1] in ('"', "'") and s[-1] == s[1]:
        return s[2:-1]
    if s and s[0] in ("r", "R"):
        if len(s) == 1:
            return s
        nxt = s[1]
        if nxt in ("^", "$", "(", "[", ".", "\\", "|", "?", "*", "+"):
            return s[1:]
        if nxt.isspace():
            return s[1:].lstrip()
    return s

def run_parse_case(
    *,
    vocab_yaml: Dict[str, Any],
    binder_yaml: Dict[str, Any],
    grammar_text: str,
    case: ParseCase
) -> Tuple[StepResult, Dict[str, Any]]:
    _step, payload = execute_parse(
        map_text=map_text,
        text=case.utterance,
        vocab_yaml=vocab_yaml,
        binder_yaml=binder_yaml,
        grammar_text=grammar_text,
        want_tree=False,
    )
    res = validate_parse_case_payload(
        payload,
        CaseExpectations(tokens_regex=_normalize_regex(case.tokens_regex), want_parse_ok=case.want_parse_ok),
    )
    return res, payload

def run_sql_case(
    *,
    vocab_yaml: Dict[str, Any],
    binder_yaml: Dict[str, Any],
    grammar_text: str,
    db_path: Optional[str],
    limit: int,
    case: SQLCase
) -> Tuple[StepResult, Dict[str, Any]]:
    _step, payload = execute_parse(
        map_text=map_text,
        text=case.utterance,
        vocab_yaml=vocab_yaml,
        binder_yaml=binder_yaml,
        grammar_text=grammar_text,
        want_tree=False,
    )
    attach_sql_if_requested(
        payload=payload,
        parse_ok=bool(payload.get("parse_ok")),
        want_sql=True,
        db_path=db_path,
        limit=limit,
        binder_yaml=binder_yaml,
        build_select_sql_from_slots=build_select_sql_from_slots,
        execute_sqlite=execute_sqlite,
    )
    res = validate_sql_case_payload(
        payload,
        CaseExpectations(tokens_regex=_normalize_regex(case.tokens_regex), min_rows=case.min_rows),
    )
    return res, payload

def run_tests(
    *,
    vocab_yaml: Dict[str, Any],
    binder_yaml: Dict[str, Any],
    grammar_text: str,
    db_path: Optional[str],
    limit: int,
    parse_cases: List[ParseCase],
    sql_cases: List[SQLCase],
) -> Tuple[int, Dict[str, int]]:
    totals = dict(total=0, ok_pass=0, ok_fail=0, unexpected_fail=0, unexpected_success=0)
    def print_parse(label: str, payload: Dict[str, Any], res: StepResult):
        joined = " ".join(payload.get("canonical_tokens") or [])
        table = ((payload.get("slots") or {}).get("table")) or ""
        print(f"   TOKENS: {joined}")
        print(f"   TABLE:  {table or '<none>'}")
        print(f"   PARSE:  {'true' if payload.get('parse_ok') else 'false'}")
        if res.warnings:
            print("   WARNINGS:")
            for w in res.warnings:
                print(f"     - {w}")

    def print_sql(payload: Dict[str, Any]):
        sql_block = payload.get("sql") or {}
        sql_query = sql_block.get("query") or ""
        rowcount = int(sql_block.get("rowcount") or 0)
        print(f"   SQL:    {sql_query or '<none>'}")
        print(f"   ROWS:   {rowcount}")

    for i, c in enumerate(parse_cases, 1):
        print(f"\n── Test #{i} [parse] {c.label}\n   NL:   {c.utterance}")
        res, payload = run_parse_case(
            vocab_yaml=vocab_yaml, binder_yaml=binder_yaml, grammar_text=grammar_text, case=c
        )
        print_parse(c.label, payload, res)
        ok_expected = c.want_parse_ok
        totals["total"] += 1
        if ok_expected and res.ok:
            totals["ok_pass"] += 1
            print("   ✅ PASS")
        elif (not ok_expected) and (not bool(payload.get("parse_ok"))):
            totals["ok_fail"] += 1
            print("   ✅ EXPECTED FAIL")
        else:
            totals["unexpected_fail"] += 1
            print("   ❌ UNEXPECTED RESULT")

    base = len(parse_cases)
    for j, c in enumerate(sql_cases, 1):
        idx = base + j
        print(f"\n── Test #{idx} [sql] {c.label}\n   NL:   {c.utterance}")
        res, payload = run_sql_case(
            vocab_yaml=vocab_yaml, binder_yaml=binder_yaml, grammar_text=grammar_text,
            db_path=db_path, limit=limit, case=c
        )
        print_parse(c.label, payload, res)
        print_sql(payload)
        totals["total"] += 1
        if res.ok:
            totals["ok_pass"] += 1
            print("   ✅ PASS")
        else:
            totals["unexpected_fail"] += 1
            print("   ❌ FAIL")

    print("\n==================== SUMMARY ====================")
    print(f"Total tests:        {totals['total']}")
    print(f"Expected PASS ok:   {totals['ok_pass']}")
    print(f"Expected FAIL ok:   {totals['ok_fail']}")
    print(f"Unexpected FAIL(s): {totals['unexpected_fail']}")
    print(f"Unexpected SUCC(s): {totals['unexpected_success']}")
    exit_code = 0 if (totals["unexpected_fail"] == 0 and totals["unexpected_success"] == 0) else 1
    return exit_code, totals

# ----------------- CLI -----------------
def _parse_bool(s: str) -> bool:
    return str(s).strip().lower() in ("1","true","t","yes","y","ok")

def _print_usage() -> None:
    print(
        'Usage: vbg_runtime "NL query" [--json] [--tree] [--sql] [--db PATH] [--limit N]\n'
        "       vbg_runtime [--db PATH] --test-case LABEL NL REGEX True|False [--test-case ...]\n"
        "       vbg_runtime [--db PATH] --test-sql-case LABEL NL REGEX MIN_ROWS [--test-sql-case ...]\n",
        file=sys.stderr,
    )

@dataclass
class _CLIParsed:
    as_json: bool
    want_tree: bool
    want_sql: bool
    limit: int
    db_path: Optional[str]
    args: List[str]
    parse_cases: List["ParseCase"]
    sql_cases: List["SQLCase"]

def _parse_cli_argv(argv: List[str]) -> _CLIParsed | Tuple[None, str]:
    args = list(argv)
    as_json   = "--json" in args
    want_tree = "--tree" in args
    want_sql  = "--sql"  in args
    args = [a for a in args if a not in ("--json", "--tree", "--sql")]

    limit = 50
    if "--limit" in args:
        try:
            i = args.index("--limit")
            limit = int(args[i + 1])
            del args[i : i + 2]
        except Exception:
            return None, "Invalid --limit value"

    db_path: Optional[str] = None
    if "--db" in args:
        try:
            i = args.index("--db")
            db_path = args[i + 1]
            del args[i : i + 2]
        except Exception:
            return None, "Missing path after --db"

    parse_cases: List[ParseCase] = []
    sql_cases: List[SQLCase] = []
    i = 0
    while i < len(args):
        a = args[i]
        if a == "--test-case":
            if i + 4 >= len(args):
                return None, "Error: --test-case requires 4 arguments: LABEL NL REGEX True|False"
            label, nl, rx, want = args[i + 1], args[i + 2], args[i + 3], args[i + 4]
            parse_cases.append(ParseCase(label, nl, rx, _parse_bool(want)))
            del args[i : i + 5]
            continue
        elif a == "--test-sql-case":
            if i + 4 >= len(args):
                return None, "Error: --test-sql-case requires 4 arguments: LABEL NL REGEX MIN_ROWS"
            label, nl, rx, min_rows_s = args[i + 1], args[i + 2], args[i + 3], args[i + 4]
            try:
                min_rows = int(min_rows_s)
            except ValueError:
                return None, "Error: MIN_ROWS must be an integer"
            sql_cases.append(SQLCase(label, nl, rx, min_rows))
            del args[i : i + 5]
            continue
        i += 1

    return _CLIParsed(
        as_json=as_json,
        want_tree=want_tree,
        want_sql=want_sql,
        limit=limit,
        db_path=db_path,
        args=args,
        parse_cases=parse_cases,
        sql_cases=sql_cases,
    )

def _ensure_and_load_artifacts() -> Tuple[Dict[str, Any], Dict[str, Any], str] | Tuple[None, None, None]:
    from .runtime_helper import resolve_artifact_paths, ensure_artifacts
    ap = resolve_artifact_paths(str(ART_DIR))
    auto_build = os.environ.get("AUTO_BUILD_ARTIFACTS", "1") not in ("0", "false", "False")
    step = ensure_artifacts(ap, auto_build=auto_build)
    if not step.ok:
        print(f"[artifacts.ensure] failed: {step.info}", file=sys.stderr)
        return None, None, None
    vocab_yaml   = must_load_yaml(Path(ap.vocab_path))
    binder_yaml  = must_load_yaml(Path(ap.binder_path))
    grammar_text = must_load_text(Path(ap.grammar_path))
    return vocab_yaml, binder_yaml, grammar_text

def _run_single_query(
    *,
    text: str,
    as_json: bool,
    want_tree: bool,
    want_sql: bool,
    db_path_opt: Optional[str],
    limit: int,
    vocab_yaml: Dict[str, Any],
    binder_yaml: Dict[str, Any],
    grammar_text: str,
) -> int:
    _step_parse, payload = execute_parse(
        map_text=map_text,
        text=text,
        vocab_yaml=vocab_yaml,
        binder_yaml=binder_yaml,
        grammar_text=grammar_text,
        want_tree=want_tree,
    )
    res_parse_ok = bool(payload.get("parse_ok", False))

    env_db = os.environ.get("DB_PATH", "").strip()
    use_db = db_path_opt or (env_db if env_db else None)
    if res_parse_ok and (want_sql or use_db):
        attach_sql_if_requested(
            payload=payload,
            parse_ok=res_parse_ok,
            want_sql=True,
            db_path=use_db,
            limit=limit,
            binder_yaml=binder_yaml,
            build_select_sql_from_slots=build_select_sql_from_slots,
            execute_sqlite=execute_sqlite,
        )

    if not as_json:
        print("CANONICAL TOKENS:", " ".join(payload.get("canonical_tokens") or []))
        print("PARSE:", "OK" if payload.get("parse_ok") else "FAIL")
        print("SLOTS:")
        print(json.dumps(payload.get("slots") or {}, indent=2))
        if "sql" in payload:
            print("SQL:", (payload["sql"] or {}).get("query") or "")
            if "rows" in (payload["sql"] or {}):
                print(f'ROWS ({(payload["sql"] or {}).get("rowcount") or 0}):')
                print(json.dumps((payload["sql"] or {}).get("rows") or [], indent=2))
        warnings = payload.get("warnings") or []
        if warnings:
            print("WARNINGS:", *warnings, sep="\n- ")
        if want_tree and "tree" in payload:
            print("\nPARSE TREE:\n")
            print(payload["tree"])
    else:
        print(json.dumps(payload, indent=2))

    return 0

def main(argv: Optional[List[str]] = None) -> int:
    argv = argv or sys.argv[1:]
    if not argv:
        print(
            'Usage: vbg_runtime "NL query" [--json] [--tree] [--sql] [--db PATH] [--limit N]\n'
            "       vbg_runtime [--db PATH] --test-case LABEL NL REGEX True|False [--test-case ...]\n"
            "       vbg_runtime [--db PATH] --test-sql-case LABEL NL REGEX MIN_ROWS [--test-sql-case ...]\n",
            file=sys.stderr,
        )
        return 2

    parsed = _parse_cli_argv(argv)
    if isinstance(parsed, tuple) and parsed[0] is None:
        _, msg = parsed
        print(msg, file=sys.stderr)
        return 2
    assert isinstance(parsed, _CLIParsed)

    vocab_yaml, binder_yaml, grammar_text = _ensure_and_load_artifacts()
    if vocab_yaml is None:
        return 2

    if parsed.parse_cases or parsed.sql_cases:
        exit_code, _counts = run_tests(
            vocab_yaml=vocab_yaml,
            binder_yaml=binder_yaml,
            grammar_text=grammar_text,
            db_path=(parsed.db_path or os.environ.get("DB_PATH") or None),
            limit=parsed.limit,
            parse_cases=parsed.parse_cases,
            sql_cases=parsed.sql_cases,
        )
        return exit_code

    if not parsed.args:
        print("No input text provided.", file=sys.stderr)
        return 2

    text = " ".join(parsed.args)
    return _run_single_query(
        text=text,
        as_json=parsed.as_json,
        want_tree=parsed.want_tree,
        want_sql=parsed.want_sql,
        db_path_opt=parsed.db_path,
        limit=parsed.limit,
        vocab_yaml=vocab_yaml,
        binder_yaml=binder_yaml,
        grammar_text=grammar_text,
    )

if __name__ == "__main__":
    sys.exit(main())
