# phase_i_helpers.py
from __future__ import annotations

import random
import re
from typing import Any, Dict, List, Optional, Set, Tuple

from lark import Lark

# Runtime canonical core (single source of truth)
from .canonical_core import (
    CanonicalBinder,
    BindError,
    canon_tokenize,
    serialize_binding,
    is_compatible,
)

# ------------------------------------------------------------------------------
# Artifact accessors (lightweight)
# ------------------------------------------------------------------------------

def i_extract_canonical_sets(
    graph: Dict[str, Any],
    binder_artifact: Optional[Dict[str, Any]] = None,  # intentionally unused for sets
) -> Dict[str, Set[str]]:
    """
    Return {'tables', 'columns', 'functions'} derived from the graph.
    We intentionally avoid binder_artifact here so we always have full topology.
    """
    tables: Set[str] = set()
    columns: Set[str] = set()
    functions: Set[str] = set()

    for k, v in (graph or {}).items():
        if not isinstance(v, dict):
            continue
        et = v.get("entity_type")
        if et == "table":
            tables.add(k)
        elif et == "column":
            columns.add(k)
        elif et in ("sql_actions", "postgis_actions"):
            functions.add(k)

    return {"tables": tables, "columns": columns, "functions": functions}


def i_get_column_meta(
    name: str,
    graph: Dict[str, Any],
    binder_artifact: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Normalize column metadata to a graph-like dict: {'type', 'type_category', 'labels'}.
    """
    # Prefer binder catalogs
    if isinstance(binder_artifact, dict):
        catalogs = binder_artifact.get("catalogs")
        if isinstance(catalogs, dict):
            col = (catalogs.get("columns") or {}).get(name, {}) or {}
            return {
                "type": col.get("type"),
                "type_category": col.get("type_category"),
                "labels": list(col.get("labels") or []),
            }
    # Fallback to graph
    node = (graph or {}).get(name, {}) or {}
    md = node.get("metadata", {}) if isinstance(node, dict) else {}
    if not isinstance(md, dict):
        md = {}
    return {
        "type": md.get("type"),
        "type_category": md.get("type_category"),
        "labels": list(md.get("labels") or []),
    }


def i_get_function_meta(
    name: str,
    graph: Dict[str, Any],
    binder_artifact: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Normalize function metadata to a graph-like dict:
      {'applicable_types', 'label_rules', 'template'?}
    Missing keys are omitted.
    """
    # Prefer binder catalogs
    if isinstance(binder_artifact, dict):
        catalogs = binder_artifact.get("catalogs")
        if isinstance(catalogs, dict):
            f = (catalogs.get("functions") or {}).get(name, {}) or {}
            out: Dict[str, Any] = {}
            if "applicable_types" in f:
                out["applicable_types"] = f.get("applicable_types")
            if "label_rules" in f:
                out["label_rules"] = f.get("label_rules")
            if "template" in f:
                out["template"] = f.get("template")
            return out

    # Fallback to graph
    node = (graph or {}).get(name, {}) or {}
    md = node.get("metadata", {}) if isinstance(node, dict) else {}
    return md if isinstance(md, dict) else {}


def i_table_to_columns(
    graph: Dict[str, Any],
    binder_artifact: Optional[Dict[str, Any]] = None,  # intentionally unused for topology
) -> Dict[str, List[str]]:
    """
    Map table -> [columns] using the graph topology (authoritative).
    """
    out: Dict[str, List[str]] = {}
    for tname, node in (graph or {}).items():
        if not isinstance(node, dict) or node.get("entity_type") != "table":
            continue
        cols_meta = ((node.get("metadata") or {}).get("columns") or {})
        if isinstance(cols_meta, dict):
            out[tname] = list(cols_meta.keys())
        elif isinstance(cols_meta, list):
            out[tname] = [str(c).strip() for c in cols_meta if str(c).strip()]
        else:
            out[tname] = []
    return out



# ------------------------------------------------------------------------------
# Grammar helpers
# ------------------------------------------------------------------------------

def i_build_parser(grammar_text: str) -> Lark:
    """
    Instantiate the canonical Lark parser (start='query').
    """
    if not isinstance(grammar_text, str) or not grammar_text.strip():
        raise ValueError("grammar_text must be a non-empty string.")
    return Lark(grammar_text, start="query")


def i_introspect_grammar(grammar_text: str) -> Dict[str, Any]:
    """
    Optional skim: identify if Oxford comma likely supported.
    This is best-effort; if unsure, default to supporting Oxford ', and'.
    """
    supports_oxford = "and" in grammar_text and "," in grammar_text
    return {"supports_oxford_and": bool(supports_oxford), "list_separators": [",", "and"]}


# ------------------------------------------------------------------------------
# Canonical generation (fallback sampler)
# ------------------------------------------------------------------------------

def i_generate_canonical_examples(
    graph: Dict[str, Any],
    parser: Lark,
    canon_sets: Dict[str, Set[str]],
    phrases: int = 100,
    rng_seed: Optional[int] = None,
    binder_artifact: Optional[Dict[str, Any]] = None,
) -> List[str]:
    """
    Produce 'select <items> from <table>' samples for C1.

    Conservative to avoid grammar ambiguity:
      • ≤ 3 items per SELECT
      • ≤ 1 function item total
      • function args (when required) use exactly 1 COLUMN
    """
    if rng_seed is not None:
        random.seed(rng_seed)

    tables = sorted(canon_sets.get("tables") or [])
    if not tables:
        return []

    # Table → columns
    t2c = i_table_to_columns(graph)

    # Function metadata (if present)
    catalogs = (binder_artifact or {}).get("catalogs") if isinstance(binder_artifact, dict) else {}
    fn_meta = catalogs.get("functions") if isinstance(catalogs, dict) else {}

    all_functions = sorted(canon_sets.get("functions") or [])
    DENY = {"order_by_asc", "order_by_desc", "group_by", "having", "limit", "distinct"}

    def _is_select_friendly(fn: str) -> bool:
        if fn in DENY:
            return False
        meta = fn_meta.get(fn, {}) if isinstance(fn_meta, dict) else {}
        klass = str(meta.get("class", "")).lower()
        clause = str(meta.get("clause", "")).lower()
        if klass == "ordering":
            return False
        if clause in {"order_by", "group_by", "having", "where", "limit"}:
            return False
        return clause in {"select", "both", ""}

    functions = [f for f in all_functions if _is_select_friendly(f)]

    out: List[str] = []
    for _ in range(max(0, phrases)):
        tbl = random.choice(tables)
        cols = list(t2c.get(tbl, []))

        # If no columns and no functions, skip
        if not cols and not functions:
            continue

        # 1–3 items, ≤1 function
        k = random.choice([1, 2, 3])
        used_function = False
        items: List[str] = []

        for _i in range(k):
            # If we already used a function, force a column
            if used_function or not functions or (cols and random.random() >= 0.35):
                if not cols:
                    continue
                items.append(random.choice(cols))
                continue

            # Consider adding a function
            fn = random.choice(functions)
            meta = fn_meta.get(fn, {}) if isinstance(fn_meta, dict) else {}
            needs_args = bool(meta.get("args"))

            if needs_args:
                # Exactly one column arg to avoid nested lists
                if not cols:
                    # fallback to column if no columns to satisfy arg
                    items.append(random.choice(cols) if cols else fn)
                else:
                    items.append(f"{fn} of {random.choice(cols)}")
            else:
                items.append(fn)

            used_function = True  # cap at one function in the select list

        if not items:
            continue

        # Join with Oxford style (grammar allows; Phase-I sanitizes before binding)
        if len(items) == 1:
            sel = items[0]
        elif len(items) == 2:
            sel = f"{items[0]} and {items[1]}"
        else:
            head = ", ".join(items[:-1])
            sel = f"{head}, and {items[-1]}"

        s = f"select {sel} from {tbl}"
        try:
            parser.parse(s)
            out.append(s)
        except Exception:
            # If the grammar rejects, skip quietly
            continue

    return out


# ------------------------------------------------------------------------------
# Binder construction (relaxed for C1)
# ------------------------------------------------------------------------------

def i_make_relaxed_binder(
    graph: Dict[str, Any],
    binder_artifact: Optional[Dict[str, Any]] = None,
) -> CanonicalBinder:
    """
    Instantiate the binder for C1 with a view that *exposes connectors* the way
    CanonicalBinder expects. We merge binder_artifact connectors into the graph-shaped view.
    """
    import copy

    # Start from the graph so entity_type topology is present
    view = copy.deepcopy(graph or {})

    # Extract connectors from binder catalogs if available
    conn_map: Dict[str, str] = {}
    if isinstance(binder_artifact, dict):
        catalogs = binder_artifact.get("catalogs") or {}
        cm = catalogs.get("connectors") or {}
        if isinstance(cm, dict):
            conn_map.update({str(k): str(v) for k, v in cm.items()})

    if conn_map:
        # Normalize: provide both UPPER and lower keys, e.g. "AND" and "and"
        norm: Dict[str, str] = {}
        for k, v in conn_map.items():
            norm[k] = v
            norm[k.upper()] = v
            norm[k.lower()] = v

        # Expose via policy (common place CanonicalBinder reads)
        pol = view.get("_policy") or {}
        pol["connectors"] = norm
        view["_policy"] = pol

        # Also expose via _binder_meta.connectors list of {name, surface} for older paths
        meta = view.get("_binder_meta") or {}
        lst = [{"name": k, "surface": v} for k, v in sorted(norm.items())]
        meta["connectors"] = lst
        view["_binder_meta"] = meta

    return CanonicalBinder(
        view,
        strict_types=False,
        coerce_types=True,
        allow_ordering_funcs_in_args=True,
    )



# ------------------------------------------------------------------------------
# C1: Canonical roundtrip
# ------------------------------------------------------------------------------
# --- List sanitizer: make 'and' lists into comma lists before binding ---

_WORD_OR_COMMA = re.compile(r",|[A-Za-z0-9_.]+")

def i_sanitize_list_connectors(canonical: str) -> str:
    """
    Rewrite Oxford/AND lists in canonical into plain comma lists, e.g.:
      'select A and B from T'          -> 'select A, B from T'
      'select A, B, and C from T'      -> 'select A, B, C from T'
      'select fn of A and B from T'    -> 'select fn of A, B from T'
      'select fn of A, B, and C from T'-> 'select fn of A, B, C from T'

    Only touches commas and 'and' when they sit between item-like tokens.
    Leaves 'of' and 'from' intact.
    """
    toks = _WORD_OR_COMMA.findall(canonical or "")
    out: List[str] = []
    i = 0

    def _is_item(tok: str) -> bool:
        if not tok: return False
        low = tok.lower()
        if low in {"and", "of", "from"}:  # treat structural tokens separately
            return False
        # canonical atoms: dotted ids or bare function/table names
        return bool(re.match(r"^[A-Za-z0-9_.]+$", tok))

    while i < len(toks):
        t = toks[i]
        low = t.lower()
        # Collapse ", and" -> ","
        if t == "," and i + 1 < len(toks) and toks[i + 1].lower() == "and":
            out.append(",")
            i += 2
            continue

        # Turn "X and Y" into "X , Y" when both sides look like items
        if low == "and":
            prev_tok = toks[i - 1] if i > 0 else ""
            next_tok = toks[i + 1] if i + 1 < len(toks) else ""
            if _is_item(prev_tok) and _is_item(next_tok):
                out.append(",")
                i += 1
                continue

        out.append(t)
        i += 1

    # Rebuild with simple spacing rules: words space-separated, comma followed by space
    rebuilt: List[str] = []
    for j, t in enumerate(out):
        if t == ",":
            rebuilt.append(",")
            rebuilt.append(" ")
        else:
            rebuilt.append(t)
            if j + 1 < len(out) and out[j + 1] != ",":
                rebuilt.append(" ")
    return "".join(rebuilt).strip()

def i_roundtrip_one(
    binder: CanonicalBinder,
    parser: Lark,
    canonical: str,
) -> Tuple[bool, Dict[str, Any]]:
    """
    Bind → serialize → parse one canonical string. Collect flight recorder logs.
    Prior to binding, sanitize list connectors so Oxford lists don't trip the binder.
    """
    recorder: List[str] = []
    info: Dict[str, Any] = {
        "input": canonical,
        "sanitized": None,
        "rebuilt": None,
        "error": None,
        "binder_trace": recorder,
    }

    try:
        safe = i_sanitize_list_connectors(canonical)
        info["sanitized"] = safe
        bound = binder.bind(canon_tokenize(safe), recorder=recorder)
        rebuilt = serialize_binding(bound)
        info["rebuilt"] = rebuilt
        parser.parse(rebuilt)
        return True, info
    except Exception as e:
        info["error"] = str(e)
        return False, info



def i_roundtrip_suite(
    graph: Dict[str, Any],
    grammar_text: str,
    phrases: int = 100,
    success_threshold: float = 0.95,
    rng_seed: Optional[int] = None,
    binder_artifact: Optional[Dict[str, Any]] = None,
) -> Tuple[bool, Dict[str, Any]]:
    """
    End-to-end roundtrip: generate canonical samples, bind & parse them, aggregate stats.
    """
    parser = i_build_parser(grammar_text)
    canon_sets = i_extract_canonical_sets(graph, binder_artifact)
    binder = i_make_relaxed_binder(graph, binder_artifact)

    samples = i_generate_canonical_examples(graph, parser, canon_sets, phrases=phrases, rng_seed=rng_seed, binder_artifact=binder_artifact)
    if not samples:
        # Nothing to test → treat as pass but report
        report = {
            "successes": 0,
            "failures": 0,
            "rate": 1.0,
            "top_error_buckets": [],
            "examples": ["NO_SAMPLES"],
        }
        return True, report

    ok_count = 0
    fail_count = 0
    bucket: Dict[str, int] = {}
    examples: List[Dict[str, Any]] = []

    for s in samples:
        ok, info = i_roundtrip_one(binder, parser, s)
        if ok:
            ok_count += 1
        else:
            fail_count += 1
            # bucket by error message prefix
            key = info.get("error") or "unknown"
            bucket[key] = bucket.get(key, 0) + 1
            if len(examples) < 5:
                examples.append(info)

    total = ok_count + fail_count
    rate = (ok_count / total) if total else 1.0
    # sort buckets by count desc
    top = sorted(bucket.items(), key=lambda kv: kv[1], reverse=True)

    report = {
        "successes": ok_count,
        "failures": fail_count,
        "rate": rate,
        "top_error_buckets": top[:10],
        "examples": examples,
        "tested": total,
    }
    return (rate >= success_threshold), report


# ------------------------------------------------------------------------------
# C2: Feasibility (lightweight)
# ------------------------------------------------------------------------------

_PLACEHOLDER_RE = re.compile(r"\{([A-Za-z0-9_]+)\}")

def _first_compatible_column(
    graph: Dict[str, Any],
    binder_artifact: Optional[Dict[str, Any]],
    func_name: str,
    canon_sets: Dict[str, Set[str]],
) -> Optional[str]:
    fn_md = i_get_function_meta(func_name, graph, binder_artifact)
    for col in canon_sets.get("columns", set()):
        col_md = i_get_column_meta(col, graph, binder_artifact)
        if is_compatible(col_md, fn_md):
            return col
    return None


def _any_table(canon_sets: Dict[str, Set[str]]) -> Optional[str]:
    s = list(canon_sets.get("tables", set()))
    return s[0] if s else None


def i_feasibility_for_function(
    graph: Dict[str, Any],
    binder_artifact: Optional[Dict[str, Any]],
    func_name: str,
    canon_sets: Dict[str, Set[str]],
) -> Dict[str, Any]:
    """
    Check if placeholders in function's SQL template are fillable:
      {column}/{columns}/{table}/{value}
    """
    md = i_get_function_meta(func_name, graph, binder_artifact)
    tmpl = md.get("template")
    if not isinstance(tmpl, str):
        return {"ok": True, "func": func_name, "status": "no_template"}

    placeholders = set(_PLACEHOLDER_RE.findall(tmpl))
    env: Dict[str, Any] = {}
    problems: List[str] = []

    if "column" in placeholders or "columns" in placeholders:
        col = _first_compatible_column(graph, binder_artifact, func_name, canon_sets)
        if not col:
            problems.append("no_compatible_column")
        else:
            env["column"] = col
            env["columns"] = [col]

    if "table" in placeholders:
        t = _any_table(canon_sets)
        if not t:
            problems.append("no_table")
        else:
            env["table"] = t

    if "value" in placeholders:
        env["value"] = "__VALUE__"

    ok = not problems
    return {
        "ok": ok,
        "func": func_name,
        "placeholders": sorted(placeholders),
        "problems": problems,
        "example_bind": env,
    }


def i_feasibility_suite(
    graph: Dict[str, Any],
    binder_artifact: Optional[Dict[str, Any]],
    sample_functions: int = 50,
) -> Dict[str, Any]:
    """
    Sample functions and report which templates are not trivially fillable.
    """
    canon_sets = i_extract_canonical_sets(graph, binder_artifact)
    funcs = list(canon_sets.get("functions", set()))
    random.shuffle(funcs)
    if sample_functions and len(funcs) > sample_functions:
        funcs = funcs[:sample_functions]

    problems: List[Dict[str, Any]] = []
    for fn in funcs:
        report = i_feasibility_for_function(graph, binder_artifact, fn, canon_sets)
        if not report.get("ok", True):
            problems.append(report)

    return {
        "ok": not problems,
        "sampled": len(funcs),
        "problems": problems,
    }


# ------------------------------------------------------------------------------
# C3: Negative canonical tests
# ------------------------------------------------------------------------------

def i_make_negative_examples(
    canon_sets: Dict[str, Set[str]],
    k: int = 8,
) -> List[str]:
    """
    Craft canonical strings that should not fully succeed (binder and/or parser).
    """
    tables = list(canon_sets.get("tables") or [])
    columns = list(canon_sets.get("columns") or [])
    functions = list(canon_sets.get("functions") or [])

    if not tables or not columns:
        return []

    tbl = random.choice(tables)
    col = random.choice(columns)
    fn = random.choice(functions) if functions else None

    cases = [
        f"select {col} {tbl}",                 # missing FROM/OF
        f"select , {col} from {tbl}",          # leading comma
        f"select and {col} from {tbl}",        # leading 'and'
        f"select {col} and , {col} from {tbl}",# bad ', and' order
        f"select from {tbl}",                  # empty select list
    ]
    if fn:
        cases += [
            f"select {fn} {col} from {tbl}",             # function missing 'of'
            f"select {fn} of from {tbl}",                # missing arg after 'of'
            f"select {fn} of {fn} of {col} from {tbl}",  # nested fn missing args for inner
        ]

    # Deduplicate & trim to k
    uniq = list(dict.fromkeys(cases))
    random.shuffle(uniq)
    return uniq[:k]


def i_negative_suite(
    graph: Dict[str, Any],
    grammar_text: str,
    k: int = 8,
    binder_artifact: Optional[Dict[str, Any]] = None,
) -> Tuple[bool, Dict[str, Any]]:
    """
    Run negative strings; fail if any pass both binder and parser.
    """
    parser = i_build_parser(grammar_text)
    binder = i_make_relaxed_binder(graph, binder_artifact)  # relaxed is fine; parser still guards structure
    canon_sets = i_extract_canonical_sets(graph, binder_artifact)
    negs = i_make_negative_examples(canon_sets, k=k)
    if not negs:
        return True, {"ok": True, "tested": 0, "passed_both": []}

    bad: List[str] = []

    for s in negs:
        bound_ok = True
        parse_ok = True
        try:
            binder.bind(canon_tokenize(s))
        except Exception:
            bound_ok = False
        try:
            parser.parse(s)
        except Exception:
            parse_ok = False

        if bound_ok and parse_ok:
            bad.append(s)

    return (len(bad) == 0), {"ok": len(bad) == 0, "tested": len(negs), "passed_both": bad}


# ------------------------------------------------------------------------------
# Log assembly
# ------------------------------------------------------------------------------

def i_build_phase_i_log(
    roundtrip_report: Dict[str, Any],
    feasibility_report: Dict[str, Any],
    negatives_report: Dict[str, Any],
) -> str:
    """
    Produce a human-friendly multi-section log similar to the old validators.
    """
    lines: List[str] = []
    lines.append("[C1] Canonical → Binder → Grammar Roundtrip")
    lines.append(f"  - Success Rate: {roundtrip_report.get('rate', 0.0):.0%} "
                 f"({roundtrip_report.get('successes', 0)}/{roundtrip_report.get('tested', 0)})")
    if roundtrip_report.get("top_error_buckets"):
        lines.append("  - Top error buckets:")
        for msg, n in roundtrip_report["top_error_buckets"]:
            lines.append(f"    • {n}× {msg}")
    if roundtrip_report.get("examples"):
        lines.append("  - Sample failures:")
        for ex in roundtrip_report["examples"][:3]:
            if isinstance(ex, str):
                lines.append(f"    * {ex}")
            else:
                lines.append(f"    * Input='{ex.get('input')}' :: Error={ex.get('error')}")

    lines.append("")
    lines.append("[C2] Binder SQL Feasibility (Light)")
    probs = feasibility_report.get("problems", [])
    if probs:
        lines.append(f"  - Problems ({len(probs)}/{feasibility_report.get('sampled', 0)} sampled):")
        for p in probs[:10]:
            lines.append(f"    * {p.get('func')}: {p.get('problems')} placeholders={p.get('placeholders')}")
        if len(probs) > 10:
            lines.append(f"      ... +{len(probs)-10} more")
    else:
        lines.append("  - PASS: All sampled functions are trivially fillable or have no template.")

    lines.append("")
    lines.append("[C3] Negative Canonical Tests")
    if negatives_report.get("passed_both"):
        lines.append("  - FAIL: Some negatives passed both binder and parser:")
        for s in negatives_report["passed_both"]:
            lines.append(f"    * {s}")
    else:
        lines.append("  - PASS: Negatives blocked as expected (binder and/or parser).")

    return "\n".join(lines)
