# phase_j_helpers.py
from __future__ import annotations
import random
import re
from typing import Dict, Any, List, Tuple, Optional, Set

from lark import Lark

# Runtime pieces
from .normalizer import normalize_text
from .canonical_core import (
    CanonicalBinder,
    canon_tokenize,
    serialize_binding,
)

# Canonical generator (for randomized tests)
from .grammar_validator import GrammarAnalyzer, SmartGenerator

# --- Phase-J list sanitizer: normalize Oxford/AND lists to commas ---

_J_WORD_OR_COMMA = re.compile(r",|[A-Za-z0-9_.]+")

def j_sanitize_list_connectors(canonical: str) -> str:
    """
    Rewrite Oxford/AND lists in canonical into plain comma lists, e.g.:
      'select A and B from T'           -> 'select A, B from T'
      'select A, B, and C from T'       -> 'select A, B, C from T'
      'select fn of A and B from T'     -> 'select fn of A, B from T'
      'select fn of A, B, and C from T' -> 'select fn of A, B, C from T'

    Only touches commas and 'and' when flanked by item-like tokens.
    Leaves 'of' and 'from' intact.
    """
    toks = _J_WORD_OR_COMMA.findall(canonical or "")
    out: List[str] = []
    i = 0

    def _is_item(tok: str) -> bool:
        if not tok:
            return False
        low = tok.lower()
        if low in {"and", "of", "from"}:
            return False
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

    # Rebuild: words space-separated, comma followed by space
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

# -----------------------------
# Parser construction
# -----------------------------

def j_make_parser(grammar_text: str) -> Lark:
    """
    Build a Lark parser from canonical grammar text.
    Raises on failure (so the caller can gate).
    """
    if not isinstance(grammar_text, str) or not grammar_text.strip():
        raise ValueError("grammar_text must be a non-empty string")
    return Lark(grammar_text, start="query")


# -----------------------------
# Binding view (binder vs graph)
# -----------------------------

def j_get_binding_view(
    graph: Dict[str, Any],
    binder_artifact: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Prefer binder catalogs when provided (they carry connectors/punctuation),
    else fall back to the graph.
    """
    if isinstance(binder_artifact, dict):
        return binder_artifact
    # Fallback: try to pluck binder from graph artifacts if not provided explicitly
    try:
        art = graph.get("_artifacts") or {}
        b = art.get("binder")
        if isinstance(b, dict):
            return b
    except Exception:
        pass
    return graph



# -----------------------------
# Reverse alias map & denorm
# -----------------------------

_CONNECTORS: Set[str] = {"of", "from", "and", "or"}
_LOCK_TOKENS: Set[str] = {",", *(_CONNECTORS)}

def _coerce_listy(v: Any) -> List[str]:
    if isinstance(v, list):
        return [("" if o is None else str(o)) for o in v]
    return ["" if v is None else str(v)]

def _j_unique_column_basenames(graph: Dict[str, Any]) -> Dict[str, str]:
    """
    Return {canonical_column: unique_basename} when basename (after the dot) is unique.
    E.g. 'sales.sale_date' -> 'sale_date' if no other column uses 'sale_date'.
    """
    by_base: Dict[str, List[str]] = {}
    for k, v in (graph or {}).items():
        if isinstance(v, dict) and v.get("entity_type") == "column":
            base = k.split(".", 1)[1] if "." in k else k
            by_base.setdefault(base, []).append(k)
    out: Dict[str, str] = {}
    for base, cols in by_base.items():
        if len(cols) == 1:
            out[cols[0]] = base
    return out



def j_build_reverse_alias_map(vocabulary: Dict[str, Any], graph: Dict[str, Any]) -> Dict[str, List[str]]:
    """
    Invert vocabulary so we can pick aliases for a canonical token.
    Ensures identity forms (canonical -> canonical) are available.
    Leaves ',', 'of', 'from', 'and', 'or' out of alias pools.
    Adds unique column basenames when safe.
    Also guarantees identities for canonicals present in the GRAPH even if
    the vocabulary does not explicitly list them (useful for unit tests / robustness).
    """
    det = (vocabulary or {}).get("deterministic_aliases", {}) or {}
    nd  = (vocabulary or {}).get("non_deterministic_aliases", {}) or {}

    rev: Dict[str, List[str]] = {}

    # Deterministic aliases
    for alias, canonical in det.items():
        a = str(alias)
        c = "" if canonical in (None, "skip", "_skip") else str(canonical)
        if not c or a.lower() in _LOCK_TOKENS:
            continue
        rev.setdefault(c, [])
        if a not in rev[c]:
            rev[c].append(a)

    # Non-deterministic aliases
    def _coerce_listy(v: Any) -> List[str]:
        if isinstance(v, list):
            return [("" if o is None else str(o)) for o in v]
        return ["" if v is None else str(v)]

    for alias, options in nd.items():
        a = str(alias)
        if a.lower() in _LOCK_TOKENS:
            continue
        for c in _coerce_listy(options):
            if c and c not in {"skip", "_skip"}:
                rev.setdefault(c, [])
                if a not in rev[c]:
                    rev[c].append(a)

    # Ensure canonical identities from VOCAB (if present there)
    seen: Set[str] = set()
    for v in det.values():
        if isinstance(v, str) and v not in {"", "skip", "_skip"}:
            seen.add(v)
    for opts in nd.values():
        for o in _coerce_listy(opts):
            if o and o not in {"", "skip", "_skip"}:
                seen.add(o)
    for c in seen:
        rev.setdefault(c, [])
        if c not in rev[c]:
            rev[c].append(c)

    # NEW: Guarantee identities for canonicals present in the GRAPH
    for k, node in (graph or {}).items():
        if not isinstance(node, dict):
            continue
        et = node.get("entity_type")
        if et in {"table", "column", "sql_actions", "postgis_actions"}:
            rev.setdefault(k, [])
            if k not in rev[k]:
                rev[k].append(k)

    # Inject unique basenames for columns (e.g. 'users.balance' -> 'balance' if unique)
    for col, base in _j_unique_column_basenames(graph).items():
        rev.setdefault(col, [])
        if base not in rev[col]:
            rev[col].append(base)

    # De-dup while preserving order
    for k, lst in list(rev.items()):
        seen_local: Set[str] = set()
        out: List[str] = []
        for s in lst:
            if s not in seen_local:
                seen_local.add(s)
                out.append(s)
        rev[k] = out

    return rev



def _choose_alias_for_token(
    canonical: str,
    next_token: Optional[str],
    alias_pool: List[str],
    *,
    rng: random.Random
) -> Tuple[str, bool]:
    """
    Choose an alias for one canonical token with connector awareness.
    Returns (alias, consume_next_connector).
    Policy:
      - If the next token is a connector, prefer plain alias.
      - If only aliases ending in that connector exist, use one and consume it.
      - Else fallback to any alias or the canonical verbatim.
    """
    nxt = (next_token or "").lower()
    groups: Dict[str, List[str]] = {c: [] for c in _CONNECTORS}
    plain: List[str] = []

    for a in alias_pool:
        s = a.strip()
        low = s.lower()
        matched = False
        for c in _CONNECTORS:
            if low.endswith(" " + c):
                groups[c].append(s)
                matched = True
                break
        if not matched:
            plain.append(s)

    # Lock tokens are handled by caller; we only replace content tokens here.
    if nxt in _CONNECTORS:
        if plain:
            return rng.choice(plain), False
        if groups[nxt]:
            return rng.choice(groups[nxt]), True
        pool = plain or [x for L in groups.values() for x in L]
        return (rng.choice(pool), False) if pool else (canonical, False)

    # No connector follows
    if plain:
        return rng.choice(plain), False
    pool = [x for L in groups.values() for x in L]
    return (rng.choice(pool), False) if pool else (canonical, False)

_WORD_OR_COMMA = re.compile(r',|[A-Za-z0-9_.]+')

def j_denormalize_canonical(
    canonical: str,
    reverse_map: Dict[str, List[str]],
    *,
    rng: random.Random,
    identity_bias: float = 0.9,   # 90% of the time, keep canonical token
) -> str:
    """
    Turn a canonical phrase into a plausible NL by replacing tokens with aliases.
    Strong identity bias (default 0.9) keeps normalizer happy for integration smoke tests.
    Leaves ',', 'of', 'from', 'and', 'or' untouched.
    """
    toks = _WORD_OR_COMMA.findall(canonical or "")
    out: List[str] = []
    i = 0
    while i < len(toks):
        t = toks[i]
        if t in _LOCK_TOKENS:
            out.append(t); i += 1; continue

        pool = reverse_map.get(t, [])
        # Identity bias — keeps most tokens canonical
        if rng.random() < identity_bias or not pool:
            out.append(t)
            i += 1
            continue

        nxt = toks[i + 1] if (i + 1) < len(toks) else None
        alias, consume_next = _choose_alias_for_token(t, nxt, pool, rng=rng)
        out.append(alias)
        i += 2 if (consume_next and (nxt in _LOCK_TOKENS)) else 1

    s = " ".join(out)
    s = re.sub(r"\s*,\s*", ", ", s)
    s = re.sub(r"\s{2,}", " ", s).strip()
    return s


# -----------------------------
# Single NL run: normalize→bind→parse
# -----------------------------
def j_contextualize_basenames(
    canonical: str,
    graph: Dict[str, Any],
) -> str:
    """
    Rewrite bare basenames to dotted canonicals using:
      1) Global uniqueness across graph columns, else
      2) Uniqueness within FROM table(s).
    Leaves ambiguous basenames untouched.

    Works on already-tokenized dotted identifiers (canon_tokenize must preserve dots).
    """
    import re

    if not canonical or not isinstance(graph, dict):
        return canonical

    # Build indices
    base_to_cols: Dict[str, List[str]] = {}
    by_table_base: Dict[Tuple[str, str], str] = {}  # (table, base) -> dotted

    for k, v in graph.items():
        if not (isinstance(v, dict) and v.get("entity_type") == "column"):
            continue
        if "." in k:
            t, b = k.split(".", 1)
        else:
            t, b = (v.get("metadata", {}) or {}).get("table"), k
        if not t or not b:
            continue
        base_to_cols.setdefault(b, []).append(k)
        by_table_base[(t, b)] = k

    toks = canon_tokenize(canonical)
    low = [t.lower() for t in toks]

    # Collect FROM tables (supports simple '... from <table>' only; OK for Phase J)
    from_tables: List[str] = []
    for i, t in enumerate(low):
        if t == "from" and (i + 1) < len(toks):
            # consume a single table token; grammar generator uses dotted cols but simple tables
            tbl = toks[i + 1]
            # Keep only true table canonicals
            if isinstance(graph.get(tbl), dict) and graph[tbl].get("entity_type") == "table":
                from_tables.append(tbl)

    # Map basenames in content positions
    out: List[str] = []
    i = 0
    while i < len(toks):
        t = toks[i]
        tl = low[i]

        # Do not touch punctuation/keywords or already dotted
        if t in {",", ".", "of", "from", "and", "or", "select"} or ("." in t):
            out.append(t); i += 1; continue

        # If it's already a known canonical (column/function/table), keep as-is
        node = graph.get(t)
        if isinstance(node, dict) and node.get("entity_type") in {"column", "sql_actions", "postgis_actions", "table"}:
            out.append(t); i += 1; continue

        # Try basename resolution
        cands = base_to_cols.get(t, [])
        chosen: Optional[str] = None
        if len(cands) == 1:
            chosen = cands[0]
        elif from_tables:
            scoped = [by_table_base.get((ft, t)) for ft in from_tables]
            scoped = [x for x in scoped if x]
            scoped = list(dict.fromkeys(scoped))  # de-dup, preserve order
            if len(scoped) == 1:
                chosen = scoped[0]

        out.append(chosen if chosen else t)
        i += 1

    return " ".join(out)

def j_run_full_pipeline_on_text(
    text: str,
    graph: Dict[str, Any],
    vocabulary: Dict[str, Any],
    parser: Lark,
    *,
    binder_artifact: Optional[Dict[str, Any]] = None,
    max_candidates: int = 50,
    binder_mode: Optional[Dict[str, bool]] = None,
    record_binder: bool = False,
) -> Tuple[bool, Dict[str, Any]]:
    """
    Execute NL→normalize→(sanitize)→bind→parse for one input.
    Returns (ok, stats).
    """
    mode = binder_mode or {
        "strict_types": True,
        "coerce_types": False,
        "allow_ordering_funcs_in_args": False,
    }

    stats: Dict[str, Any] = {
        "input": text,
        "normalizer_candidates": 0,
        "bound_candidates": 0,
        "parsed_candidates": 0,
        "fail_category": None,
        "binder_errors": [],
        "parse_errors": [],
        "picked": None,
        "binder_debug": [],
        # NEW: visibility into any list-connector rewrites
        "sanitized_diffs": 0,
    }

    # Normalize
    try:
        candidates = normalize_text(vocabulary, text)
    except Exception as e:
        stats["fail_category"] = f"normalizer_exception:{e}"
        return False, stats

    stats["normalizer_candidates"] = len(candidates)
    if not candidates:
        stats["fail_category"] = "normalizer_zero"
        return False, stats

    if len(candidates) > max_candidates:
        candidates = candidates[:max_candidates]
        stats["fail_category"] = "normalizer_many"  # non-fatal; just noted

    # Binder view
    bind_view = j_get_binding_view(graph, binder_artifact=binder_artifact)
    recorder: Optional[List[str]] = [] if record_binder else None
    binder = CanonicalBinder(
        bind_view,
        strict_types=bool(mode.get("strict_types", True)),
        coerce_types=bool(mode.get("coerce_types", False)),
        allow_ordering_funcs_in_args=bool(mode.get("allow_ordering_funcs_in_args", False)),
    )

    any_success = False
    for cand in candidates:
        # NEW: sanitize list connectors in the canonical candidate before binding
        safe_cand = j_sanitize_list_connectors(cand)
        if safe_cand != cand:
            stats["sanitized_diffs"] += 1

        try:
            recorder_list: Optional[List[str]] = [] if record_binder else None
            bound = binder.bind(canon_tokenize(safe_cand), recorder=recorder_list)
            stats["bound_candidates"] += 1
            if record_binder and recorder_list:
                stats["binder_debug"].extend(recorder_list)
        except Exception as be:
            stats["binder_errors"].append(str(be))
            continue

        # serialize → parse
        canonical_text = serialize_binding(bound)
        try:
            parser.parse(canonical_text)
            stats["parsed_candidates"] += 1
            stats["picked"] = canonical_text
            any_success = True
            break
        except Exception as pe:
            stats["parse_errors"].append(str(pe))
            continue

    if not any_success:
        if stats["bound_candidates"] == 0:
            stats["fail_category"] = "binder_fail"
        elif stats["parsed_candidates"] == 0:
            stats["fail_category"] = "parser_fail"
        else:
            stats["fail_category"] = "unknown_fail"
        return False, stats

    stats["fail_category"] = "ok"
    return True, stats



# -----------------------------
# Randomized integration suite
# -----------------------------

def _j_canonical_placeholder_fill(
    canonical: str,
    graph: Dict[str, Any],
    *,
    rng: Optional[random.Random] = None
) -> str:
    """
    Replace placeholder tokens (FUNCTION/COLUMN/TABLE) with real canonicals from the graph.
    No-op if sets are empty or token not present.
    """
    R = rng or random.Random()

    tables: List[str] = []
    columns: List[str] = []
    functions: List[str] = []

    for k, v in (graph or {}).items():
        if not isinstance(v, dict):
            continue
        et = v.get("entity_type")
        if et == "table":
            tables.append(k)
        elif et == "column":
            columns.append(k)
        elif et in {"sql_actions", "postgis_actions"}:
            functions.append(k)

    if not any(t in canonical for t in ("FUNCTION", "COLUMN", "TABLE")):
        return canonical  # fast path

    def _swap(text: str, token: str, pool: List[str]) -> str:
        if token not in text or not pool:
            return text
        # Replace each occurrence independently with a random choice
        parts = text.split(token)
        out = []
        for i, p in enumerate(parts):
            out.append(p)
            if i < len(parts) - 1:
                out.append(R.choice(pool))
        return "".join(out)

    s = canonical
    s = _swap(s, "FUNCTION", functions)
    s = _swap(s, "COLUMN", columns)
    s = _swap(s, "TABLE", tables)
    return s

def _make_generator(grammar_text: str, graph: Dict[str, Any]) -> SmartGenerator:
    parser = j_make_parser(grammar_text)
    analyzer = GrammarAnalyzer(parser)
    return SmartGenerator(parser, graph, analyzer)

def j_random_integration_suite(
    graph: Dict[str, Any],
    vocabulary: Dict[str, Any],
    grammar_text: str,
    *,
    num_phrases: int = 100,
    success_threshold: float = 0.90,
    max_candidates: int = 50,
    rng_seed: Optional[int] = None,
    binder_artifact: Optional[Dict[str, Any]] = None,
    binder_mode: Optional[Dict[str, bool]] = None,
    sample_failures: int = 3,
) -> Tuple[bool, Dict[str, Any]]:
    rng = random.Random(rng_seed)
    parser = j_make_parser(grammar_text)
    gen = _make_generator(grammar_text, graph)
    reverse_map = j_build_reverse_alias_map(vocabulary, graph)

    success = 0
    fail = 0
    categories: Dict[str, int] = {}
    samples: List[Dict[str, Any]] = []

    for _ in range(num_phrases):
        canonical, _ = gen.generate(graph)
        if not canonical:
            fail += 1
            categories["generator_fail"] = categories.get("generator_fail", 0) + 1
            if len(samples) < sample_failures:
                samples.append({"canonical": None, "messy": None, "stats": {"fail_category": "generator_fail"}})
            continue

        canonical = _j_canonical_placeholder_fill(canonical, graph, rng=rng)
        messy = j_denormalize_canonical(canonical, reverse_map, rng=rng, identity_bias=0.9)

        ok, stats = j_run_full_pipeline_on_text(
            messy, graph, vocabulary, parser,
            binder_artifact=binder_artifact,
            max_candidates=max_candidates,
            binder_mode=binder_mode,
            record_binder=False,
        )

        if ok:
            success += 1
        else:
            fail += 1
            cat = stats.get("fail_category", "unknown")
            categories[cat] = categories.get(cat, 0) + 1
            if len(samples) < sample_failures:
                samples.append({"canonical": canonical, "messy": messy, "stats": stats})

    total = success + fail
    rate = (success / total) if total else 1.0
    report = {
        "ok": bool(rate >= success_threshold),
        "success_rate": rate,
        "successes": success,
        "failures": fail,
        "categories": dict(sorted(categories.items(), key=lambda kv: (-kv[1], kv[0]))),
        "samples": samples,
        "seed": rng_seed,
        "params": {
            "num_phrases": num_phrases,
            "success_threshold": success_threshold,
            "max_candidates": max_candidates,
            "binder_mode": binder_mode or {
                "strict_types": True,
                "coerce_types": False,
                "allow_ordering_funcs_in_args": False,
            },
        },
    }
    return report["ok"], report



# -----------------------------
# Lossiness & coverage audit
# -----------------------------

def j_audit_lossiness_suite(
    graph: Dict[str, Any],
    vocabulary: Dict[str, Any],
    grammar_text: str,
    *,
    num_phrases: int = 100,
    max_candidates: int = 50,
    rng_seed: Optional[int] = None,
    binder_artifact: Optional[Dict[str, Any]] = None,
    binder_mode: Optional[Dict[str, bool]] = None,
) -> Dict[str, Any]:
    rng = random.Random(rng_seed)
    parser = j_make_parser(grammar_text)
    gen = _make_generator(grammar_text, graph)
    reverse_map = j_build_reverse_alias_map(vocabulary, graph)

    hist: Dict[int, int] = {}
    categories: Dict[str, int] = {}

    for _ in range(num_phrases):
        canonical, _ = gen.generate(graph)
        if not canonical:
            categories["generator_fail"] = categories.get("generator_fail", 0) + 1
            continue

        canonical = _j_canonical_placeholder_fill(canonical, graph, rng=rng)
        messy = j_denormalize_canonical(canonical, reverse_map, rng=rng, identity_bias=0.9)

        ok, stats = j_run_full_pipeline_on_text(
            messy, graph, vocabulary, parser,
            binder_artifact=binder_artifact,
            max_candidates=max_candidates,
            binder_mode=binder_mode,
            record_binder=False,
        )
        n = int(stats.get("normalizer_candidates", 0))
        hist[n] = hist.get(n, 0) + 1
        if not ok:
            cat = stats.get("fail_category", "unknown")
            categories[cat] = categories.get(cat, 0) + 1

    return {
        "histogram": dict(sorted(hist.items())),
        "fail_categories": dict(sorted(categories.items(), key=lambda kv: (-kv[1], kv[0]))),
        "params": {"num_phrases": num_phrases, "max_candidates": max_candidates, "seed": rng_seed},
    }



# -----------------------------
# Golden-set NL suite
# -----------------------------

def j_golden_suite(
    golden_queries: List[str],
    graph: Dict[str, Any],
    vocabulary: Dict[str, Any],
    grammar_text: str,
    *,
    success_threshold: float = 1.0,
    max_candidates: int = 50,
    binder_artifact: Optional[Dict[str, Any]] = None,
    binder_mode: Optional[Dict[str, bool]] = None,
) -> Tuple[bool, Dict[str, Any]]:

    parser = j_make_parser(grammar_text)

    successes = 0
    failures: List[Dict[str, Any]] = []

    for q in golden_queries:
        ok, stats = j_run_full_pipeline_on_text(
            q, graph, vocabulary, parser,
            binder_artifact=binder_artifact,
            max_candidates=max_candidates,
            binder_mode=binder_mode,
            record_binder=False,
        )
        if ok:
            successes += 1
        else:
            failures.append({"query": q, "stats": stats})

    total = len(golden_queries) or 1
    rate = successes / total
    report = {
        "ok": bool(rate >= success_threshold),
        "success_rate": rate,
        "successes": successes,
        "total": len(golden_queries),
        "failures": failures,
        "params": {"success_threshold": success_threshold, "max_candidates": max_candidates},
    }
    return report["ok"], report


# -----------------------------
# Log builder (human readable)
# -----------------------------

def j_build_phase_j_log(
    random_report: Dict[str, Any],
    lossiness_report: Dict[str, Any],
    golden_report: Optional[Dict[str, Any]] = None,
) -> str:
    lines: List[str] = []
    # Random suite
    lines.append("--- Running I1: Random Canonical → De-Norm → Norm → Bind → Parse ---")
    rate = random_report.get("success_rate", 0.0)
    succ = random_report.get("successes", 0)
    total = succ + random_report.get("failures", 0)
    lines.append(f"  - Success Rate: {rate:.0%} ({succ}/{total})")
    cats = random_report.get("categories", {}) or {}
    if cats:
        lines.append("  - Failure categories:")
        for k, v in cats.items():
            lines.append(f"    * {k}: {v}")
    samples = random_report.get("samples", []) or []
    if samples:
        lines.append("  - Sample failures:")
        for ex in samples:
            lines.append(f"    - Canonical: {ex.get('canonical')}")
            lines.append(f"      Messy:     {ex.get('messy')}")
            lines.append(f"      Stats:     {ex.get('stats')}")

    # Lossiness
    lines.append("--- Running I2: Lossiness & Coverage Audit ---")
    hist = lossiness_report.get("histogram", {}) or {}
    if hist:
        lines.append("  - Normalizer candidate count histogram:")
        for k in sorted(hist.keys()):
            lines.append(f"      {k}: {hist[k]}")
    fcats = lossiness_report.get("fail_categories", {}) or {}
    if fcats:
        lines.append("  - Failure categories:")
        for k, v in fcats.items():
            lines.append(f"    * {k}: {v}")

    # Golden
    if golden_report is not None:
        lines.append("--- Running I3: Golden-set NL Queries ---")
        g_rate = golden_report.get("success_rate", 0.0)
        g_succ = golden_report.get("successes", 0)
        g_total = golden_report.get("total", 0)
        lines.append(f"  - Success Rate: {g_rate:.0%} ({g_succ}/{g_total})")
        fails = golden_report.get("failures", []) or []
        if fails:
            lines.append("  - Failures:")
            for f in fails[:10]:
                lines.append(f"    * Query: {f['query']}")
                lines.append(f"      Stats: {f['stats']}")
            if len(fails) > 10:
                lines.append(f"      ... +{len(fails)-10} more")

    return "\n".join(lines)
