# vbg_runtime/nl2sql_engine.py
from __future__ import annotations
import re

from typing import Any, Dict, List, Optional, Tuple

# Step-2 wrapper (normalization + list sanitizer)
from vbg_runtime.normalize_runtime import normalize_nl  # type: ignore

# Step-3 parser
from vbg_runtime.parser_runtime import make_parser  # type: ignore

# Step-4 binder
from vbg_runtime.binder_runtime import make_runtime_binder  # type: ignore

# Step-5 SQL emitter
from vbg_runtime.sql_emitter import emit_select_with_warnings, emit_select  # type: ignore

# Canonical core
from vbg_generate.canonical_core import canon_tokenize, serialize_binding  # type: ignore


_WORD_OR_COMMA = re.compile(r',|[A-Za-z0-9_.]+')


def _table_to_basenames_map(graph: Dict[str, Any]) -> Dict[str, Dict[str, str]]:
    """Build {table: {basename -> dotted}} from column nodes in the graph."""
    out: Dict[str, Dict[str, str]] = {}
    for cname, cnode in (graph or {}).items():
        if not (isinstance(cnode, dict) and cnode.get("entity_type") == "column"):
            continue
        md = (cnode.get("metadata") or {})
        t = md.get("table")
        if not t:
            continue
        base = cname.split(".", 1)[1] if "." in cname else cname
        out.setdefault(t, {})[base] = cname
    return out


def _extract_from_table(canonical: str) -> Optional[str]:
    """Very light extraction of the table after 'from' or 'of'."""
    toks = _WORD_OR_COMMA.findall(canonical or "")
    for i, t in enumerate(toks):
        if t.lower() in {"from", "of"} and (i + 1) < len(toks):
            nxt = toks[i + 1]
            if nxt not in {",", "and"}:
                return nxt
    return None


def _rewrite_basenames_in_canonical(canonical: str, base_map: Dict[str, str]) -> Tuple[str, int]:
    """Replace bare basenames with dotted canonicals using base_map. Returns (text, replacements)."""
    toks = _WORD_OR_COMMA.findall(canonical or "")
    out: List[str] = []
    replaced = 0
    LOCK = {"select", "from", "of", "and", ","}
    for t in toks:
        low = t.lower()
        if "." in t or low in LOCK:
            out.append(t); continue
        repl = base_map.get(t)
        if repl:
            out.append(repl); replaced += 1
        else:
            out.append(t)
    s = " ".join(out)
    s = re.sub(r"\s*,\s*", ", ", s)
    s = re.sub(r"\s{2,}", " ", s).strip()
    return s, replaced


def _contextualize_basenames_for_from_table(canonical: str, graph: Dict[str, Any]) -> Tuple[str, int]:
    """If the canonical uses basenames, upgrade them to dotted using the FROM table context."""
    tbl = _extract_from_table(canonical)
    if not tbl:
        return canonical, 0
    t2base = _table_to_basenames_map(graph)
    base_map = t2base.get(tbl, {})
    if not base_map:
        return canonical, 0
    return _rewrite_basenames_in_canonical(canonical, base_map)


# ---- main entrypoint ----

def nl2sql_once(
    nl_text: str,
    *,
    graph: Dict[str, Any],
    vocabulary: Dict[str, Any],
    binder_artifact: Dict[str, Any],
    parser,
    engine: str = "sqlite",
    topk: int = 5,
    case_insensitive: bool = False,   # accepted for CLI compatibility
    strict_binder: bool = False,      # lenient by default for runtime UX
) -> Dict[str, Any]:
    """
    One-shot NL → candidates → (fallback: FROM-table contextualization) → bind → parse → SQL.
    """
    candidates, nstats = normalize_nl(
        vocabulary, nl_text,
        case_insensitive=case_insensitive,
        cap_results=max(5, topk)
    )

    stats = {
        "candidates": list(candidates),
        "considered": 0,
        "bound": 0,
        "parsed": 0,
        "binder_errors": [],
        "parser_errors": [],
        "normalizer": {
            "sanitized_count": int(nstats.get("sanitized_count", 0)),
            "raw_candidates": int(nstats.get("raw_candidates", len(candidates))),
        },
    }

    if not candidates:
        return {
            "ok": False,
            "fail_category": "normalizer_zero",
            "chosen_canonical": None,
            "serialized_canonical": None,
            "sql": None,
            "warnings": [],
            "stats": stats,
        }

    binder = make_runtime_binder(graph, binder_artifact, strict=bool(strict_binder))

    for cand in candidates[: max(1, topk)]:
        stats["considered"] += 1

        # Try binding the candidate as-is
        try:
            bound = binder.bind(canon_tokenize(cand))
            stats["bound"] += 1
        except Exception as be_first:
            # Fallback: contextualize basenames using FROM table, then retry ONCE
            cand2, replaced = _contextualize_basenames_for_from_table(cand, graph)
            if cand2 != cand:
                try:
                    bound = binder.bind(canon_tokenize(cand2))
                    stats["bound"] += 1
                    cand = cand2  # promote contextualized candidate for the rest of the pipeline
                except Exception as be_second:
                    # record the more informative error (second) first
                    stats["binder_errors"].append(str(be_second))
                    stats["binder_errors"].append(str(be_first))
                    continue
            else:
                stats["binder_errors"].append(str(be_first))
                continue

        # Serialize + parse
        try:
            serialized = serialize_binding(bound)
            parser.parse(serialized)
            stats["parsed"] += 1
        except Exception as pe:
            stats["parser_errors"].append(str(pe))
            continue

        # Emit SQL (support both (sql) and (sql, warn) signatures)
        warn: Optional[str] = None
        try:
            sql = emit_select(bound, binder_artifact=binder_artifact, engine=engine)
        except TypeError:
            sql, warn = emit_select(bound, binder_artifact=binder_artifact, engine=engine)

        return {
            "ok": True,
            "fail_category": None,
            "chosen_canonical": cand,
            "serialized_canonical": serialized,
            "sql": sql,
            "warnings": ([warn] if warn else []),
            "stats": stats,
        }

    # None succeeded
    fail_cat = "binder_fail" if stats["bound"] == 0 else "parser_fail"
    return {
        "ok": False,
        "fail_category": fail_cat,
        "chosen_canonical": None,
        "serialized_canonical": None,
        "sql": None,
        "warnings": [],
        "stats": stats,
    }