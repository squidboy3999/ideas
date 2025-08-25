# vbg_runtime/diagnostics.py
from __future__ import annotations

from typing import Any, Dict, List


def _first_line(s: str) -> str:
    if not isinstance(s, str):
        return ""
    # Avoid leaking stack traces or multi-line internals
    return s.splitlines()[0].strip()


def _summarize_bucket(res: Dict[str, Any]) -> str:
    cat = (res or {}).get("fail_category")
    if cat == "normalizer_zero":
        return "No normalization candidates were produced from the input."
    if cat == "binder_fail":
        errs: List[str] = (res.get("stats", {}) or {}).get("binder_errors", []) or []
        detail = _first_line(errs[0]) if errs else "Binding failed for all candidates."
        return f"Binding failed: {detail}"
    if cat == "parser_fail":
        errs: List[str] = (res.get("stats", {}) or {}).get("parser_errors", []) or []
        detail = _first_line(errs[0]) if errs else "Parsing failed for all candidates."
        return f"Parsing failed: {detail}"
    if cat:
        return f"Failed ({cat})."
    return "Unknown failure."


def format_error(error_result: Dict[str, Any]) -> str:
    """
    Human-readable one-liner (or two) for errors with 'bucket' context and
    a short, safe detail (no stack traces).
    """
    lines: List[str] = []
    cat = (error_result or {}).get("fail_category", "unknown_fail")
    lines.append(f"[FAIL:{cat}] {_summarize_bucket(error_result)}")

    # Helpful hints (short)
    stats = (error_result or {}).get("stats", {}) or {}
    cands = stats.get("candidates") or []
    if cands:
        lines.append(f"- candidates tried: {min(len(cands), stats.get('considered', 0))}/{len(cands)}")
    if stats.get("normalizer"):
        san = int(stats["normalizer"].get("sanitized_count", 0) or 0)
        raw = int(stats["normalizer"].get("raw_candidates", 0) or 0)
        lines.append(f"- normalization: raw={raw} sanitized={san}")
    return "\n".join(lines)


def format_result(result: Dict[str, Any], emit_mode: str = "both", debug: bool = False) -> str:
    """
    Pretty-print success or error for console output.
    On success, prints canonical/SQL depending on emit_mode and, if debug,
    appends top-k stats and warnings.
    """
    if not result or not result.get("ok"):
        return format_error(result or {})

    can = result.get("serialized_canonical") or result.get("chosen_canonical") or ""
    sql = result.get("sql") or ""
    warnings = result.get("warnings") or []
    stats = result.get("stats", {}) or {}

    # Primary body
    if emit_mode == "canonical":
        lines = [can]
    elif emit_mode == "sql":
        lines = [sql]
    elif emit_mode == "tokens":
        # Defer tokenization to the CLI if needed; keep consistent with CLI behavior.
        lines = [can]
    else:  # both
        lines = [can, sql]

    # Debug/observability tails
    if debug:
        considered = int(stats.get("considered", 0) or 0)
        bound = int(stats.get("bound", 0) or 0)
        parsed = int(stats.get("parsed", 0) or 0)
        ccount = len(stats.get("candidates", []) or [])
        nstats = stats.get("normalizer", {}) or {}
        san = int(nstats.get("sanitized_count", 0) or 0)
        raw = int(nstats.get("raw_candidates", 0) or 0)

        lines.append("-- diagnostics")
        lines.append(f"candidates={ccount} considered={considered} bound={bound} parsed={parsed}")
        if raw or san:
            # Explicit note when lists were sanitized (requested in tests)
            note = "lists sanitized" if san > 0 else "no list sanitation"
            lines.append(f"normalizer: raw={raw} sanitized={san} ({note})")

        if warnings:
            lines.append(f"warnings: {warnings}")

    return "\n".join(lines)
