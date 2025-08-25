# runtime/normalize_runtime.py
from __future__ import annotations

import re
from typing import Any, Dict, Iterable, List, Tuple


# Try a few import paths to align with your repo layout
try:
    from vbg_runtime.normalizer import normalize_text  # your canonical normalizer
except Exception:
    # fallback if package is namespaced differently in your project
    from src.vbg_runtime.normalizer import normalize_text  # type: ignore


_AND_WORD_RE = re.compile(r"\bAND\b", re.IGNORECASE)
_OXFORD_RE = re.compile(r"\s*,\s*and\s+", re.IGNORECASE)
_BARE_AND_RE = re.compile(r"(?<=\S)\s+and\s+(?=\S)", re.IGNORECASE)
_MULTI_COMMA_RE = re.compile(r"\s*,\s*,\s*")
_SPACE_COMMA_RE = re.compile(r"\s*,\s*")


def sanitize_list_connectors(text: str) -> str:
    """
    Rewrite list connectors so binders that expect comma-separated lists are happy.
    - "A and B"           => "A, B"
    - "A, B, and C"       => "A, B, C"
    The canonical shape does not use 'and' for anything except list joins, so
    a global rewrite is acceptable in this constrained grammar.
    """
    if not text:
        return text

    s = text

    # 1) Oxford comma: ", and" -> ", "
    s = _OXFORD_RE.sub(", ", s)

    # 2) Bare "and" between tokens -> ", "
    s = _BARE_AND_RE.sub(", ", s)

    # 3) Normalize commas to a single ", "
    s = _SPACE_COMMA_RE.sub(", ", s)

    # 4) Coalesce accidental duplicates
    while True:
        s2 = _MULTI_COMMA_RE.sub(", ", s)
        if s2 == s:
            break
        s = s2

    # 5) Cleanup multiple spaces
    s = " ".join(s.split())
    return s


def normalize_nl(
    vocabulary: Dict[str, Dict[str, Any]],
    nl_text: str,
    *,
    case_insensitive: bool = False,
    cap_results: int = 50,
) -> Tuple[List[str], Dict[str, Any]]:
    """
    NL → canonical candidates with list-connector sanitation applied.
    Returns (candidates, stats) where:
      - candidates: de-duped list of sanitized canonicals (original token order preserved)
      - stats: {'raw_candidates': int, 'sanitized_count': int}
    Never raises on empty results; returns ([], stats).
    """
    raw_candidates = normalize_text(
        vocabulary,
        nl_text,
        case_insensitive=case_insensitive,
        cap_results=cap_results,
    )

    seen = set()
    out: List[str] = []
    for c in raw_candidates or []:
        s = sanitize_list_connectors(c)
        if s and s not in seen:
            seen.add(s)
            out.append(s)

    stats = {
        "raw_candidates": len(raw_candidates or []),
        "sanitized_count": len(out),
    }
    return out, stats
