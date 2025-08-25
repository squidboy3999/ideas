# vbg_runtime/config.py
from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Any, Dict, Optional, Tuple


@dataclass(frozen=True)
class RuntimeConfig:
    engine: str = "sqlite"
    topk: int = 5
    case_insensitive: bool = False
    strict_binding: bool = True       # strict_types=True; coerce_types=False
    execute_sql: bool = False
    limit_rows: Optional[int] = None
    db: Optional[str] = None          # informational; used for defaults


def _clamp(n: int, lo: int, hi: int) -> int:
    return max(lo, min(hi, n))


def apply_config_defaults(
    config: RuntimeConfig,
    artifacts: Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any], str] | None = None
) -> RuntimeConfig:
    """
    Apply sensible defaults and heuristics based on provided configuration and environment.
    - If db path endswith '.db' (or '.sqlite'), default engine='sqlite'.
    - Clamp topk to [1, 50].
    - If execute_sql and limit_rows is None, set a safe default (1000).
    """
    c = config

    # Infer engine from DB path if not explicitly overridden
    if c.db:
        low = c.db.lower()
        if low.endswith(".db") or low.endswith(".sqlite"):
            if c.engine not in {"sqlite", "postgres"}:
                c = replace(c, engine="sqlite")
        # For postgres URIs you might infer 'postgres' here if desired.

    # Clamp topk
    if (c.topk is None) or (c.topk <= 0) or (c.topk > 50):
        c = replace(c, topk=_clamp(int(c.topk or 5), 1, 50))

    # Default row limit when executing SQL
    if c.execute_sql and (c.limit_rows is None):
        c = replace(c, limit_rows=1000)

    return c
