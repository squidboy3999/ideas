# phase_f_global_policy_block.py
from __future__ import annotations
from typing import Dict, Any, List, Tuple, Set

# Reuse / shadow policy knobs intentionally (align with validators)
PREP_BARE = {"of", "from", "in", "on", "at"}
OF_CANONICALS = {"distinct", "avg", "sum", "st_distance"}
DOMAIN_PREFER_SPATIAL = {
    "contains": ("like", "st_contains"),
    "intersects": ("st_spatial_index", "st_intersects"),
    "overlaps": ("st_spatial_index", "st_intersects"),
}
RESERVED_TOKENS: Set[str] = {",", "&&", "||", "==", "!=", "<>", "<=", ">=", "=", "!", "<", ">"}
ARG_FUNCTION_DENY: Set[str] = {"order_by_asc", "order_by_desc"}
_ALLOWED_SYMBOL_TARGET_TYPES = {"comparison_operators", "logical_operators", "prepositions"}

# -------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------
# Add near the other helpers


def _alias_targets(graph: Dict[str, Any], vocabulary: Dict[str, Any], alias: str) -> List[Tuple[str, str]]:
    """
    Returns [(canonical, entity_type), ...] that the alias maps to, using graph to discover types.
    Handles both deterministic and non-deterministic vocab entries. Skips empty ('') filler maps.
    """
    det = (vocabulary.get("deterministic_aliases") or {})
    nd  = (vocabulary.get("non_deterministic_aliases") or {})

    canonicals: List[str] = []
    if alias in det:
        c = det.get(alias, None)
        if isinstance(c, str) and c.strip() != "":
            canonicals.append(c)
    if alias in nd:
        for c in (nd.get(alias) or []):
            if isinstance(c, str) and c.strip() != "":
                canonicals.append(c)

    out: List[Tuple[str, str]] = []
    for c in canonicals:
        node = graph.get(c) or {}
        etype = str(node.get("entity_type") or "")
        out.append((c, etype))
    return out

def _build_symbolic_alias_map(graph: Dict[str, Any], vocabulary: Dict[str, Any]) -> Dict[str, List[str]]:
    """
    For convenience, publish a subset of vocabulary where alias is a reserved symbol
    and all targets are within allowed operator-ish types.
    """
    out: Dict[str, List[str]] = {}
    keys = _all_alias_keys(vocabulary)
    for alias in keys:
        if alias not in RESERVED_TOKENS:
            continue
        targets = _alias_targets(graph, vocabulary, alias)
        types = {t for _, t in targets}
        if targets and types.issubset(_ALLOWED_SYMBOL_TARGET_TYPES):
            out[alias] = sorted({c for c, _ in targets})
    return out

def _connectors_from_graph(graph: Dict[str, Any]) -> Dict[str, str]:
    meta = graph.get("_binder_meta") or {}
    conns = meta.get("connectors") or []
    mapping = {}
    for c in conns:
        if isinstance(c, dict) and "name" in c and "surface" in c:
            mapping[str(c["name"]).upper()] = str(c["surface"])
    return mapping

def _all_alias_keys(vocab: Dict[str, Any]) -> Set[str]:
    det = vocab.get("deterministic_aliases", {}) or {}
    nd  = vocab.get("non_deterministic_aliases", {}) or {}
    return set(det.keys()) | set(nd.keys())

def _graph_entities(graph: Dict[str, Any], etype: str) -> Set[str]:
    return {k for k, v in graph.items() if isinstance(v, dict) and v.get("entity_type") == etype}

# -------------------------------------------------------------------
# F1. Build policy block
# -------------------------------------------------------------------

def build_global_policy_block(graph: Dict[str, Any], vocabulary: Dict[str, Any]) -> Dict[str, Any]:
    policy = {
        "connectors": _connectors_from_graph(graph),
        "reserved_tokens": sorted(RESERVED_TOKENS),
        "preposition_purity": {"bare_prepositions": sorted(PREP_BARE)},
        "domain_preferences": DOMAIN_PREFER_SPATIAL,
        "of_surface_canonicals": sorted(OF_CANONICALS),
        "arg_function_deny": sorted(ARG_FUNCTION_DENY),
        "canonicals": {
            "tables": sorted(_graph_entities(graph, "table")),
            "columns": sorted(_graph_entities(graph, "column")),
            "functions": sorted(_graph_entities(graph, "sql_actions") | _graph_entities(graph, "postgis_actions")),
            "comparison_operators": sorted(_graph_entities(graph, "comparison_operators")),
        },
        "vocabulary": {
            "deterministic_keys": sorted((vocabulary.get("deterministic_aliases") or {}).keys()),
            "non_deterministic_keys": sorted((vocabulary.get("non_deterministic_aliases") or {}).keys()),
        },
        # NEW: publish symbol alias map (only allowed ones)
        "symbolic_alias_map": _build_symbolic_alias_map(graph, vocabulary),
    }
    graph["_policy"] = policy
    return graph


# -------------------------------------------------------------------
# F2. Gates / policy sanity
# -------------------------------------------------------------------

def _gate_connectors_present(policy: Dict[str, Any]) -> None:
    want = {"OF": "of", "FROM": "from", "AND": "and"}
    have = policy.get("connectors") or {}
    missing = [k for k, v in want.items() if have.get(k) != v]
    if missing:
        raise AssertionError(f"[F-GATE] Missing/incorrect connectors in policy.connectors: {missing}")

def _gate_reserved_token_hygiene(vocabulary: Dict[str, Any], graph: Dict[str, Any]) -> None:
    """
    Reserved tokens are allowed as aliases *only if* they resolve exclusively to
    operator-ish categories (comparison_operators, logical_operators, prepositions).
    Everything else is a gate failure.
    """
    keys = _all_alias_keys(vocabulary)
    offenders: List[str] = []

    for alias in sorted(keys):
        if alias not in RESERVED_TOKENS:
            continue
        targets = _alias_targets(graph, vocabulary, alias)
        if not targets:
            offenders.append(alias)
            continue
        types = {t for _, t in targets}
        # If any target type is not in allowed categories -> violation
        if not types or not types.issubset(_ALLOWED_SYMBOL_TARGET_TYPES):
            offenders.append(alias)

    if offenders:
        raise AssertionError(f"[F-GATE] Reserved tokens mapped to non-operator targets: {offenders[:20]}")


def _gate_arg_function_deny_in_vocab(vocabulary: Dict[str, Any]) -> None:
    # Ensure that forbidden-in-args functions appear as deterministics to themselves,
    # but NOT inside multi-word aliases like "order by" (which should map to order_by_asc)
    det = vocabulary.get("deterministic_aliases", {}) or {}
    nd  = vocabulary.get("non_deterministic_aliases", {}) or {}

    for f in ARG_FUNCTION_DENY:
        # identity OK
        if det.get(f) not in (f, "", None):
            raise AssertionError(f"[F-GATE] Arg-denied function '{f}' missing deterministic identity.")
    # Optional: ensure phrases like "order by" stay in ND (generator already denies GENERIC_DENY)
    # No hard gate here; vocabulary build should have filtered them.

# -------------------------------------------------------------------
# Orchestrator
# -------------------------------------------------------------------

def run_phase_f(graph_v5_or_v6: Dict[str, Any], vocabulary: Dict[str, Any]) -> Dict[str, Any]:
    g = graph_v5_or_v6
    g = build_global_policy_block(g, vocabulary)

    # Gates
    _gate_connectors_present(g.get("_policy") or {})
    _gate_reserved_token_hygiene(vocabulary, g)  # <-- pass graph now
    _gate_arg_function_deny_in_vocab(vocabulary)

    return g

