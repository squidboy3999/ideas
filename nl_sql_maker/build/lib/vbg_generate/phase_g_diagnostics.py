# phase_g_diagnostics.py
from __future__ import annotations
from typing import Dict, Any, List, Tuple, Set
from collections import defaultdict, Counter

# -------------------------------------------------------------------
# Utilities
# -------------------------------------------------------------------

def _graph_entities(graph: Dict[str, Any], etype: str) -> Dict[str, Dict[str, Any]]:
    return {k: v for k, v in graph.items() if isinstance(v, dict) and v.get("entity_type") == etype}

def _graph_tables(graph: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    return _graph_entities(graph, "table")

def _graph_columns(graph: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    return _graph_entities(graph, "column")

def _graph_functions(graph: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    f = _graph_entities(graph, "sql_actions")
    f.update(_graph_entities(graph, "postgis_actions"))
    return f

def _push_diag(graph: Dict[str, Any], bucket: str, payload: Any) -> None:
    graph["_diagnostics"] = graph.get("_diagnostics") or {}
    graph["_diagnostics"].setdefault(bucket, []).append(payload)

def _policy_connectors(graph: Dict[str, Any]) -> Dict[str, str]:
    pol = graph.get("_policy") or {}
    return pol.get("connectors") or {}

def _compat_meta(fn_node: Dict[str, Any]) -> Dict[str, Any]:
    b = fn_node.get("binder") or {}
    return b.get("compatibility") or {}

# -------------------------------------------------------------------
# G1. Schema diagnostics
# -------------------------------------------------------------------

def _diagnose_schema_coherence(graph: Dict[str, Any]) -> Dict[str, Any]:
    tables = _graph_tables(graph)
    columns = _graph_columns(graph)

    # Table → column names (declared under table.metadata.columns)
    table_cols = {
        t: list(((node.get("metadata") or {}).get("columns") or {}).keys())
        for t, node in tables.items()
    }

    # Column → owning table (if available from a Phase C/D enrichment)
    col_owner: Dict[str, str] = {}
    for cname, cnode in columns.items():
        md = cnode.get("metadata") or {}
        owner = md.get("table")
        if owner:
            col_owner[cname] = owner

    # Orphans (columns not found under any table’s declared list)
    declared_cols = {c for cols in table_cols.values() for c in cols}
    orphans = [c for c in columns.keys() if c not in declared_cols and col_owner.get(c) not in table_cols]

    # Empty tables
    empty_tables = [t for t, cols in table_cols.items() if not cols]

    # Cross-check inconsistencies: col_owner references missing/unknown table
    bad_owners = [c for c, t in col_owner.items() if t not in tables]

    diag = {
        "empty_tables": empty_tables,
        "orphan_columns": orphans,
        "bad_column_owners": bad_owners,
        "table_column_counts": {t: len(cols) for t, cols in table_cols.items()},
    }
    _push_diag(graph, "phase_g.schema", diag)
    return diag

# -------------------------------------------------------------------
# G2. Alias & vocabulary diagnostics
# -------------------------------------------------------------------

def _diagnose_alias_coverage(graph: Dict[str, Any], vocabulary: Dict[str, Any]) -> Dict[str, Any]:
    det = vocabulary.get("deterministic_aliases", {}) or {}
    nd  = vocabulary.get("non_deterministic_aliases", {}) or {}
    vocab_keys = set(det.keys()) | set(nd.keys())

    # Gather all aliases declared in graph metadata
    graph_aliases: Set[str] = set()
    alias_to_canonicals: Dict[str, Set[str]] = defaultdict(set)

    for cname, node in graph.items():
        if not isinstance(node, dict):
            continue
        md = node.get("metadata") or {}
        als = md.get("aliases") or []
        for a in als:
            s = str(a).strip().lower()
            if not s:
                continue
            graph_aliases.add(s)
            alias_to_canonicals[s].add(cname)

    missing_in_vocab = sorted(a for a in graph_aliases if a not in vocab_keys)

    # Ambiguous aliases (map to multiple canonicals in graph)
    ambiguous_in_graph = sorted([a for a, cs in alias_to_canonicals.items() if len(cs) > 1])

    # Aliases in vocab that point to unknown canonical
    unknown_targets: List[str] = []
    canonicals_in_graph = set(k for k in graph.keys() if isinstance(graph.get(k), dict))
    for a, tgt in det.items():
        if tgt not in ("", None) and tgt not in canonicals_in_graph:
            unknown_targets.append(f"{a}→{tgt}")
    for a, tgts in nd.items():
        for t in (tgts or []):
            if t not in ("", None) and t not in canonicals_in_graph:
                unknown_targets.append(f"{a}→{t}")

    diag = {
        "missing_in_vocab": missing_in_vocab,
        "ambiguous_in_graph": ambiguous_in_graph,
        "unknown_vocab_targets": unknown_targets[:200],
        "alias_fanout": {a: len(cs) for a, cs in alias_to_canonicals.items()},
    }
    _push_diag(graph, "phase_g.aliases", diag)
    return diag

# -------------------------------------------------------------------
# G3. Function coverage & argument health
# -------------------------------------------------------------------

def _diagnose_function_coverage(graph: Dict[str, Any]) -> Dict[str, Any]:
    funcs = _graph_functions(graph)

    zeros: List[str] = []
    low: List[Tuple[str, int]] = []
    per_fn_counts: Dict[str, int] = {}
    for f, fnode in funcs.items():
        comp = _compat_meta(fnode)
        n = int(comp.get("total_compatible_columns", 0))
        per_fn_counts[f] = n
        if n == 0 and (fnode.get("binder") or {}).get("args", ["column"]) != []:
            zeros.append(f)
        elif 0 < n < 2:
            low.append((f, n))

    diag = {
        "zero_compat_functions": zeros,
        "low_compat_functions": low[:50],
        "compat_counts": per_fn_counts,
    }
    _push_diag(graph, "phase_g.function_compat", diag)
    return diag

# -------------------------------------------------------------------
# G4. Policy / connector health
# -------------------------------------------------------------------

def _diagnose_policy_and_connectors(graph: Dict[str, Any], vocabulary: Dict[str, Any]) -> Dict[str, Any]:
    want = {"OF": "of", "FROM": "from", "AND": "and"}
    have = _policy_connectors(graph)
    missing = [k for k, v in want.items() if have.get(k) != v]

    # Reserved tokens sanity: ensure they’re not general-purpose aliases
    reserved = set((graph.get("_policy") or {}).get("reserved_tokens") or [])
    det = vocabulary.get("deterministic_aliases", {}) or {}
    nd  = vocabulary.get("non_deterministic_aliases", {}) or {}
    vocab_keys = set(det.keys()) | set(nd.keys())
    reserved_misused = sorted([a for a in vocab_keys if a in reserved])

    diag = {
        "connectors_missing_or_wrong": missing,
        "reserved_alias_misuse": reserved_misused[:100],
    }
    _push_diag(graph, "phase_g.policy", diag)
    return diag

# -------------------------------------------------------------------
# Gates (fatal sanity)
# -------------------------------------------------------------------

def _gate_nonempty_schema(graph: Dict[str, Any]) -> None:
    if not _graph_tables(graph) or not _graph_columns(graph):
        raise AssertionError("[G-GATE] Schema must contain at least one table and one column.")

def _gate_no_orphan_columns(diag_schema: Dict[str, Any]) -> None:
    orphans = diag_schema.get("orphan_columns", [])
    if orphans:
        raise AssertionError(f"[G-GATE] Orphan columns detected (not declared under any table): {orphans[:10]}")

def _gate_connectors_present(diag_policy: Dict[str, Any]) -> None:
    missing = diag_policy.get("connectors_missing_or_wrong", [])
    if missing:
        raise AssertionError(f"[G-GATE] Missing/incorrect connectors in policy: {missing}")

# -------------------------------------------------------------------
# Orchestrator
# -------------------------------------------------------------------

def run_phase_g(graph_v6_or_v7: Dict[str, Any], vocabulary: Dict[str, Any]) -> Dict[str, Any]:
    """
    Phase G: compute diagnostics & heatmaps, attach them to graph['_diagnostics'].
    Fails fast on structural issues; otherwise records warnings for downstream review.
    """
    _gate_nonempty_schema(graph_v6_or_v7)

    diag_schema = _diagnose_schema_coherence(graph_v6_or_v7)
    diag_alias  = _diagnose_alias_coverage(graph_v6_or_v7, vocabulary)
    diag_funcs  = _diagnose_function_coverage(graph_v6_or_v7)
    diag_policy = _diagnose_policy_and_connectors(graph_v6_or_v7, vocabulary)

    # Fatal gates (keep small)
    _gate_no_orphan_columns(diag_schema)
    _gate_connectors_present(diag_policy)

    return graph_v6_or_v7
