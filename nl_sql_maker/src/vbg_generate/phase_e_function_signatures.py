# phase_e_function_signatures.py
from __future__ import annotations
from typing import Dict, Any, List, Tuple, Optional, Set
import math

# -------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------

_SQL_FUNC_TYPES: Set[str] = {"sql_actions", "postgis_actions"}
_ZERO_ARITY_FUNCS: Set[str] = {
    # add real zero-arity canonicals here if you use them
    # e.g. "now", "current_timestamp"
}
# Basic return-type and label-rule overrides (extend as needed)
_FUNC_RETURNS_OVERRIDES: Dict[str, str] = {
    "count": "int",
    "sum": "float",
    "avg": "float",
    "min": "same_as_arg",
    "max": "same_as_arg",
    "st_distance": "float",
    "st_area": "float",
    "st_length": "float",
    "st_perimeter": "float",
    "st_x": "float",
    "st_y": "float",
    "st_buffer": "geometry",
    "st_centroid": "geometry",
    "st_union": "geometry",
    "st_intersects": "bool",
    "st_contains": "bool",
    "st_within": "bool",
    "st_crosses": "bool",
    "st_overlaps": "bool",
    "st_touches": "bool",
    "st_simplify": "geometry",
    "st_transform": "geometry",
    "st_geometrytype": "text",
}

_GEOMETRY_FUNCS: Set[str] = {
    k for k, v in _FUNC_RETURNS_OVERRIDES.items()
    if k.startswith("st_")
}

_ORDERING_FUNCS: Set[str] = {"order_by_asc", "order_by_desc"}

def _iter_funcs(graph: Dict[str, Any]):
    for cname, cnode in graph.items():
        if isinstance(cnode, dict) and cnode.get("entity_type") in _SQL_FUNC_TYPES:
            yield cname, cnode

def _iter_columns(graph: Dict[str, Any]):
    for cname, cnode in graph.items():
        if isinstance(cnode, dict) and cnode.get("entity_type") == "column":
            yield cname, cnode

def _iter_tables(graph: Dict[str, Any]):
    for tname, tnode in graph.items():
        if isinstance(tnode, dict) and tnode.get("entity_type") == "table":
            yield tname, tnode

def _labels(md: Dict[str, Any]) -> Set[str]:
    return set(md.get("labels") or [])

def _table_columns(graph: Dict[str, Any]) -> Dict[str, List[str]]:
    out: Dict[str, List[str]] = {}
    for tname, tnode in _iter_tables(graph):
        cols = list(((tnode.get("metadata") or {}).get("columns") or {}).keys())
        out[tname] = cols
    return out

def _compat_col(graph: Dict[str, Any], col_name: str, fn_meta: Dict[str, Any]) -> bool:
    """
    Simple shared compatibility (same spirit as runtime core):
      - applicable_types: ANY-satisfiable (case-insensitive types)
      - label_rules: ["id", "not postgis"] style
    Empty fn_meta => permissive.
    """
    cnode = graph.get(col_name, {})
    cmd = (cnode.get("metadata") or {})
    if not fn_meta:
        return True

    # types
    ok_type = True
    app = fn_meta.get("applicable_types")
    if isinstance(app, dict) and app:
        ctype = str(cmd.get("type", "")).lower()
        ok_type = False
        for _var, allowed in app.items():
            if not isinstance(allowed, list): continue
            allowed_lc = {str(a).lower() for a in allowed}
            if "any" in allowed_lc or ctype in allowed_lc:
                ok_type = True; break

    # labels
    ok_label = True
    labs = {str(x).lower() for x in cmd.get("labels", []) if isinstance(x, str)}
    for r in fn_meta.get("label_rules", []) or []:
        rr = str(r)
        if rr.startswith("not "):
            if rr[4:].lower() in labs:
                ok_label = False; break
        else:
            if rr.lower() not in labs:
                ok_label = False; break

    return bool(ok_type and ok_label)

def _ensure_binder_block(fn_node: Dict[str, Any]) -> Dict[str, Any]:
    b = fn_node.get("binder") or {}
    if not isinstance(b, dict):
        b = {}
    fn_node["binder"] = b
    return b

def _guess_returns_type(fname: str, first_arg_type: Optional[str]) -> str:
    ret = _FUNC_RETURNS_OVERRIDES.get(fname)
    if ret == "same_as_arg":
        return first_arg_type or "any"
    return ret or "any"

def _default_label_rules_for(fname: str) -> List[str]:
    # Spatial functions usually require geometry/postgis columns
    if fname in _GEOMETRY_FUNCS:
        return ["postgis"]
    return []

def _default_applicable_types_for(fname: str) -> Dict[str, List[str]]:
    # Flexible default: most accept any column; spatial prefer geometry.
    if fname in _GEOMETRY_FUNCS:
        return {"column": ["geometry", "geography", "any"]}
    # aggregates and numerics often accept any numeric/text; keep permissive
    return {"column": ["any"]}

def _is_zero_arity(fname: str) -> bool:
    return fname in _ZERO_ARITY_FUNCS

def _push_diag(graph: Dict[str, Any], key: str, payload: Any) -> None:
    graph["_diagnostics"] = graph.get("_diagnostics") or {}
    graph["_diagnostics"].setdefault(key, []).append(payload)

# -------------------------------------------------------------------
# E1. Normalize/complete function signatures
# -------------------------------------------------------------------

def normalize_function_signatures(graph: Dict[str, Any]) -> Dict[str, Any]:
    """
    Ensure every function node has a populated binder block:
      - returns_type
      - class (aggregate|scalar|predicate|spatial|ordering|transform)
      - clause (select|where|both|having|order_by)
      - args ([], ["column"], ["columns"], etc.)
      - applicable_types / label_rules (compatibility)
      - surfaces (optional)
    """
    for fname, fnode in _iter_funcs(graph):
        b = _ensure_binder_block(fnode)

        # args: default 1 column, unless zero-arity or explicit list present
        if "args" not in b or not isinstance(b["args"], list):
            b["args"] = [] if _is_zero_arity(fname) else ["column"]

        # returns_type: derive from override or first-arg type (if single column)
        first_arg_type = None
        if b["args"] == ["column"]:
            # Not guaranteed, but try to infer later in coverage pass
            first_arg_type = None
        b.setdefault("returns_type", _guess_returns_type(fname, first_arg_type))

        # class & clause defaults
        if fname in _ORDERING_FUNCS:
            b.setdefault("class", "ordering")
            b.setdefault("clause", "order_by")
            b.setdefault("args", ["column"])  # ordering usually references a column
        elif fname in _GEOMETRY_FUNCS:
            b.setdefault("class", "spatial")
            b.setdefault("clause", "both")   # allow in select/where for now
        elif fname in {"count", "sum", "avg", "min", "max"}:
            b.setdefault("class", "aggregate")
            b.setdefault("clause", "select")
        elif fnode.get("entity_type") == "postgis_actions":
            b.setdefault("class", "spatial")
            b.setdefault("clause", "both")
        else:
            # generic scalar/predicate
            if b.get("returns_type") == "bool":
                b.setdefault("class", "predicate")
                b.setdefault("clause", "where")
            else:
                b.setdefault("class", "scalar")
                b.setdefault("clause", "select")

        # compatibility defaults
        b.setdefault("applicable_types", _default_applicable_types_for(fname))
        if "label_rules" not in b:
            b["label_rules"] = _default_label_rules_for(fname)

        # surfaces: keep whatever is there; ensure list
        surfaces = b.get("surfaces")
        if isinstance(surfaces, str):
            b["surfaces"] = [surfaces]
        elif isinstance(surfaces, list):
            b["surfaces"] = [str(s) for s in surfaces]
        else:
            b["surfaces"] = []

        fnode["binder"] = b

    return graph

# -------------------------------------------------------------------
# E2. Compatibility coverage summary
# -------------------------------------------------------------------

def compute_compatibility_coverage(graph: Dict[str, Any]) -> Dict[str, Any]:
    """
    For each function, compute:
      - total_compatible_columns
      - per_table_compatible_counts[t]
      - has_any_compat
    Stored under node['binder']['compatibility'].
    """
    tcols = _table_columns(graph)
    # collect columns meta cache
    for fname, fnode in _iter_funcs(graph):
        b = _ensure_binder_block(fnode)
        meta = {
            "total_compatible_columns": 0,
            "per_table_compatible_counts": {},
            "has_any_compat": True,   # default true for zero-arity
        }
        if _is_zero_arity(fname) or b.get("args", ["column"]) == []:
            b["compatibility"] = meta
            fnode["binder"] = b
            continue

        fn_meta = {
            "applicable_types": b.get("applicable_types"),
            "label_rules": b.get("label_rules"),
        }
        total = 0
        per_t: Dict[str, int] = {}
        for t, cols in tcols.items():
            c = 0
            for col in cols:
                if _compat_col(graph, col, fn_meta):
                    c += 1
            per_t[t] = c
            total += c

        meta["total_compatible_columns"] = total
        meta["per_table_compatible_counts"] = per_t
        meta["has_any_compat"] = bool(total > 0)

        b["compatibility"] = meta
        fnode["binder"] = b

    return graph

# -------------------------------------------------------------------
# E3. Gates
# -------------------------------------------------------------------
# -------------------------------------------------------------------
# Helper: type family mapping used by schema-aware coverage gate
# -------------------------------------------------------------------

def _family_of_type(t: str) -> str:
    """
    Coarse family bucketing for both column types and applicable_types
    declared by functions. Keeps things permissive and robust across
    heterogeneous schemas.
    """
    if not t:
        return "text"
    s = str(t).strip().lower()

    # Spatial
    if s.startswith("geometry") or s.startswith("geography") or s == "geometry":
        return "geometry"

    # Numerics
    if s in {"int", "integer", "bigint", "smallint", "tinyint"}:
        return "int"
    if s in {"float", "double", "real", "decimal", "numeric"}:
        return "float"

    # Temporal
    if s in {"datetime", "timestamp"}:
        return "datetime"
    if s in {"date", "time"}:
        return s  # keep "date" or "time"

    # Booleans
    if s in {"bool", "boolean"}:
        return "bool"

    # Fallback
    return "text"


def _collect_schema_facets(graph: Dict[str, Any]) -> Dict[str, Any]:
    """
    Summarize what's actually available in the schema so gates can
    avoid penalizing functions that are irrelevant to this dataset.
    """
    type_families: Set[str] = set()
    raw_types: Set[str] = set()
    labels: Set[str] = set()

    for cname, cnode in _iter_columns(graph):
        md = cnode.get("metadata") or {}
        t = str(md.get("type") or "").strip().lower()
        raw_types.add(t)
        type_families.add(_family_of_type(t))
        for lab in (md.get("labels") or []):
            labels.add(str(lab).strip().lower())

    has_geometry = (
        "geometry" in type_families
        or "geometry" in raw_types
        or "geography" in raw_types
        or "postgis" in labels
    )

    return {
        "type_families": type_families,  # e.g., {"int","float","text","geometry",...}
        "raw_types": raw_types,          # raw strings as stored on columns
        "labels": labels,                # union of all column labels
        "has_geometry": has_geometry,
    }

def _gate_every_function_has_signature(graph: Dict[str, Any]) -> None:
    missing: List[str] = []
    for fname, fnode in _iter_funcs(graph):
        b = fnode.get("binder") or {}
        if not isinstance(b, dict):
            missing.append(fname); continue
        for key in ("returns_type", "class", "clause", "args"):
            if key not in b:
                missing.append(fname); break
    if missing:
        raise AssertionError(f"[E-GATE] Functions missing basic signatures: {missing[:20]}")

def _gate_ordering_funcs_marked(graph: Dict[str, Any]) -> None:
    bad: List[str] = []
    for fname in _ORDERING_FUNCS:
        node = graph.get(fname)
        if not node: continue
        cls = ((node.get("binder") or {}).get("class"))
        clause = ((node.get("binder") or {}).get("clause"))
        if cls != "ordering" or clause != "order_by":
            bad.append(fname)
    if bad:
        raise AssertionError(f"[E-GATE] Ordering functions not properly marked: {bad}")

def _gate_minimum_viable_query_surface(graph: Dict[str, Any]) -> None:
    """
    The schema must be queryable with *no* functions:
      - at least one table
      - at least one column
      - and at least one column correctly linked to a parent table.
    """
    has_table = False
    has_column = False
    for _, tnode in _iter_tables(graph):
        has_table = True
        break
    for _, cnode in _iter_columns(graph):
        has_column = True
        break
    if not has_table or not has_column:
        raise AssertionError("[E-GATE] Minimum query surface missing: need at least one table and one column.")

    # Ensure we can form 'select <column> from <table>'
    linked_ok = False
    for cname, cnode in _iter_columns(graph):
        t = (cnode.get("metadata") or {}).get("table")
        if t and t in graph and (graph[t] or {}).get("entity_type") == "table":
            linked_ok = True
            break
    if not linked_ok:
        raise AssertionError("[E-GATE] No column is linked to a valid parent table; cannot form a basic SELECT.")


def _warn_low_function_coverage(
    graph: Dict[str, Any],
) -> None:
    """
    Non-fatal diagnostic: report functions that *could* apply to this schema
    but currently have zero compatible columns. Irrelevant functions (e.g.,
    spatial when the schema has no geometry) are ignored from the warning.
    """
    facets = _collect_schema_facets(graph)
    present_families = facets["type_families"]
    has_geometry = facets["has_geometry"]

    considered, zeros = 0, 0
    ignored_irrelevant: List[str] = []
    zero_list: List[str] = []

    for fname, fnode in _iter_funcs(graph):
        b = fnode.get("binder") or {}
        args = b.get("args", ["column"])
        if not args:
            continue  # zero-arity → not a column-coverage concern

        comp = (b.get("compatibility") or {})
        if comp.get("has_any_compat", False):
            considered += 1
            continue

        app = b.get("applicable_types") or {}
        label_rules = {str(x).strip().lower() for x in (b.get("label_rules") or [])}

        # spatial irrelevance
        needs_geometry = (
            ("postgis" in label_rules) or
            any(
                any((_family_of_type(typ) == "geometry") for typ in (allowed or []))
                for allowed in app.values() if isinstance(allowed, list)
            )
        )
        if needs_geometry and not has_geometry:
            ignored_irrelevant.append(fname)
            continue

        # pure type mismatch irrelevance: declared families have no overlap with schema families
        declared_fams: Set[str] = set()
        for allowed in app.values():
            if isinstance(allowed, list):
                declared_fams.update(
                    _family_of_type(x) for x in allowed if str(x).strip().lower() != "any"
                )
        if declared_fams and declared_fams.isdisjoint(present_families):
            ignored_irrelevant.append(fname)
            continue

        # considered and zero
        considered += 1
        zeros += 1
        zero_list.append(fname)

    _push_diag(graph, "phase_e.function_coverage", {
        "considered": considered,
        "zeros": zeros,
        "zero_examples": zero_list[:15],
        "ignored_irrelevant": ignored_irrelevant[:15],
        "present_families": sorted(present_families),
    })



# -------------------------------------------------------------------
# Small coercion helper (add near the top of this file)
# -------------------------------------------------------------------
def _coerce_graph_input(obj: Any) -> Dict[str, Any]:
    """
    Accept either:
      - a graph dict, or
      - a (vocabulary, graph) tuple from Phase D,
    and return the graph dict. Raise a clear error otherwise.
    """
    if isinstance(obj, dict):
        return obj
    if isinstance(obj, tuple) and len(obj) == 2:
        # Phase D returns (vocabulary, graph_updated)
        _, maybe_graph = obj
        if isinstance(maybe_graph, dict):
            return maybe_graph
    raise AssertionError(
        "Phase E expected a graph dict or (vocabulary, graph) tuple from Phase D."
    )

# -------------------------------------------------------------------
# Orchestrator
# -------------------------------------------------------------------
def run_phase_e(graph_v4_or_v5: Dict[str, Any]) -> Dict[str, Any]:
    """
    External entrypoint for Phase E.
    Input: enriched graph (after Phase C/D). For robustness, also accepts
           the (vocabulary, graph) tuple returned by Phase D.
    Output: graph updated with normalized function signatures + compatibility summaries.
    """
    g = _coerce_graph_input(graph_v4_or_v5)  # <-- unwrap if a Phase D tuple
    g = normalize_function_signatures(g)
    g = compute_compatibility_coverage(g)

    # Hard gates (schema-driven or correctness)
    _gate_every_function_has_signature(g)
    _gate_ordering_funcs_marked(g)
    _gate_minimum_viable_query_surface(g)

    # Soft diagnostic (no exception)
    _warn_low_function_coverage(g)

    return g
