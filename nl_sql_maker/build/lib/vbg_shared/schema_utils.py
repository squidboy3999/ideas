# src/vbg_shared/schema_utils.py
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple


def _is_table_node(node: Dict[str, Any]) -> bool:
    return isinstance(node, dict) and node.get("entity_type") == "table"


def _is_column_node(node: Dict[str, Any]) -> bool:
    return isinstance(node, dict) and node.get("entity_type") == "column"


def _is_function_node(node: Dict[str, Any]) -> bool:
    # Functions arrive as "sql_actions" or "postgis_actions" in the H-graph
    return isinstance(node, dict) and node.get("entity_type") in {"sql_actions", "postgis_actions"}


def _node_md(node: Dict[str, Any]) -> Dict[str, Any]:
    md = node.get("metadata") if isinstance(node, dict) else {}
    return md if isinstance(md, dict) else {}


def _table_of_column(node: Dict[str, Any]) -> Optional[str]:
    md = _node_md(node)
    t = md.get("table")
    return t if isinstance(t, str) and t else None


def list_tables(graph: Dict[str, Any]) -> List[str]:
    return sorted([k for k, v in (graph or {}).items() if _is_table_node(v)])


def list_columns(graph: Dict[str, Any]) -> List[str]:
    return sorted([k for k, v in (graph or {}).items() if _is_column_node(v)])


def list_functions(graph: Dict[str, Any]) -> List[str]:
    return sorted([k for k, v in (graph or {}).items() if _is_function_node(v)])


def table_to_columns(graph: Dict[str, Any]) -> Dict[str, List[str]]:
    """
    Return table -> [canonical dotted column ids].
    Robust to table.metadata.columns being dict, list, basenames, or missing.
    If a table’s columns are listed by basename, upgrade them to dotted canonicals
    when a matching dotted canonical exists in the graph.
    """
    dotted_by_table: Dict[str, List[str]] = {}
    basenames_by_table: Dict[str, Dict[str, str]] = {}
    for cname, cnode in (graph or {}).items():
        if not isinstance(cnode, dict) or cnode.get("entity_type") != "column":
            continue
        md = cnode.get("metadata") or {}
        t = md.get("table")
        if not t:
            continue
        dotted_by_table.setdefault(t, []).append(cname)
        base = cname.split(".", 1)[1] if "." in cname else cname
        basenames_by_table.setdefault(t, {})[base] = cname

    out: Dict[str, List[str]] = {}
    for tname, tnode in (graph or {}).items():
        if not isinstance(tnode, dict) or tnode.get("entity_type") != "table":
            continue

        md = tnode.get("metadata") or {}
        cols_meta = md.get("columns") or {}

        # Normalize table.metadata.columns -> list of candidate keys
        if isinstance(cols_meta, dict):
            raw = list(cols_meta.keys())
        elif isinstance(cols_meta, list):
            raw = [str(c).strip() for c in cols_meta if str(c).strip()]
        else:
            raw = []

        resolved: List[str] = []
        if not raw:
            resolved = list(dotted_by_table.get(tname, []))
        else:
            base_map = basenames_by_table.get(tname, {})
            for key in raw:
                if key in (graph or {}):
                    if (graph[key] or {}).get("entity_type") == "column":
                        resolved.append(key)
                        continue
                # Try basename→dotted for this table
                base = key.split(".", 1)[1] if "." in key else key
                dotted = base_map.get(base)
                if dotted:
                    resolved.append(dotted)
                else:
                    # keep as-is (unlikely to bind, but visible)
                    resolved.append(key)

        out[tname] = resolved

    return out

def _col_md(graph: Dict[str, Any], col: str) -> Dict[str, Any]:
    node = (graph or {}).get(col, {}) or {}
    md = node.get("metadata", {}) if isinstance(node, dict) else {}
    return md if isinstance(md, dict) else {}


def is_geometry_col(graph: Dict[str, Any], col: str) -> bool:
    md = _col_md(graph, col)
    if str(md.get("type_category", "")).lower() in {"geometry", "geography"}:
        return True
    labs = {str(x).lower() for x in (md.get("labels") or []) if isinstance(x, str)}
    return "postgis" in labs


def is_numeric_col(graph: Dict[str, Any], col: str) -> bool:
    """
    Heuristic numeric detector from metadata.
    """
    md = _col_md(graph, col)
    cat = str(md.get("type_category", "")).lower()
    if cat in {"integer", "int", "float", "double", "decimal", "numeric", "number"}:
        return True
    # Optional label-based fallback
    labs = {str(x).lower() for x in (md.get("labels") or []) if isinstance(x, str)}
    if {"numeric", "number", "integer", "float", "decimal"} & labs:
        return True
    return False


def get_global_unique_basenames(graph: Dict[str, Any]) -> Set[str]:
    """
    Bases that map to exactly one canonical column across the whole graph.
    Useful for safe basename variants.
    """
    by_base: Dict[str, int] = {}
    for cname, cnode in (graph or {}).items():
        if not isinstance(cnode, dict) or cnode.get("entity_type") != "column":
            continue
        base = cname.split(".", 1)[1] if "." in cname else cname
        by_base[base] = by_base.get(base, 0) + 1
    return {b for b, n in by_base.items() if n == 1}

def _binder_class_clause_from_graph(graph: Dict[str, Any], fn_id: str) -> Tuple[str, str]:
    """
    For function node in the graph (if present), pull binder.{class, clause}.
    Returns lowercased (klass, clause) or ('','') when unknown.
    """
    node = (graph or {}).get(fn_id) or {}
    binder_md = node.get("binder") if isinstance(node, dict) else None
    if not isinstance(binder_md, dict):
        return "", ""
    klass = str(binder_md.get("class", "")).lower()
    clause = str(binder_md.get("clause", "")).lower()
    return klass, clause


def _binder_class_clause_from_artifact(binder_artifact: Dict[str, Any], fn_id: str) -> Tuple[str, str]:
    """
    Same idea as above, but from binder_artifact.catalogs.functions[fn_id] if available.
    """
    catalogs = binder_artifact.get("catalogs") if isinstance(binder_artifact, dict) else {}
    fns = catalogs.get("functions") if isinstance(catalogs, dict) else {}
    meta = (fns or {}).get(fn_id) or {}
    if not isinstance(meta, dict):
        return "", ""
    klass = str(meta.get("class", "")).lower()
    clause = str(meta.get("clause", "")).lower()
    return klass, clause


_DEFAULT_DENY = {"group_by", "having", "limit", "order_by_asc", "order_by_desc"}


def select_friendly_functions(graph_or_binder: Dict[str, Any]) -> List[str]:
    """
    Filter out clause/ordering-like functions; return those suitable for SELECT lists.
    Accepts either the Phase-H graph or the binder artifact. Prefers binder metadata.
    """
    # Try binder first
    if isinstance(graph_or_binder, dict) and "catalogs" in graph_or_binder:
        catalogs = graph_or_binder.get("catalogs") or {}
        fns = (catalogs.get("functions") or {}).keys()
        deny = {"group_by", "having", "limit", "order_by_asc", "order_by_desc", "where", "distinct"}
        out = []
        for fn in fns:
            meta = (catalogs.get("functions") or {}).get(fn, {}) or {}
            klass = str(meta.get("class", "")).lower()
            clause = str(meta.get("clause", "")).lower()
            if fn in deny:
                continue
            if klass == "ordering" or clause in {"order_by", "having", "where", "distinct"}:
                continue
            out.append(fn)
        return sorted(out)

    # Fallback: look in graph nodes
    out = []
    for k, v in (graph_or_binder or {}).items():
        if not isinstance(v, dict):
            continue
        if v.get("entity_type") in {"sql_actions", "postgis_actions"}:
            b = v.get("binder") or {}
            klass = str(b.get("class", "")).lower()
            clause = str(b.get("clause", "")).lower()
            if klass == "ordering" or clause in {"order_by", "having", "where", "distinct"}:
                continue
            out.append(k)
    return sorted(out)
