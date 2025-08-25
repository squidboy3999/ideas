#phase_c_enriching_and_profiling.py
from __future__ import annotations
import re
from typing import Dict, Any, List, Tuple, Set

# ---------------------------------------------------------
# Small helpers
# ---------------------------------------------------------

_ID_RE = re.compile(r"(^|_)id$")
_LAT_RE = re.compile(r"(?:^|_)lat(?:itude)?$")
_LON_RE = re.compile(r"(?:^|_)lon|(?:^|_)lng|(?:^|_)long(?:itude)?$")
_DATE_RE = re.compile(r"(?:^|_)(date|day|dob)$")
_TIME_RE = re.compile(r"(?:^|_)(time|timestamp|updated_at|created_at)$")
_GEO_NAME_RE = re.compile(r"(geom|geometry|geog|location|point|polygon|multipolygon|linestring)")

_LOW_CARD_HINTS = {"status", "type", "category", "region", "zone", "state", "source"}
_BOOLEAN_HINTS = {"is_", "has_", "flag", "active", "enabled"}

def _iter_columns(graph: Dict[str, Any]):
    for cname, cnode in graph.items():
        if isinstance(cnode, dict) and cnode.get("entity_type") == "column":
            yield cname, cnode

def _iter_tables(graph: Dict[str, Any]):
    for tname, tnode in graph.items():
        if isinstance(tnode, dict) and tnode.get("entity_type") == "table":
            yield tname, tnode

def _labels(md: Dict[str, Any]) -> Set[str]:
    return set((md.get("labels") or []))

def _add_label(md: Dict[str, Any], lab: str) -> None:
    labs = set(md.get("labels") or [])
    labs.add(lab)
    md["labels"] = sorted(labs)

def _push_diag(graph: Dict[str, Any], key: str, payload: Any) -> None:
    graph["_diagnostics"] = graph.get("_diagnostics") or {}
    graph["_diagnostics"].setdefault(key, []).append(payload)

# ---------------------------------------------------------
# C1: Type categories and spatial awareness
# ---------------------------------------------------------

def enrich_type_categories(graph: Dict[str, Any]) -> Dict[str, Any]:
    """
    Adds/repairs 'type_category' for columns from their 'type'.
    Adds 'postgis' label for obvious geometry columns by type or *name* hints.
    """
    for fqid, cnode in _iter_columns(graph):
        md = cnode.get("metadata") or {}
        ctype = (md.get("type") or "").lower()
        bare = (md.get("canonical") or fqid.split(".", 1)[-1]).lower()

        # type_category
        if "type_category" not in md:
            if ctype in {"int", "integer", "bigint", "smallint", "tinyint"}:
                md["type_category"] = "int"
            elif ctype in {"float", "double", "real", "decimal", "numeric"}:
                md["type_category"] = "float"
            elif ctype in {"date", "datetime", "timestamp", "time"}:
                md["type_category"] = "datetime" if "time" in ctype or "stamp" in ctype else "date"
            elif ctype in {"geometry", "geography"} or _GEO_NAME_RE.search(bare):
                md["type_category"] = "geometry"
            elif ctype in {"bool", "boolean"}:
                md["type_category"] = "bool"
            else:
                md["type_category"] = "text"

        # spatial label
        if md.get("type_category") == "geometry" or ctype in {"geometry", "geography"}:
            _add_label(md, "postgis")

        cnode["metadata"] = md
    return graph

# ---------------------------------------------------------
# C2: Semantic labels (id/lat/lon/date/time) and boolean/low-cardinality hints
# ---------------------------------------------------------

def enrich_semantic_labels(graph: Dict[str, Any]) -> Dict[str, Any]:
    for fqid, cnode in _iter_columns(graph):
        md = cnode.get("metadata") or {}
        bare = (md.get("canonical") or fqid.split(".", 1)[-1]).lower()

        if _ID_RE.search(bare):
            _add_label(md, "id")

        if _LAT_RE.search(bare):
            _add_label(md, "latitude")
        if _LON_RE.search(bare):
            _add_label(md, "longitude")

        if _DATE_RE.search(bare):
            _add_label(md, "date")
        if _TIME_RE.search(bare):
            _add_label(md, "time")

        # Low-cardinality heuristic
        if any(h in bare for h in _LOW_CARD_HINTS):
            _add_label(md, "low_cardinality")

        # Boolean heuristic
        if any(bare.startswith(pref) for pref in ("is_", "has_")) or any(h in bare for h in _BOOLEAN_HINTS):
            _add_label(md, "boolean_like")

        cnode["metadata"] = md
    return graph


# ---------------------------------------------------------
# C3: Heuristic FK detection (name-based)
# ---------------------------------------------------------

def infer_fk_relationships(graph: Dict[str, Any]) -> Dict[str, Any]:
    """
    Suggest likely FKs using '_id' patterns and table names.
    Adds graph['_relationships']['fk']: {from_col: <fqid>, to_table, confidence, reason}
    """
    table_names = {t for t, _ in _iter_tables(graph)}
    rels: List[Dict[str, Any]] = []

    for fqid, cnode in _iter_columns(graph):
        md = cnode.get("metadata") or {}
        bare = (md.get("canonical") or fqid.split(".", 1)[-1]).lower()

        if not _ID_RE.search(bare):
            continue

        base = re.sub(_ID_RE, "", bare).strip("_")
        if not base:
            # generic 'id' → probably PK of its own table
            tname = md.get("table")
            if tname:
                rels.append({"from_col": fqid, "to_table": tname, "confidence": 0.5, "reason": "generic_id"})
            continue

        # try exact, plural, singular
        candidates = {base, base + "s"}
        if base.endswith("s"):
            candidates.add(base[:-1])

        for t in sorted(candidates):
            if t in table_names:
                rels.append({"from_col": fqid, "to_table": t, "confidence": 0.8, "reason": "name_match"})

    if rels:
        graph["_relationships"] = graph.get("_relationships") or {}
        graph["_relationships"]["fk"] = rels
        _push_diag(graph, "fk_inferred", rels)
    return graph

# ---------------------------------------------------------
# C4: Selectivity / profile hints (static heuristics)
# ---------------------------------------------------------

def attach_profile_hints(graph: Dict[str, Any]) -> Dict[str, Any]:
    """
    Adds lightweight profile hints:
      - selectivity: high for id/primary-ish; medium default; low for low_cardinality.
      - groupable: True for low_cardinality; False for geometry; True otherwise.
    """
    for cname, cnode in _iter_columns(graph):
        md = cnode.get("metadata") or {}
        labs = _labels(md)
        profile = md.get("profile") or {}

        if "id" in labs:
            profile["selectivity"] = "high"
        elif "low_cardinality" in labs:
            profile["selectivity"] = "low"
        else:
            profile.setdefault("selectivity", "medium")

        if md.get("type_category") == "geometry":
            profile["groupable"] = False
        else:
            profile.setdefault("groupable", "low_cardinality" in labs or "id" in labs)

        md["profile"] = profile
        cnode["metadata"] = md
    return graph

# ---------------------------------------------------------
# C5: Gates / validations
# ---------------------------------------------------------

def _gate_columns_have_tables(graph: Dict[str, Any]) -> None:
    for cname, cnode in _iter_columns(graph):
        t = (cnode.get("metadata") or {}).get("table")
        if not t or t not in graph or graph[t].get("entity_type") != "table":
            raise AssertionError(f"[C-GATE] Column '{cname}' has missing/invalid parent table '{t}'.")

def _gate_connectors_preserved(graph: Dict[str, Any]) -> None:
    meta = graph.get("_binder_meta") or {}
    conns = meta.get("connectors")
    if not isinstance(conns, list) or not conns:
        raise AssertionError("[C-GATE] Connectors missing after enrichment.")
    want = {"OF": "of", "FROM": "from", "AND": "and"}
    have = {c.get("name"): c.get("surface") for c in conns if isinstance(c, dict)}
    missing = [k for k, v in want.items() if have.get(k) != v]
    if missing:
        raise AssertionError(f"[C-GATE] Expected connectors not found/preserved: {missing}")

# ---------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------

def run_phase_c(graph_v3: Dict[str, Any]) -> Dict[str, Any]:
    """
    External entrypoint for Phase C (enriching & profiling).
    Input: graph_v3 from Phase B.
    Output: graph_v4 with enriched metadata and relationship hints.
    """
    g = graph_v3  # mutate in place by convention

    g = enrich_type_categories(g)
    g = enrich_semantic_labels(g)
    g = infer_fk_relationships(g)
    g = attach_profile_hints(g)

    # Gates
    _gate_columns_have_tables(g)
    _gate_connectors_preserved(g)

    return g
