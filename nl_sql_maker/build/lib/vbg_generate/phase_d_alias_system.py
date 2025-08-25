# phase_d_alias_system.py
from __future__ import annotations
from typing import Dict, Any, List, Tuple, Set
import re
from collections import defaultdict

# Policy knobs
PREP_BARE = {"of", "from", "in", "on", "at"}
GENERIC_DENY = {"order by", "sort by", "by", "ascending", "descending"}
DOMAIN_PREFER_SPATIAL = {
    "contains": ("like", "st_contains"),
    "intersects": ("st_spatial_index", "st_intersects"),
    "overlaps": ("st_spatial_index", "st_intersects"),
}
ALLOWED_TYPES_FOR_PLURAL = {"table", "column"}
PLURAL_LASTWORD = {"id": "ids", "name": "names", "value": "values", "item": "items", "date": "dates"}

# -------------------------------------------
# Helpers
# -------------------------------------------

def _is_multi(alias: str) -> bool:
    return " " in alias.strip()

def _add(master: Dict[str, List[dict]], alias: str, entry: dict) -> None:
    a = (alias or "").strip().lower()
    if not a:
        return
    lst = master.setdefault(a, [])
    if entry not in lst:
        lst.append(entry)

def _graph_aliases(graph: Dict[str, Any], canonical: str) -> List[str]:
    node = graph.get(canonical, {}) or {}
    return list((node.get("metadata") or {}).get("aliases") or [])

def _entity_type(graph: Dict[str, Any], canonical: str) -> str:
    node = graph.get(canonical, {}) or {}
    return str(node.get("entity_type") or "")

def _canonicals(graph: Dict[str, Any]) -> List[str]:
    return [k for k, v in graph.items() if isinstance(v, dict) and "entity_type" in v]

def _prefix_to_longers(keys: List[str]) -> Dict[str, List[str]]:
    out: Dict[str, List[str]] = defaultdict(list)
    for k in keys:
        parts = k.split()
        if len(parts) > 1:
            out[parts[0]].append(k)
    return out

def _synthesize_plurals(master: Dict[str, List[dict]], graph: Dict[str, Any], diags: Dict[str, Any]) -> None:
    keys = list(master.keys())
    for alias in keys:
        # add only for tables/columns targets
        types_here = {m["type"] for m in master[alias]}
        if not types_here.issubset(ALLOWED_TYPES_FOR_PLURAL):
            continue
        parts = alias.split()
        if not parts:
            continue
        lw = parts[-1]
        if lw in PLURAL_LASTWORD:
            plural = " ".join(parts[:-1] + [PLURAL_LASTWORD[lw]])
            if plural not in master:
                master[plural] = list(master[alias])
                diags["plural_added"].append({"from": alias, "to": plural})

def _apply_preposition_purity(alias: str, meanings: List[dict], diags: Dict[str, Any]) -> List[dict]:
    if alias not in PREP_BARE:
        return meanings
    kept = [m for m in meanings if m["type"] == "prepositions"]
    if len(kept) < len(meanings):
        diags["preposition_conflicts"].append({"alias": alias, "dropped": len(meanings) - len(kept)})
    return kept

def _apply_domain_preference(alias: str, meanings: List[dict], diags: Dict[str, Any]) -> List[dict]:
    if alias not in DOMAIN_PREFER_SPATIAL:
        return meanings
    lose, prefer = DOMAIN_PREFER_SPATIAL[alias]
    tgts = {m["canonical"] for m in meanings}
    if lose in tgts and prefer in tgts:
        kept = [m for m in meanings if m["canonical"] != lose]
        diags["domain_conflicts"].append({"alias": alias, "dropped": lose, "kept": prefer})
        return kept
    return meanings

def _apply_prefix_protection(alias: str, meanings: List[dict], pfx_map: Dict[str, List[str]], diags: Dict[str, Any]) -> List[dict]:
    if " " in alias or alias not in pfx_map:
        return meanings
    longers = pfx_map[alias]
    before = len(meanings)
    kept = [m for m in meanings if m["type"] != "table"]
    if len(kept) < before:
        diags["prefix_collisions"].append({"alias": alias, "longer_keys": sorted(longers), "action": "dropped_table_meaning"})
    return kept

# -------------------------------------------
# D1: Collect alias universe from graph
# -------------------------------------------

_ALLOWED_TYPES = {
    "table", "column", "sql_actions", "postgis_actions",
    "select_verbs", "prepositions", "logical_operators",
    "comparison_operators", "filler_words"
}

def collect_alias_master(graph: Dict[str, Any], diags: Dict[str, Any]) -> Dict[str, List[dict]]:
    master: Dict[str, List[dict]] = defaultdict(list)

    for node_id in _canonicals(graph):
        et = _entity_type(graph, node_id)
        if et not in _ALLOWED_TYPES:
            continue

        node = graph.get(node_id) or {}
        md = node.get("metadata") or {}

        if et == "column":
            # Columns: identity & aliases map to the *bare* canonical token
            target = (md.get("canonical") or node_id.split(".", 1)[-1]).strip().lower()
            identity_surface = target
        else:
            target = node_id
            identity_surface = node_id

        # identity (surface → canonical target)
        _add(master, identity_surface, {"canonical": target, "type": et})

        # explicit aliases
        for alias in _graph_aliases(graph, node_id):
            a = alias.strip().lower()
            if not a:
                continue
            if a in GENERIC_DENY:
                diags["generic_denied"].append(a)
                continue
            _add(master, a, {"canonical": target, "type": et})

    return master

# -------------------------------------------
# D2: Clean / policy passes + collision repair
# -------------------------------------------

def apply_policy_passes(master: Dict[str, List[dict]], graph: Dict[str, Any], diags: Dict[str, Any]) -> Dict[str, List[dict]]:
    _synthesize_plurals(master, graph, diags)
    pfx = _prefix_to_longers(list(master.keys()))

    cleaned: Dict[str, List[dict]] = {}
    for alias, meanings in master.items():
        kept = list(meanings)
        kept = _apply_preposition_purity(alias, kept, diags)
        kept = _apply_domain_preference(alias, kept, diags)
        kept = _apply_prefix_protection(alias, kept, pfx, diags)
        if kept:
            cleaned[alias] = kept
    return cleaned

def _column_alias_collision_repair(graph: Dict[str, Any], diags: Dict[str, Any]) -> None:
    """
    Within each table, if two columns share an alias (case-insensitive),
    drop that alias from *those fq column nodes* and record a warning.
    Robust to table.metadata.columns being dict (with stubs), list, or missing.
    """
    from collections import defaultdict

    for tname, tnode in graph.items():
        if not isinstance(tnode, dict) or tnode.get("entity_type") != "table":
            continue

        md = tnode.get("metadata") or {}
        cols_meta = md.get("columns") or {}

        # Resolve table's columns → fqids
        fqids: List[str] = []

        if isinstance(cols_meta, dict):
            for bare, stub in cols_meta.items():
                if isinstance(stub, dict) and "id" in stub:
                    fqids.append(stub["id"])
                else:
                    # best-effort fallback
                    fqids.append(f"{tname}.{bare}")
        elif isinstance(cols_meta, list):
            for bare in cols_meta:
                b = str(bare).strip()
                if b:
                    fqids.append(f"{tname}.{b}")
        else:
            # fallback: scan graph
            for cname, cnode in graph.items():
                if (
                    isinstance(cnode, dict)
                    and cnode.get("entity_type") == "column"
                    and (cnode.get("metadata") or {}).get("table") == tname
                ):
                    fqids.append(cname)

        # Build alias → [fqid,...] bucket
        bucket: Dict[str, List[str]] = defaultdict(list)
        for fid in fqids:
            cnode = graph.get(fid) or {}
            a_list = list(((cnode.get("metadata") or {}).get("aliases") or []))
            for a in a_list:
                al = str(a).strip().lower()
                if al:
                    bucket[al].append(fid)

        # Remove collided aliases from each participating fq column
        for alias, owners in bucket.items():
            if len(owners) <= 1:
                continue
            for fid in owners:
                cnode = graph.get(fid) or {}
                md_c = cnode.get("metadata") or {}
                md_c["aliases"] = [x for x in (md_c.get("aliases") or []) if str(x).strip().lower() != alias]
                cnode["metadata"] = md_c
                graph[fid] = cnode
            diags["alias_collisions"].append({
                "table": tname,
                "alias": alias,
                "columns": owners,  # fqids
                "action": "dropped_from_all"
            })



# -------------------------------------------
# D3: Build vocabulary
# -------------------------------------------

def _dedupe(seq: List[str]) -> List[str]:
    seen: Set[str] = set()
    out: List[str] = []
    for s in seq:
        if s not in seen:
            seen.add(s); out.append(s)
    return out

def build_vocabulary(graph: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build the normalization vocabulary:
      - deterministic_aliases: alias -> single canonical target
      - non_deterministic_aliases: alias -> [multiple canonical targets]
    Column targets are the *bare* column tokens (not fqids), e.g., 'user_id'.
    Filler words map deterministically to "".
    Also includes connector surfaces mirrored from graph['_binder_meta'].
    """
    diags: Dict[str, Any] = {
        "generic_denied": [],
        "prefix_collisions": [],
        "preposition_conflicts": [],
        "domain_conflicts": [],
        "plural_added": [],
        "alias_collisions": [],
    }

    # 1) Per-table column alias collision repair (edits column nodes in-place)
    _column_alias_collision_repair(graph, diags)

    # 2) Collect alias universe (columns contribute *bare* targets; others use their canonical ids)
    master = collect_alias_master(graph, diags)

    # 3) Apply policy passes
    cleaned = apply_policy_passes(master, graph, diags)

    # 4) Build det/nd maps from cleaned alias→meanings
    det: Dict[str, Any] = {}
    nd: Dict[str, List[str]] = {}

    for alias, meanings in cleaned.items():
        # Filler words → deterministic empty mapping
        if meanings and all(m.get("type") == "filler_words" for m in meanings):
            det[alias] = ""
            continue

        targets = _dedupe(sorted(m.get("canonical") for m in meanings if m.get("canonical")))
        if not targets:
            continue

        # Multi-word alias or true ambiguity → ND; otherwise deterministic
        if _is_multi(alias) or len(targets) > 1:
            nd[alias] = targets
        else:
            det[alias] = targets[0]

    # 5) Identity pass to ensure coverage even if policy passes filtered something out
    #    - columns → bare canonical (not fqid)
    #    - filler_words → ""
    #    - others → identity to themselves
    seen_bare_cols: Set[str] = set()
    for node_id, node in graph.items():
        if not isinstance(node, dict) or "entity_type" not in node:
            continue
        et = node.get("entity_type")
        md = node.get("metadata") or {}

        if et == "column":
            bare = (md.get("canonical") or node_id.split(".", 1)[-1]).strip().lower()
            if bare and bare not in seen_bare_cols:
                det.setdefault(bare, bare)
                seen_bare_cols.add(bare)
        elif et == "filler_words":
            det.setdefault(node_id, "")
        else:
            det.setdefault(node_id, node_id)

    # 6) Add connectors (for downstream binder checks)
    connector_list = []
    try:
        meta = graph.get("_binder_meta") or {}
        connector_list = list(meta.get("connectors") or [])
    except Exception:
        connector_list = []
    connector_map = {c["name"]: c["surface"] for c in connector_list if isinstance(c, dict)}

    # 7) Package vocabulary
    vocab = {
        "deterministic_aliases": det,
        "non_deterministic_aliases": nd,
        "connectors": connector_list,     # list of {name, surface}
        "connector_map": connector_map,   # convenience for validators
        "_diagnostics": diags,
    }
    return vocab


# -------------------------------------------
# D4: Gates / validations
# -------------------------------------------

def _gate_vocab_covers_graph_aliases(graph: Dict[str, Any], vocab: Dict[str, Any]) -> None:
    """
    Ensure that graph-provided aliases are covered by the vocabulary,
    except those intentionally dropped by policy passes (recorded in diagnostics)
    or handled as connectors.
    """
    # Collect all aliases that appear in the graph
    graph_aliases: Set[str] = set()
    for canonical in _canonicals(graph):
        for a in _graph_aliases(graph, canonical):
            s = (a or "").strip().lower()
            if s:
                graph_aliases.add(s)

    det_keys = set((vocab.get("deterministic_aliases") or {}).keys())
    nd_keys  = set((vocab.get("non_deterministic_aliases") or {}).keys())
    present  = det_keys | nd_keys

    # Build exemption set from diagnostics (policy-dropped) and connector surfaces
    diags = vocab.get("_diagnostics") or {}
    exempt: Set[str] = set()

    # Generic denied aliases (e.g., "order by", "sort by")
    for s in diags.get("generic_denied", []):
        if isinstance(s, str) and s.strip():
            exempt.add(s.strip().lower())

    # Prefix collisions (single-word prefixes like "order", "user" losing table meaning)
    for rec in diags.get("prefix_collisions", []):
        if isinstance(rec, dict):
            a = str(rec.get("alias", "")).strip().lower()
            if a:
                exempt.add(a)

    # Preposition conflicts (if any were dropped)
    for rec in diags.get("preposition_conflicts", []):
        if isinstance(rec, dict):
            a = str(rec.get("alias", "")).strip().lower()
            if a:
                exempt.add(a)

    # Connector surfaces (they live in vocab['connectors']/connector_map, not det/nd)
    conn_map = vocab.get("connector_map") or {}
    for surface in conn_map.values():
        s = (surface or "").strip().lower()
        if s:
            exempt.add(s)

    # Final check: everything not present must be explainably exempt
    missing = sorted(a for a in graph_aliases if a not in present and a not in exempt)
    if missing:
        raise AssertionError(f"[D-GATE] {len(missing)} aliases from graph missing in vocabulary (first 20): {missing[:20]}")


def _gate_identity_presence(graph: Dict[str, Any], vocab: Dict[str, Any]) -> None:
    det = vocab.get("deterministic_aliases") or {}
    missing: Set[str] = set()

    for node_id, node in graph.items():
        if not isinstance(node, dict) or "entity_type" not in node:
            continue
        et = node.get("entity_type")
        md = node.get("metadata") or {}

        if et == "column":
            bare = (md.get("canonical") or node_id.split(".", 1)[-1]).strip().lower()
            val = det.get(bare, None)
            if val not in (bare, None):
                missing.add(bare)
        elif et == "filler_words":
            val = det.get(node_id, None)
            if val not in ("", None):
                missing.add(node_id)
        else:
            val = det.get(node_id, None)
            if val not in (node_id, None):
                missing.add(node_id)

    if missing:
        raise AssertionError(f"[D-GATE] Missing canonical identity entries (first 20): {sorted(list(missing))[:20]}")

def _gate_connectors_present(vocab: Dict[str, Any]) -> None:
    conn_map = vocab.get("connector_map") or {}
    want = {"OF": "of", "FROM": "from", "AND": "and"}
    for k, v in want.items():
        if conn_map.get(k) != v:
            raise AssertionError(f"[D-GATE] Connector {k}→{v} not present in vocabulary.connector_map")

# -------------------------------------------
# Orchestrator
# -------------------------------------------

def run_phase_d(graph_v4: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
    External entrypoint for Phase D.
    Input: enriched graph (Phase C)
    Output: (vocabulary, graph_updated)
      - vocabulary: deterministic / non_deterministic alias maps, connectors, diagnostics
      - graph_updated: graph with alias collision repairs applied
    """
    graph_updated = graph_v4  # mutate in place by design
    vocabulary = build_vocabulary(graph_updated)

    # Gates
    _gate_vocab_covers_graph_aliases(graph_updated, vocabulary)
    _gate_identity_presence(graph_updated, vocabulary)
    _gate_connectors_present(vocabulary)

    return vocabulary, graph_updated
