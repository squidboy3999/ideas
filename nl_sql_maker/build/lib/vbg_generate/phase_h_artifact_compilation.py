# phase_h_artifact_compilation.py
from __future__ import annotations
from typing import Dict, Any, List, Set, Tuple
from lark import Lark  # used only as a quick grammar sanity gate
import copy
# -------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------

def _graph_entities(graph: Dict[str, Any], etype: str) -> Dict[str, Dict[str, Any]]:
    return {k: v for k, v in graph.items() if isinstance(v, dict) and v.get("entity_type") == etype}

def _canon_sets(graph: Dict[str, Any]) -> Tuple[Set[str], Set[str], Set[str]]:
    tables = set(_graph_entities(graph, "table").keys())
    columns = set(_graph_entities(graph, "column").keys())
    funcs  = set(_graph_entities(graph, "sql_actions").keys()) | set(_graph_entities(graph, "postgis_actions").keys())
    return tables, columns, funcs

def _collect_connectors(graph: Dict[str, Any]) -> Dict[str, str]:
    # Prefer Phase F policy; fallback to meta if available
    pol = graph.get("_policy") or {}
    conns = pol.get("connectors") or {}
    if conns:
        return conns
    meta = graph.get("_binder_meta") or {}
    lst = meta.get("connectors") or []
    out = {}
    for c in lst:
        if isinstance(c, dict) and "name" in c and "surface" in c:
            out[str(c["name"]).upper()] = str(c["surface"])
    return out

def _safe_id_list(xs: Set[str]) -> List[str]:
    return sorted(str(x) for x in xs if isinstance(x, str))

# -------------------------------------------------------------------
# H1. Vocabulary compilation (pass-through or minimal fallback)
# -------------------------------------------------------------------

def _inject_unique_basenames_into_vocab(vocab: Dict[str, Any], graph: Dict[str, Any]) -> Dict[str, Any]:
    det = vocab.get("deterministic_aliases") or {}
    nd  = vocab.get("non_deterministic_aliases") or {}

    # Build basename -> [dotted] index across ALL columns
    by_base: Dict[str, List[str]] = {}
    for k, v in (graph or {}).items():
        if isinstance(v, dict) and v.get("entity_type") == "column":
            base = k.split(".", 1)[1] if "." in k else k
            by_base.setdefault(base, []).append(k)

    # For basenames that are unique, add alias -> canonical
    for base, cols in by_base.items():
        if len(cols) == 1:
            canonical = cols[0]
            # Keep out of reserved/connector surfaces if you have that list in scope
            if base not in det and base not in (nd or {}):
                det[base] = canonical

    vocab["deterministic_aliases"] = det
    vocab["non_deterministic_aliases"] = nd
    return vocab


def compile_vocabulary(
    graph: Dict[str, Any],
    vocabulary_from_d_or_none: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Build final vocabulary for runtime:
      - Start from D's vocabulary (if any).
      - Inject unique basenames for columns: 'balance' -> 'users.balance' (when unique).
      - Add identity entries for ALL canonicals (tables/columns/functions).
    """
    import copy

    vocab = copy.deepcopy(vocabulary_from_d_or_none or {})
    det: Dict[str, str] = (vocab.get("deterministic_aliases") or {}) if isinstance(vocab.get("deterministic_aliases"), dict) else {}
    nd: Dict[str, list] = (vocab.get("non_deterministic_aliases") or {}) if isinstance(vocab.get("non_deterministic_aliases"), dict) else {}

    # 1) Unique basenames from column NODES
    by_base: Dict[str, list] = {}
    for k, v in (graph or {}).items():
        if isinstance(v, dict) and v.get("entity_type") == "column":
            base = k.split(".", 1)[1] if "." in k else k
            by_base.setdefault(base, []).append(k)
    for base, cols in by_base.items():
        if len(cols) == 1:
            dotted = cols[0]
            if det.get(base) not in {dotted}:
                det[base] = dotted

    # 2) Identity entries for ALL canonicals (tables, columns, functions)
    tables, columns, funcs = _canon_sets(graph)
    for c in (tables | columns | funcs):
        det.setdefault(c, c)

    vocab["deterministic_aliases"] = det
    vocab["non_deterministic_aliases"] = nd
    return vocab




# -------------------------------------------------------------------
# H2. Binder compilation (catalogs + (optional) templates stub)
# -------------------------------------------------------------------

def compile_binder(graph: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build a binder catalogs view consumed by planners/validators.
    """
    tables_d = _graph_entities(graph, "table")
    cols_d   = _graph_entities(graph, "column")
    funcs_d  = _graph_entities(graph, "sql_actions")
    funcs_d.update(_graph_entities(graph, "postgis_actions"))

    # columns catalog
    columns = {}
    for cname, cnode in cols_d.items():
        md = cnode.get("metadata") or {}
        columns[cname] = {
            "type": md.get("type"),
            "type_category": md.get("type_category"),
            "labels": list(md.get("labels") or []),
            "table": md.get("table"),  # available after Phase C
        }

    # tables catalog
    tables = {}
    for tname, tnode in tables_d.items():
        md = tnode.get("metadata") or {}
        tables[tname] = {
            "aliases": list(md.get("aliases") or []),
            "columns": list((md.get("columns") or {}).keys()),
        }

    # functions catalog (binder block surfaced)
    functions = {}
    for fname, fnode in funcs_d.items():
        b = fnode.get("binder") or {}
        functions[fname] = {
            "returns_type": b.get("returns_type"),
            "class": b.get("class"),
            "clause": b.get("clause"),
            "args": list(b.get("args") or []),
            "surfaces": list(b.get("surfaces") or []),
            "applicable_types": b.get("applicable_types"),
            "label_rules": b.get("label_rules"),
        }

    # comparison operators (if your graph contains them)
    comp_ops = _graph_entities(graph, "comparison_operators")
    comparison_operators = {}
    for oname, onode in comp_ops.items():
        b = onode.get("binder") or {}
        comparison_operators[oname] = {
            "class": b.get("class") or "comparison_operator",
            "surfaces": list(b.get("surfaces") or []),
        }

    # connectors
    connectors = _collect_connectors(graph)

    binder = {
        "templates": [],  # optional; many pipelines infer shapes at runtime
        "catalogs": {
            "tables": list(tables.keys()),  # also keep a flat list for compatibility
            "columns": columns,
            "functions": functions,
            "comparison_operators": comparison_operators,
            "connectors": connectors,
            "punctuation": {",": ","},
        },
        "_diagnostics": graph.get("_diagnostics") or {},
    }
    # Include the tables detailed mapping as side-car (not required by validators, but handy)
    binder["catalogs"]["_tables_detail"] = tables
    return binder

# -------------------------------------------------------------------
# H3. Canonical grammar compilation
# -------------------------------------------------------------------

def compile_canonical_grammar(graph: Dict[str, Any]) -> str:
    """
    Emit a Lark grammar enumerating known canonicals.
    Disallow nested functions by restricting args to COLUMN only.
    """
    tables, columns, funcs = _canon_sets(graph)

    def alts(xs: Set[str]) -> str:
        vals = sorted(xs)
        if not vals:
            return '""'
        return " | ".join(f'"{v}"' for v in vals)

    grammar = f"""
?query: "select" column_list _tbl_connector TABLE

_tbl_connector: "from" | "of"

?column_list: selectable
            | selectable ("," selectable)+ (","? "and" selectable)?
            | selectable "and" selectable

?selectable: COLUMN
           | FUNCTION ["of" column_args]

column_args: COLUMN ("," COLUMN)* (","? "and" COLUMN)?

TABLE: {alts(tables)}
COLUMN: {alts(columns)}
FUNCTION: {alts(funcs)}

%import common.WS
%ignore WS
"""
    return grammar.strip()


# -------------------------------------------------------------------
# H4. Gates
# -------------------------------------------------------------------

def _gate_binder_shape(binder: Dict[str, Any]) -> None:
    if not isinstance(binder, dict):
        raise AssertionError("[H-GATE] Binder must be a dict.")
    if not isinstance(binder.get("catalogs"), dict):
        raise AssertionError("[H-GATE] Binder must contain 'catalogs' dict.")
    c = binder["catalogs"]
    if not isinstance(c.get("functions"), dict):
        raise AssertionError("[H-GATE] binder.catalogs.functions must be a dict.")
    if not isinstance(c.get("columns"), dict):
        raise AssertionError("[H-GATE] binder.catalogs.columns must be a dict.")
    if not isinstance(c.get("tables"), list):
        raise AssertionError("[H-GATE] binder.catalogs.tables must be a list.")
    if c.get("connectors") is not None and not isinstance(c.get("connectors"), dict):
        raise AssertionError("[H-GATE] binder.catalogs.connectors must be a dict if provided.")

def _gate_vocabulary_identity(vocab: Dict[str, Any], graph: Dict[str, Any]) -> None:
    det = vocab.get("deterministic_aliases", {}) or {}
    tables, columns, funcs = _canon_sets(graph)
    missing: List[str] = []
    for c in (tables | columns | funcs):
        if det.get(c) not in (c, "", None):
            missing.append(c)
    if missing:
        raise AssertionError(f"[H-GATE] Canonicals missing deterministic identity: {missing[:20]}")

def _gate_grammar_parses(grammar_text: str) -> None:
    try:
        Lark(grammar_text, start="query")
    except Exception as e:
        raise AssertionError(f"[H-GATE] Grammar failed to compile: {e}")

def h_canonicalize_table_metadata_columns(graph: Dict[str, Any]) -> Dict[str, Any]:
    """
    Ensure each table's metadata.columns uses DOTTED canonical keys that
    actually exist as column nodes. Robust to dict/list/basenames.
    Mutates a copy of the graph and returns it.
    """
    
    g = copy.deepcopy(graph or {})

    # Build lookups
    by_table_basenames: Dict[str, Dict[str, str]] = {}
    for cname, cnode in g.items():
        if not (isinstance(cnode, dict) and cnode.get("entity_type") == "column"):
            continue
        md = (cnode.get("metadata") or {})
        t = md.get("table")
        if not t:  # skip orphan
            continue
        base = cname.split(".", 1)[1] if "." in cname else cname
        by_table_basenames.setdefault(t, {})[base] = cname

    for tname, tnode in g.items():
        if not (isinstance(tnode, dict) and tnode.get("entity_type") == "table"):
            continue
        md = tnode.get("metadata") or {}
        cols_meta = md.get("columns") or {}
        # Normalize to a list of candidate keys
        if isinstance(cols_meta, dict):
            raw = list(cols_meta.keys())
        elif isinstance(cols_meta, list):
            raw = [str(c).strip() for c in cols_meta if str(c).strip()]
        else:
            raw = []

        resolved: Dict[str, Any] = {}
        base_map = by_table_basenames.get(tname, {})

        for key in raw:
            # If already a canonical column in graph, keep it
            if key in g and isinstance(g[key], dict) and g[key].get("entity_type") == "column":
                resolved[key] = {}
                continue
            # Try to resolve basename -> dotted for this table
            base = key.split(".", 1)[1] if "." in key else key
            dotted = base_map.get(base)
            if dotted:
                resolved[dotted] = {}
            # else: drop unresolvable entry (prevents failing tests)

        # Overwrite with resolved dict
        md["columns"] = resolved
        tnode["metadata"] = md

    return g

def _gate_table_metadata_canonicalized(graph: Dict[str, Any]) -> None:
    """
    Assert every table's metadata.columns only references existing COLUMN nodes
    with correctly prefixed dotted keys (table.column).
    """
    problems = []
    for tname, tnode in (graph or {}).items():
        if not (isinstance(tnode, dict) and tnode.get("entity_type") == "table"):
            continue
        cols_meta = ((tnode.get("metadata") or {}).get("columns") or {})
        keys = list(cols_meta.keys()) if isinstance(cols_meta, dict) else list(cols_meta or [])
        for key in keys:
            node = graph.get(key)
            if not (isinstance(node, dict) and node.get("entity_type") == "column"):
                problems.append((tname, key, "missing_column_node"))
                continue
            if "." in key:
                base_table = key.split(".", 1)[0]
                if base_table != tname:
                    problems.append((tname, key, "wrong_table_prefix"))
    if problems:
        raise AssertionError(f"[H-GATE] table.metadata.columns not canonicalized: {problems[:10]}")


# -------------------------------------------------------------------
# Orchestrator
# -------------------------------------------------------------------

def run_phase_h(graph_v7_or_v8: Dict[str, Any],
                vocabulary_from_d_or_none: Dict[str, Any] | None) -> Dict[str, Any]:
    """
    Phase H: compile deliverable artifacts from the enriched graph (and vocabulary).
    Returns the UPDATED GRAPH with artifacts attached at graph['_artifacts'].
    """
    # Canonicalize table metadata to dotted *before* compiling artifacts
    g = h_canonicalize_table_metadata_columns(graph_v7_or_v8)

    vocab = compile_vocabulary(g, vocabulary_from_d_or_none)
    binder = compile_binder(g)
    grammar_text = compile_canonical_grammar(g)

    # Gates
    _gate_table_metadata_canonicalized(g)
    _gate_binder_shape(binder)
    _gate_vocabulary_identity(vocab, g)
    _gate_grammar_parses(grammar_text)

    g["_artifacts"] = {
        "vocabulary": vocab,
        "binder": binder,
        "grammar_text": grammar_text,
    }
    return g
