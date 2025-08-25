# phase_b_base_graph.py
from __future__ import annotations
from typing import Dict, Any, Tuple, List

# -----------------------------
# Helpers
# -----------------------------

_ALLOWED_FUNC_CLASSES = {"sql_actions", "postgis_actions"}
_KEYWORD_CLASSES = {"prepositions", "logical_operators", "select_verbs", "filler_words"}

def _ensure_list_str(x) -> List[str]:
    if not x:
        return []
    if isinstance(x, list):
        return [str(s).strip().lower() for s in x if str(s).strip()]
    return []

# -----------------------------
# B1: Tables & Columns
# -----------------------------

def build_table_column_entities(schema_norm: Dict[str, Any]) -> Dict[str, Any]:
    graph: Dict[str, Any] = {}
    tables = schema_norm.get("tables") or {}
    if not isinstance(tables, dict):
        raise AssertionError("schema_norm.tables must be a dict.")

    # For indexes
    column_token_to_fqids: Dict[str, List[str]] = {}
    table_to_column_tokens: Dict[str, List[str]] = {}

    for tname, tinfo in tables.items():
        if not isinstance(tinfo, dict):
            raise AssertionError(f"Table '{tname}' spec must be a dict.")

        # Normalize aliases on the table
        t_aliases = _ensure_list_str(tinfo.get("aliases"))

        # Normalize columns: dict preferred; list allowed (→ dict stubs)
        cols_in = tinfo.get("columns") or {}
        if isinstance(cols_in, list):
            cols_norm: Dict[str, Any] = {str(c).strip(): {} for c in cols_in if str(c).strip()}
        elif isinstance(cols_in, dict):
            cols_norm = cols_in
        else:
            raise AssertionError(f"Table '{tname}' columns must be a dict or list.")

        # Create table node with empty columns-backref
        graph[tname] = {
            "entity_type": "table",
            "metadata": {
                "aliases": t_aliases,
                "columns": {},  # will hold stubs: { bare_col: {"entity_type":"column","id": fqid} }
            },
        }

        # Column nodes (namespaced)
        table_to_column_tokens[tname] = []

        for cname, cinfo in cols_norm.items():
            if not isinstance(cinfo, dict):
                cinfo = {"type": str(cinfo).strip()} if str(cinfo).strip() else {}

            c_type = cinfo.get("type")
            c_labels = _ensure_list_str(cinfo.get("labels"))
            c_aliases = _ensure_list_str(cinfo.get("aliases"))

            fqid = f"{tname}.{cname}"
            if fqid in graph:
                raise AssertionError(f"Duplicate fully-qualified column id '{fqid}' in schema_norm.")

            # Column node keyed by fqid; store bare token in metadata.canonical
            graph[fqid] = {
                "entity_type": "column",
                "metadata": {
                    "type": c_type,
                    "labels": c_labels,
                    "aliases": c_aliases,
                    "table": tname,
                    "canonical": cname,  # <-- bare column token
                },
            }

            # Back-reference in table metadata: stub with id
            graph[tname]["metadata"]["columns"][cname] = {"entity_type": "column", "id": fqid}

            # Indexes
            table_to_column_tokens[tname].append(cname)
            column_token_to_fqids.setdefault(cname, []).append(fqid)

    # Gate: every column has a valid parent table and appears in that table’s backref
    for node_id, node in graph.items():
        if node.get("entity_type") != "column":
            continue
        md = node.get("metadata") or {}
        t = md.get("table")
        bare = md.get("canonical") or node_id.split(".", 1)[-1]
        if not t or t not in graph or graph[t].get("entity_type") != "table":
            raise AssertionError(f"Column '{node_id}' missing or invalid parent table '{t}'.")
        tcols = (graph[t]["metadata"] or {}).get("columns") or {}
        stub = tcols.get(bare)
        if not (isinstance(stub, dict) and stub.get("id") == node_id):
            raise AssertionError(
                f"Table '{t}' must list column '{bare}' with stub id='{node_id}' in metadata.columns."
            )

    # Attach light catalogs for downstream binder/normalizer helpers
    graph["_catalogs"] = {
        "column_token_to_fqids": column_token_to_fqids,   # e.g. {'user_id': ['users.user_id','sales.user_id']}
        "table_to_column_tokens": table_to_column_tokens, # e.g. {'users': ['user_id','name',...]}
    }

    return graph


# -----------------------------
# B2: Functions / Operators / Keyword classes
# -----------------------------

def build_function_operator_entities(graph: Dict[str, Any], keywords_norm: Dict[str, Any]) -> Dict[str, Any]:
    # Functions
    for fname, fmeta in (keywords_norm.get("functions") or {}).items():
        graph[fname] = {
            "entity_type": "sql_actions",  # default; fix to postgis below if label or binder says so
            "metadata": {
                "aliases": _ensure_list_str(fmeta.get("aliases")),
                "label_rules": list(fmeta.get("label_rules") or []),
                "applicable_types": fmeta.get("applicable_types") or {},
            },
            "binder": fmeta.get("binder") or {},
        }
        # Heuristic: if binder/class says postgis, mark it
        b = graph[fname].get("binder") or {}
        fn_class = str(b.get("class") or "").strip().lower()
        if "postgis" in fn_class:
            graph[fname]["entity_type"] = "postgis_actions"

    # Comparison operators (optional)
    for oname, ometa in (keywords_norm.get("operators") or {}).items():
        graph[oname] = {
            "entity_type": "comparison_operators",
            "metadata": {"aliases": _ensure_list_str((ometa or {}).get("aliases"))},
        }

    # Keyword classes
    for cls in _KEYWORD_CLASSES:
        for item in (keywords_norm.get(cls) or []):
            graph[item] = {"entity_type": cls, "metadata": {"aliases": [item]}}

    # Gate: functions have aliases lists (can be empty but must exist), binder is dict if present
    for fname, node in list(graph.items()):
        if node.get("entity_type") in _ALLOWED_FUNC_CLASSES:
            md = node.get("metadata") or {}
            if "aliases" not in md or not isinstance(md["aliases"], list):
                raise AssertionError(f"Function '{fname}' missing metadata.aliases list.")
            if "binder" in node and not isinstance(node["binder"], dict):
                raise AssertionError(f"Function '{fname}' binder must be a dict if present.")

    return graph


# -----------------------------
# B3: Connectors (catalog-style, under graph meta)
# -----------------------------

def attach_connectors_catalog(graph: Dict[str, Any], keywords_norm: Dict[str, Any]) -> Dict[str, Any]:
    connectors = keywords_norm.get("connectors") or {}
    # Ensure required default connectors present (fallback surfaces)
    required = {"OF": "of", "FROM": "from", "AND": "and", "COMMA": ","}
    merged = dict(required)
    for k, v in connectors.items():
        merged[str(k).strip().upper()] = str(v).strip().lower()

    graph["_binder_meta"] = graph.get("_binder_meta") or {}
    graph["_binder_meta"]["connectors"] = [{"name": k, "surface": v} for k, v in merged.items()]

    # Gate: OF/FROM/AND exist with valid surfaces
    want = {"OF": "of", "FROM": "from", "AND": "and"}
    have = {c["name"]: c["surface"] for c in graph["_binder_meta"]["connectors"]}
    missing = [k for k, v in want.items() if have.get(k) != v]
    if missing:
        raise AssertionError(f"Missing expected connectors or surfaces: {missing}")

    return graph


# -----------------------------
# Orchestrator for Phase B
# -----------------------------

def run_phase_b(schema_norm: Dict[str, Any], keywords_norm: Dict[str, Any]) -> Dict[str, Any]:
    """
    External entrypoint for Phase B.
    Returns graph_v3 (tables/columns + functions/operators + connectors).
    """
    g1 = build_table_column_entities(schema_norm)
    g2 = build_function_operator_entities(g1, keywords_norm)
    g3 = attach_connectors_catalog(g2, keywords_norm)
    # Final gate: must have at least one table and one column
    tables = [k for k, v in g3.items() if isinstance(v, dict) and v.get("entity_type") == "table"]
    cols   = [k for k, v in g3.items() if isinstance(v, dict) and v.get("entity_type") == "column"]
    if not tables or not cols:
        raise AssertionError("Base graph must contain at least one table and one column.")
    return g3
