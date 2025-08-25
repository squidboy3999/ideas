import os
import re
import yaml
import pytest
from vbg_generate.canonical_core import serialize_binding, canon_tokenize

from vbg_generate.phase_i_helpers import (
    i_build_parser,
    i_make_relaxed_binder,
    i_roundtrip_one,
)

OUT_DIR = os.environ.get("VGB_OUT_DIR", "out")

def _load_yaml(path):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

@pytest.mark.skipif(not os.path.exists(os.path.join(OUT_DIR, "h_graph_with_artifacts.yaml")),
                    reason="Phase H artifact not found in OUT_DIR (set VGB_OUT_DIR)")
def test_graph_table_columns_are_canonical():
    g = _load_yaml(os.path.join(OUT_DIR, "h_graph_with_artifacts.yaml"))
    problems = []
    for tname, tnode in (g or {}).items():
        if not isinstance(tnode, dict) or tnode.get("entity_type") != "table":
            continue
        cols_meta = ((tnode.get("metadata") or {}).get("columns") or {})
        keys = list(cols_meta.keys()) if isinstance(cols_meta, dict) else list(cols_meta or [])
        for key in keys:
            # Must exist as a column node
            node = g.get(key)
            if not (isinstance(node, dict) and node.get("entity_type") == "column"):
                problems.append((tname, key, "missing_column_node"))
            # If dotted, table prefix must match metadata.table
            if "." in key:
                base_table = key.split(".", 1)[0]
                if base_table != tname:
                    problems.append((tname, key, "wrong_table_prefix"))
    assert not problems, f"Table metadata columns not canonical or mismatched: {problems[:10]}"

@pytest.mark.skipif(not os.path.exists(os.path.join(OUT_DIR, "h_graph_with_artifacts.yaml")),
                    reason="Phase H artifact not found in OUT_DIR")
def test_column_nodes_match_table_metadata():
    g = _load_yaml(os.path.join(OUT_DIR, "h_graph_with_artifacts.yaml"))
    problems = []
    for cname, cnode in (g or {}).items():
        if not isinstance(cnode, dict) or cnode.get("entity_type") != "column":
            continue
        md = cnode.get("metadata") or {}
        t = md.get("table")
        if not t or t not in g or (g[t] or {}).get("entity_type") != "table":
            problems.append((cname, "invalid_parent"))
            continue
        tcols = ((g[t].get("metadata") or {}).get("columns") or {})
        if cname not in tcols:
            problems.append((cname, "not_listed_in_parent"))
    assert not problems, f"Columns not correctly linked/listed in table metadata: {problems[:10]}"

@pytest.mark.skipif(not os.path.exists(os.path.join(OUT_DIR, "h_vocabulary.yaml")),
                    reason="Phase H vocabulary not found in OUT_DIR")
def test_vocabulary_has_unique_basename_aliases():
    vocab = _load_yaml(os.path.join(OUT_DIR, "h_vocabulary.yaml"))
    g = _load_yaml(os.path.join(OUT_DIR, "h_graph_with_artifacts.yaml"))
    det = (vocab or {}).get("deterministic_aliases", {}) or {}

    # Build basename uniqueness across columns
    by_base = {}
    for k, v in (g or {}).items():
        if isinstance(v, dict) and v.get("entity_type") == "column":
            base = k.split(".", 1)[1] if "." in k else k
            by_base.setdefault(base, []).append(k)

    missing = []
    for base, cols in by_base.items():
        if len(cols) == 1:
            if base not in det or det.get(base) != cols[0]:
                missing.append((base, cols[0]))
    assert not missing, f"Unique basenames missing in vocabulary: {missing[:10]}"



def _require(path):
    if not os.path.exists(path):
        pytest.skip(f"Artifact not found: {path} (set VGB_OUT_DIR)")
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

@pytest.fixture(scope="module")
def graph_and_artifacts():
    gpath = os.path.join(OUT_DIR, "h_graph_with_artifacts.yaml")
    g = _require(gpath)
    arts = g.get("_artifacts") or {}
    if not arts:
        pytest.skip("Missing _artifacts in graph")
    binder = arts.get("binder")
    grammar = arts.get("grammar_text")
    if not isinstance(binder, dict) or not isinstance(grammar, str) or not grammar.strip():
        pytest.skip("Binder or grammar_text missing from artifacts")
    return g, binder, grammar

# -------------------------
# 1) Sanity: H connectors contract
# -------------------------

def test_h_binder_connectors_contract(graph_and_artifacts):
    g, binder, _ = graph_and_artifacts
    catalogs = binder.get("catalogs") or {}
    conns = catalogs.get("connectors")
    assert isinstance(conns, dict) and conns, "binder.catalogs.connectors must be a non-empty dict"
    # Expected names → surfaces (the usual contract)
    for required in ("AND", "COMMA", "OF", "FROM"):
        assert required in conns, f"connectors missing key '{required}'"
    # Optional: surfaces should look correct
    assert conns["AND"].lower() == "and"
    assert conns["OF"].lower() == "of"
    assert conns["FROM"].lower() == "from"
    assert conns["COMMA"] in {",", ", "}

# -------------------------
# 2) Grammar: list shapes parse (no binder yet)
# -------------------------

@pytest.fixture(scope="module")
def parser(graph_and_artifacts):
    _, _, grammar = graph_and_artifacts
    return i_build_parser(grammar)

def _pick_table_with_n_columns(graph, n_required=3):
    for tname, tnode in (graph or {}).items():
        if not isinstance(tnode, dict) or tnode.get("entity_type") != "table":
            continue
        cols_meta = ((tnode.get("metadata") or {}).get("columns") or {})
        cols = list(cols_meta.keys()) if isinstance(cols_meta, dict) else list(cols_meta or [])
        if len(cols) >= n_required:
            return tname, cols
    return None, []

def test_grammar_parses_two_and_three_item_lists(graph_and_artifacts, parser):
    g, _, _ = graph_and_artifacts
    t, cols = _pick_table_with_n_columns(g, n_required=3)
    if not t:
        pytest.skip("No table with >=3 columns")
    c1, c2, c3 = cols[:3]

    # 2-item list with 'and'
    q2 = f"select {c1} and {c2} from {t}"
    parser.parse(q2)  # should not raise

    # 3-item Oxford list
    q3 = f"select {c1}, {c2}, and {c3} from {t}"
    parser.parse(q3)

def _pick_function_with_args(binder):
    fmeta = (binder.get("catalogs") or {}).get("functions") or {}
    for fname, meta in fmeta.items():
        args = list(meta.get("args") or [])
        clause = str(meta.get("clause") or "").lower()
        klass = str(meta.get("class") or "").lower()
        # Select-friendly function with at least one arg
        if args and clause in {"select", "both", ""} and klass != "ordering":
            return fname, args
    return None, []

def test_grammar_parses_function_arg_list(graph_and_artifacts, parser):
    g, binder, _ = graph_and_artifacts
    t, cols = _pick_table_with_n_columns(g, n_required=3)
    if not t:
        pytest.skip("No table with >=3 columns")
    fn, args = _pick_function_with_args(binder)
    if not fn:
        pytest.skip("No select-friendly function with args found in binder")
    c1, c2, c3 = cols[:3]

    # Single arg
    q1 = f"select {fn} of {c1} from {t}"
    parser.parse(q1)

    # Try 2-arg list if grammar allows lists in column_args
    q2 = f"select {fn} of {c1} and {c2} from {t}"
    parser.parse(q2)

    # Try Oxford 3-arg list
    q3 = f"select {fn} of {c1}, {c2}, and {c3} from {t}"
    parser.parse(q3)

# -------------------------
# 3) Binder: lists should bind (this is where Phase-I is failing)
# -------------------------

@pytest.fixture(scope="module")
def relaxed_binder(graph_and_artifacts):
    g, binder, _ = graph_and_artifacts
    # Use the same relaxed path as Phase-I (merging connectors into a graph-shaped view)
    return i_make_relaxed_binder(g, binder_artifact=binder)

def test_binder_binds_three_item_oxford_list(graph_and_artifacts, parser, relaxed_binder):
    g, _, _ = graph_and_artifacts
    t, cols = _pick_table_with_n_columns(g, n_required=3)
    if not t:
        pytest.skip("No table with >=3 columns")
    c1, c2, c3 = cols[:3]
    q = f"select {c1}, {c2}, and {c3} from {t}"
    ok, info = i_roundtrip_one(relaxed_binder, parser, q)
    assert ok, f"Binder must handle Oxford list commas; error={info.get('error')}"

def test_binder_binds_function_with_arg_list(graph_and_artifacts, parser, relaxed_binder):
    g, binder, _ = graph_and_artifacts
    t, cols = _pick_table_with_n_columns(g, n_required=3)
    if not t:
        pytest.skip("No table with >=3 columns")
    fn, args = _pick_function_with_args(binder)
    if not fn:
        pytest.skip("No select-friendly function with args found in binder")
    c1, c2, c3 = cols[:3]

    q1 = f"select {fn} of {c1} from {t}"
    ok1, info1 = i_roundtrip_one(relaxed_binder, parser, q1)
    assert ok1, f"Single-arg '{fn}' should bind; error={info1.get('error')}"

    # If your binder only supports single-arg for this fn, the next two may fail legitimately.
    # We still run them to surface whether 'and' causes the error vs arg arity.
    q2 = f"select {fn} of {c1} and {c2} from {t}"
    ok2, info2 = i_roundtrip_one(relaxed_binder, parser, q2)
    if not ok2:
        # If it fails, it should NOT be due to 'and' being unknown; allow arity-related failures.
        assert "and" not in (info2.get("error") or "").lower(), f"'and' should be recognized; error={info2.get('error')}"

    q3 = f"select {fn} of {c1}, {c2}, and {c3} from {t}"
    ok3, info3 = i_roundtrip_one(relaxed_binder, parser, q3)
    if not ok3:
        assert "and" not in (info3.get("error") or "").lower(), f"'and' should be recognized; error={info3.get('error')}"

# -------------------------
# helpers
# -------------------------

def test_binder_roundtrip_three_item_oxford_list(graph_and_artifacts, parser, relaxed_binder):
    g, _, _ = graph_and_artifacts
    t, cols = _pick_table_with_n_columns(g, n_required=3)
    if not t:
        pytest.skip("No table with >=3 columns")
    c1, c2, c3 = cols[:3]
    q = f"select {c1}, {c2}, and {c3} from {t}"
    ok, info = i_roundtrip_one(relaxed_binder, parser, q)
    assert ok, f"Roundtrip should succeed after list sanitization; error={info.get('error')}"

def test_binder_roundtrip_function_arg_list(graph_and_artifacts, parser, relaxed_binder):
    g, binder, _ = graph_and_artifacts
    t, cols = _pick_table_with_n_columns(g, n_required=3)
    if not t:
        pytest.skip("No table with >=3 columns")
    fn, args = _pick_function_with_args(binder)
    if not fn:
        pytest.skip("No select-friendly function with args found in binder")
    c1, c2, c3 = cols[:3]

    q1 = f"select {fn} of {c1} from {t}"
    ok1, info1 = i_roundtrip_one(relaxed_binder, parser, q1)
    assert ok1, f"Single-arg '{fn}' should roundtrip; error={info1.get('error')}"

    q3 = f"select {fn} of {c1}, {c2}, and {c3} from {t}"
    ok3, info3 = i_roundtrip_one(relaxed_binder, parser, q3)
    # If your binder enforces arity=1 for this fn, it may still fail—but not due to 'and'.
    if not ok3:
        assert "and" not in (info3.get("error") or "").lower(), f"'and' should be neutralized; error={info3.get('error')}"
