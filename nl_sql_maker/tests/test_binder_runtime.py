# tests/test_binder_runtime.py
import os
import pytest

from vbg_runtime.artifacts_loader import load_artifacts  # re-use from step 1
from vbg_runtime.parser_runtime import make_parser
from vbg_runtime.binder_runtime import make_runtime_binder, bind_and_parse_once


OUT_DIR = os.environ.get("VGB_OUT_DIR", "out")


def _pick_table_with_n_columns(graph, n_required=3):
    for tname, tnode in (graph or {}).items():
        if not isinstance(tnode, dict) or tnode.get("entity_type") != "table":
            continue
        cols_meta = ((tnode.get("metadata") or {}).get("columns") or {})
        keys = list(cols_meta.keys()) if isinstance(cols_meta, dict) else list(cols_meta or [])
        if len(keys) >= n_required:
            return tname, keys
    return None, []


def _pick_select_friendly_fn_with_args(binder_artifact):
    catalogs = binder_artifact.get("catalogs") or {}
    fns = (catalogs.get("functions") or {}).items()
    for name, meta in fns:
        klass = str((meta or {}).get("class") or "").lower()
        clause = str((meta or {}).get("clause") or "").lower()
        args = (meta or {}).get("args") or []
        if klass == "ordering":
            continue
        if clause in {"order_by", "group_by", "having", "where", "limit"}:
            continue
        if args:
            return name, args
    return None, []


def _pick_ordering_like_function(binder_artifact):
    catalogs = binder_artifact.get("catalogs") or {}
    fns = (catalogs.get("functions") or {}).items()
    for name, meta in fns:
        klass = str((meta or {}).get("class") or "").lower()
        clause = str((meta or {}).get("clause") or "").lower()
        if klass == "ordering" or clause == "order_by":
            return name
    return None


@pytest.mark.skipif(
    not (os.path.exists(os.path.join(OUT_DIR, "h_graph_with_artifacts.yaml"))
         and os.path.exists(os.path.join(OUT_DIR, "h_binder.yaml"))
         and os.path.exists(os.path.join(OUT_DIR, "h_grammar.lark"))),
    reason="Phase H artifacts not found in OUT_DIR.",
)
def test_binder_binds_simple_column_list_with_commas():
    graph, _, binder_artifact, grammar = load_artifacts(OUT_DIR)
    parser = make_parser(grammar)
    binder = make_runtime_binder(graph, binder_artifact, strict=False)

    t, cols = _pick_table_with_n_columns(graph, n_required=2)
    if not t:
        pytest.skip("No table with ≥2 columns found.")
    c1, c2 = cols[:2]

    q = f"select {c1}, {c2} from {t}"
    ok, err = bind_and_parse_once(binder, parser, q)
    assert ok, f"Expected binder to bind comma list; error={err}"


@pytest.mark.skipif(
    not (os.path.exists(os.path.join(OUT_DIR, "h_graph_with_artifacts.yaml"))
         and os.path.exists(os.path.join(OUT_DIR, "h_binder.yaml"))
         and os.path.exists(os.path.join(OUT_DIR, "h_grammar.lark"))),
    reason="Phase H artifacts not found in OUT_DIR.",
)
def test_binder_oxford_list_requires_sanitizer():
    graph, _, binder_artifact, grammar = load_artifacts(OUT_DIR)
    parser = make_parser(grammar)
    binder = make_runtime_binder(graph, binder_artifact, strict=False)

    t, cols = _pick_table_with_n_columns(graph, n_required=3)
    if not t:
        pytest.skip("No table with ≥3 columns found.")
    c1, c2, c3 = cols[:3]

    q = f"select {c1}, {c2}, and {c3} from {t}"
    ok, err = bind_and_parse_once(binder, parser, q)

    # At runtime we sanitize Oxford lists before binding; raw 'and' should not bind here.
    assert not ok, "Unsanitized Oxford list should not bind; sanitize before binding."


@pytest.mark.skipif(
    not (os.path.exists(os.path.join(OUT_DIR, "h_graph_with_artifacts.yaml"))
         and os.path.exists(os.path.join(OUT_DIR, "h_binder.yaml"))
         and os.path.exists(os.path.join(OUT_DIR, "h_grammar.lark"))),
    reason="Phase H artifacts not found in OUT_DIR.",
)
def test_binder_binds_function_with_args_and_blocks_ordering_functions():
    graph, _, binder_artifact, grammar = load_artifacts(OUT_DIR)
    parser = make_parser(grammar)
    binder = make_runtime_binder(graph, binder_artifact, strict=False)

    t, cols = _pick_table_with_n_columns(graph, n_required=1)
    if not t:
        pytest.skip("No table with ≥1 column found.")
    c1 = cols[0]

    # Select-friendly function with declared args
    fn_tuple = _pick_select_friendly_fn_with_args(binder_artifact)
    if not fn_tuple[0]:
        pytest.skip("No select-friendly function with args found in binder.")
    fn_name, _ = fn_tuple

    q1 = f"select {fn_name} of {c1} from {t}"
    ok1, err1 = bind_and_parse_once(binder, parser, q1)
    assert ok1, f"Expected '{fn_name} of {c1}' to bind; error={err1}"

    # Ordering-like function should NOT be allowed in the select list
    ord_fn = _pick_ordering_like_function(binder_artifact)
    if ord_fn:
        q2 = f"select {ord_fn} of {c1} from {t}"
        ok2, _ = bind_and_parse_once(binder, parser, q2)
        assert not ok2, f"Ordering function '{ord_fn}' should not bind in SELECT."
