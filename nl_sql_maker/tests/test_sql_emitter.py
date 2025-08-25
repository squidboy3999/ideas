# tests/test_sql_emitter.py
import os
import pytest

from vbg_runtime.artifacts_loader import load_artifacts  # from your step 1
from vbg_runtime.parser_runtime import make_parser
from vbg_runtime.binder_runtime import make_runtime_binder
from vbg_runtime.sql_emitter import emit_select_with_warnings

# Also need canonical tokenizer to build bound objects
try:
    from vbg_generate.canonical_core import canon_tokenize  # type: ignore
except Exception:  # pragma: no cover
    from src.vbg_generate.canonical_core import canon_tokenize  # type: ignore

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


def _pick_geom_column(graph):
    for cname, cnode in (graph or {}).items():
        if not isinstance(cnode, dict) or cnode.get("entity_type") != "column":
            continue
        md = (cnode.get("metadata") or {})
        tc = str(md.get("type_category") or "").lower()
        labs = {str(x).lower() for x in (md.get("labels") or []) if isinstance(x, str)}
        if tc in {"geometry", "geography"} or "postgis" in labs:
            return cname, md.get("table")
    return None, None


def _pick_select_fn_with_args(binder_artifact):
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
            return name
    return None


@pytest.mark.skipif(
    not (os.path.exists(os.path.join(OUT_DIR, "h_graph_with_artifacts.yaml"))
         and os.path.exists(os.path.join(OUT_DIR, "h_binder.yaml"))
         and os.path.exists(os.path.join(OUT_DIR, "h_grammar.lark"))),
    reason="Phase H artifacts not found in OUT_DIR.",
)
def test_emit_single_column_and_list_variants():
    graph, _, binder_artifact, grammar = load_artifacts(OUT_DIR)
    parser = make_parser(grammar)
    binder = make_runtime_binder(graph, binder_artifact, strict=False)

    t, cols = _pick_table_with_n_columns(graph, n_required=3)
    if not t:
        pytest.skip("No table with ≥3 columns found.")
    c1, c2, c3 = cols[:3]

    # Single column
    can1 = f"select {c1} from {t}"
    bound1 = binder.bind(canon_tokenize(can1))
    sql1, warns1 = emit_select_with_warnings(bound1, binder_artifact=binder_artifact, engine="sqlite")
    assert sql1.strip().upper() == f'SELECT "{t.upper()}"."{c1.split(".",1)[1].upper()}" FROM "{t.upper()}";'
    assert not warns1

    # Two columns (comma)
    can2 = f"select {c1}, {c2} from {t}"
    bound2 = binder.bind(canon_tokenize(can2))
    sql2, _ = emit_select_with_warnings(bound2, binder_artifact=binder_artifact, engine="sqlite")
    assert ','.join(sql2.split("SELECT ")[1].split(" FROM ")[0].split(",")).count(",") == 1  # has one comma

    # Three columns (already comma-sanitized)
    can3 = f"select {c1}, {c2}, {c3} from {t}"
    bound3 = binder.bind(canon_tokenize(can3))
    sql3, _ = emit_select_with_warnings(bound3, binder_artifact=binder_artifact, engine="sqlite")
    assert sql3.count(",") >= 2


@pytest.mark.skipif(
    not (os.path.exists(os.path.join(OUT_DIR, "h_graph_with_artifacts.yaml"))
         and os.path.exists(os.path.join(OUT_DIR, "h_binder.yaml"))
         and os.path.exists(os.path.join(OUT_DIR, "h_grammar.lark"))),
    reason="Phase H artifacts not found in OUT_DIR.",
)
def test_emit_function_calls_and_postgis_warning_if_applicable():
    graph, _, binder_artifact, grammar = load_artifacts(OUT_DIR)
    parser = make_parser(grammar)
    binder = make_runtime_binder(graph, binder_artifact, strict=False)

    # Pick a select-friendly function with args
    fn = _pick_select_fn_with_args(binder_artifact)
    if not fn:
        pytest.skip("No select-friendly function-with-args in binder.")
    t, cols = _pick_table_with_n_columns(graph, n_required=1)
    if not t:
        pytest.skip("No table with at least 1 column.")
    c1 = cols[0]

    # Emit SQL for function-of-one-column
    can = f"select {fn} of {c1} from {t}"
    bound = binder.bind(canon_tokenize(can))
    sql, warns = emit_select_with_warnings(bound, binder_artifact=binder_artifact, engine="sqlite")
    assert fn in sql, "Function name should appear in SQL"

    # If we can find a geometry column and a PostGIS-y function, ensure warning is present
    geom_col, geom_tbl = _pick_geom_column(graph)
    if geom_col:
        st_fn = None
        for name, meta in (binder_artifact.get("catalogs", {}).get("functions", {}) or {}).items():
            if str(name).lower().startswith("st_"):
                st_fn = name
                break
        if st_fn:
            can2 = f"select {st_fn} of {geom_col} from {geom_tbl}"
            bound2 = binder.bind(canon_tokenize(can2))
            sql2, warns2 = emit_select_with_warnings(bound2, binder_artifact=binder_artifact, engine="sqlite")
            assert any(st_fn in w for w in warns2), "Expected a PostGIS warning on SQLite"
