# tests/test_parser_runtime.py
import os
import pytest

from vbg_runtime.artifacts_loader import load_artifacts  # re-use from step 1
from vbg_runtime.parser_runtime import make_parser, parse_canonical

OUT_DIR = os.environ.get("VGB_OUT_DIR", "out")


def _pick_table_with_n_columns(graph, n_required=2):
    for tname, tnode in (graph or {}).items():
        if not isinstance(tnode, dict) or tnode.get("entity_type") != "table":
            continue
        cols_meta = ((tnode.get("metadata") or {}).get("columns") or {})
        keys = list(cols_meta.keys()) if isinstance(cols_meta, dict) else list(cols_meta or [])
        if len(keys) >= n_required:
            return tname, keys
    return None, []


@pytest.mark.skipif(
    not (os.path.exists(os.path.join(OUT_DIR, "h_graph_with_artifacts.yaml"))
         and os.path.exists(os.path.join(OUT_DIR, "h_grammar.lark"))),
    reason="Phase H artifacts not found in OUT_DIR.",
)
def test_make_parser_compiles_and_parses_valid_and_invalid():
    graph, _, _, grammar = load_artifacts(OUT_DIR)
    parser = make_parser(grammar)

    # Pick any table with ≥2 columns to form a valid canonical
    t, cols = _pick_table_with_n_columns(graph, n_required=2)
    if not t:
        pytest.skip("No table with ≥2 columns found in graph.")
    c1, c2 = cols[:2]

    valid = f"select {c1}, {c2} from {t}"
    assert parse_canonical(parser, valid) is True

    invalid = f"select and {c1} from {t}"
    assert parse_canonical(parser, invalid) is False
