# tests/test_nl2sql_engine.py
import os
import pytest

from vbg_runtime.artifacts_loader import load_artifacts  # step 1
from vbg_runtime.parser_runtime import make_parser
from vbg_runtime.nl2sql_engine import nl2sql_once

OUT_DIR = os.environ.get("VGB_OUT_DIR", "out")


@pytest.mark.skipif(
    not (os.path.exists(os.path.join(OUT_DIR, "h_graph_with_artifacts.yaml"))
         and os.path.exists(os.path.join(OUT_DIR, "h_vocabulary.yaml"))
         and os.path.exists(os.path.join(OUT_DIR, "h_binder.yaml"))
         and os.path.exists(os.path.join(OUT_DIR, "h_grammar.lark"))),
    reason="Phase-H artifacts not found; set VGB_OUT_DIR.",
)
def test_nl2sql_end_to_end_simple_and_list():
    graph, vocabulary, binder_artifact, grammar_text = load_artifacts(OUT_DIR)
    parser = make_parser(grammar_text)

    # This relies on Phase-H unique-basename injection to map 'boundaries'/'name'
    res = nl2sql_once(
        "select boundaries and name from regions",
        graph=graph,
        vocabulary=vocabulary,
        binder_artifact=binder_artifact,
        parser=parser,
        engine="sqlite",
        topk=5,
    )
    assert res["ok"], f"should succeed: {res}"
    assert "SELECT" in res["sql"] and "FROM" in res["sql"]
    assert '"regions"' in res["sql"]  # table quoted


@pytest.mark.skipif(
    not (os.path.exists(os.path.join(OUT_DIR, "h_graph_with_artifacts.yaml"))
         and os.path.exists(os.path.join(OUT_DIR, "h_vocabulary.yaml"))
         and os.path.exists(os.path.join(OUT_DIR, "h_binder.yaml"))
         and os.path.exists(os.path.join(OUT_DIR, "h_grammar.lark"))),
    reason="Phase-H artifacts not found; set VGB_OUT_DIR.",
)
def test_nl2sql_function_call_or_template():
    graph, vocabulary, binder_artifact, grammar_text = load_artifacts(OUT_DIR)
    parser = make_parser(grammar_text)

    # Try a common aggregate that should exist
    # The vocabulary typically maps 'sum' or 'count' to canonical functions.
    res = nl2sql_once(
        "select sum of quantity from sales",
        graph=graph,
        vocabulary=vocabulary,
        binder_artifact=binder_artifact,
        parser=parser,
        engine="sqlite",
        topk=5,
    )
    assert res["ok"], f"aggregate should bind: {res}"
    assert "SUM" in res["sql"].upper() or "sum(" in res["sql"]


@pytest.mark.skipif(
    not (os.path.exists(os.path.join(OUT_DIR, "h_graph_with_artifacts.yaml"))
         and os.path.exists(os.path.join(OUT_DIR, "h_vocabulary.yaml"))
         and os.path.exists(os.path.join(OUT_DIR, "h_binder.yaml"))
         and os.path.exists(os.path.join(OUT_DIR, "h_grammar.lark"))),
    reason="Phase-H artifacts not found; set VGB_OUT_DIR.",
)
def test_nl2sql_failure_surface_for_bad_column():
    graph, vocabulary, binder_artifact, grammar_text = load_artifacts(OUT_DIR)
    parser = make_parser(grammar_text)

    res = nl2sql_once(
        "select not_a_real_column from users",
        graph=graph,
        vocabulary=vocabulary,
        binder_artifact=binder_artifact,
        parser=parser,
        engine="sqlite",
        topk=3,
    )
    assert not res["ok"]
    assert res["fail_category"] in {"binder_fail", "normalizer_zero", "parser_fail"}
    # If it reached binder, ensure a helpful error is captured
    if res["fail_category"] != "normalizer_zero":
        assert any("not_a_real_column" in e or "token" in e.lower() for e in res["stats"].get("binder_errors", []))
