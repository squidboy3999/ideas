from __future__ import annotations

import os
import pytest

from vbg_runtime.config import RuntimeConfig, apply_config_defaults
from vbg_runtime.artifacts_loader import load_artifacts  # Step 1
from vbg_runtime.parser_runtime import make_parser
from vbg_runtime.nl2sql_engine import nl2sql_once

OUT_DIR = os.environ.get("VGB_OUT_DIR", "out")


def test_apply_config_defaults_infers_engine_and_clamps_topk(tmp_path):
    db_path = tmp_path / "test.db"
    db_path.write_bytes(b"")  # just to exist

    cfg = RuntimeConfig(engine="unknown", topk=0, execute_sql=True, db=str(db_path))
    out = apply_config_defaults(cfg, artifacts=None)

    # engine inferred to sqlite; topk clamped; default row limit added
    assert out.engine == "sqlite"
    assert 1 <= out.topk <= 50
    assert out.limit_rows == 1000


@pytest.mark.skipif(
    not (os.path.exists(os.path.join(OUT_DIR, "h_graph_with_artifacts.yaml"))
         and os.path.exists(os.path.join(OUT_DIR, "h_vocabulary.yaml"))
         and os.path.exists(os.path.join(OUT_DIR, "h_binder.yaml"))
         and os.path.exists(os.path.join(OUT_DIR, "h_grammar.lark"))),
    reason="Phase-H artifacts not found; set VGB_OUT_DIR.",
)
def test_config_strictness_toggle_does_not_break_success_path():
    # This test asserts that turning strict off is at least no worse on an easy query.
    graph, vocabulary, binder_artifact, grammar_text = load_artifacts(OUT_DIR)
    parser = make_parser(grammar_text)

    q = "select users.user_id and users.username from users"

    res_strict = nl2sql_once(
        q,
        graph=graph,
        vocabulary=vocabulary,
        binder_artifact=binder_artifact,
        parser=parser,
        engine="sqlite",
        topk=3,
        strict_binder=True,  # strict on
    )
    res_lenient = nl2sql_once(
        q,
        graph=graph,
        vocabulary=vocabulary,
        binder_artifact=binder_artifact,
        parser=parser,
        engine="sqlite",
        topk=3,
        strict_binder=False,  # strict off (coercion enabled downstream)
    )

    # On a straightforward query, both should succeed; at minimum, strict-off should not regress.
    assert res_strict["ok"] is True
    assert res_lenient["ok"] is True


@pytest.mark.skipif(
    not (os.path.exists(os.path.join(OUT_DIR, "h_graph_with_artifacts.yaml"))
         and os.path.exists(os.path.join(OUT_DIR, "h_vocabulary.yaml"))
         and os.path.exists(os.path.join(OUT_DIR, "h_binder.yaml"))
         and os.path.exists(os.path.join(OUT_DIR, "h_grammar.lark"))),
    reason="Phase-H artifacts not found; set VGB_OUT_DIR.",
)
def test_topk_changes_candidate_exploration_count():
    graph, vocabulary, binder_artifact, grammar_text = load_artifacts(OUT_DIR)
    parser = make_parser(grammar_text)

    # Use a query that typically yields at least one candidate; the assertion
    # allows equal considered counts in case the first candidate already succeeds.
    q = "select boundaries and name from regions"

    r1 = nl2sql_once(
        q, graph=graph, vocabulary=vocabulary, binder_artifact=binder_artifact, parser=parser,
        engine="sqlite", topk=1
    )
    r5 = nl2sql_once(
        q, graph=graph, vocabulary=vocabulary, binder_artifact=binder_artifact, parser=parser,
        engine="sqlite", topk=5
    )

    # Both runs should be valid; 'considered' must never exceed topk.
    assert r1["stats"]["considered"] <= 1
    assert r5["stats"]["considered"] <= 5

    # And with a higher topk, we should consider at least as many candidates.
    assert r5["stats"]["considered"] >= r1["stats"]["considered"]
