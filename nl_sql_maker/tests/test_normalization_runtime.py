# tests/test_normalization_runtime.py
import os
import pytest

from vbg_runtime.artifacts_loader import load_artifacts
from vbg_runtime.normalize_runtime import normalize_nl, sanitize_list_connectors


OUT_DIR = os.environ.get("VGB_OUT_DIR", "out")


@pytest.mark.skipif(
    not (os.path.exists(os.path.join(OUT_DIR, "h_graph_with_artifacts.yaml"))
         and os.path.exists(os.path.join(OUT_DIR, "h_vocabulary.yaml"))
         and os.path.exists(os.path.join(OUT_DIR, "h_binder.yaml"))
         and os.path.exists(os.path.join(OUT_DIR, "h_grammar.lark"))),
    reason="Phase H artifacts not found in OUT_DIR (set VGB_OUT_DIR).",
)
def test_normalize_basename_to_dotted_and_sanitize_lists():
    graph, vocab, binder, grammar = load_artifacts(OUT_DIR)  # noqa: F401 (graph/binder/grammar unused here)

    # NL using basenames and 'and' list
    nl = "select boundaries and name from regions"
    cands, stats = normalize_nl(vocab, nl)

    assert stats["raw_candidates"] >= 1
    assert stats["sanitized_count"] >= 1

    # Expect at least one candidate with dotted forms and comma list
    expected = "select regions.boundaries, regions.name from regions"
    assert any(c == expected for c in cands), f"Expected candidate:\n{expected}\nGot:\n{cands}"


@pytest.mark.skipif(
    not (os.path.exists(os.path.join(OUT_DIR, "h_vocabulary.yaml"))),
    reason="Phase H vocabulary not found.",
)
def test_sanitizer_handles_oxford_and_bare_and():
    s1 = sanitize_list_connectors("select a and b from t")
    assert s1 == "select a, b from t"

    s2 = sanitize_list_connectors("select a, b, and c from t")
    assert s2 == "select a, b, c from t"

    s3 = sanitize_list_connectors("sum of a and b")
    assert s3 == "sum of a, b"

    s4 = sanitize_list_connectors("sum of a, b, and c")
    assert s4 == "sum of a, b, c"


@pytest.mark.skipif(
    not (os.path.exists(os.path.join(OUT_DIR, "h_graph_with_artifacts.yaml"))
         and os.path.exists(os.path.join(OUT_DIR, "h_vocabulary.yaml"))),
    reason="Phase H artifacts not found.",
)
def test_case_insensitive_normalization_matches():
    _, vocab, _, _ = load_artifacts(OUT_DIR)

    nl_upper = "SELECT BOUNDARIES AND NAME FROM REGIONS"
    cands_upper, _ = normalize_nl(vocab, nl_upper, case_insensitive=True)

    expected = "select regions.boundaries, regions.name from regions"
    assert any(c == expected for c in cands_upper)
