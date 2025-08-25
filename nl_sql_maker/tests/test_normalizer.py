import pytest
from vbg_generate.phase_h_artifact_compilation import compile_vocabulary
from vbg_generate.normalizer import normalize_text

def _graph_regions():
    return {
        "regions": {"entity_type": "table", "metadata": {"columns": {"regions.boundaries": {}, "regions.name": {}}}},
        "regions.boundaries": {"entity_type": "column", "metadata": {"table": "regions", "type": "geometry"}},
        "regions.name": {"entity_type": "column", "metadata": {"table": "regions", "type": "text"}},
        # minimal connector meta so the normalizer knows OF/FROM/AND
        "_binder_meta": {"connectors": [{"name":"OF","surface":"of"},{"name":"FROM","surface":"from"},{"name":"AND","surface":"and"},{"name":"COMMA","surface":","}]},
    }

def test_unique_basename_aliases_enable_normalization():
    g = _graph_regions()
    vocab = compile_vocabulary(g, vocabulary_from_d_or_none=None)  # injects unique basenames
    # The NL uses basenames; we expect candidates that canonicalize to dotted ids
    cands = normalize_text(vocab, "select boundaries and name from regions")
    assert cands, "normalizer should produce candidates"
    joined = " || ".join(cands).lower()
    assert "regions.boundaries" in joined and "regions.name" in joined
