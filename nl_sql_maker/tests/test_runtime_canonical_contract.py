# tests/test_runtime_canonical_contract.py
from __future__ import annotations
import importlib
import pytest

def _import_runtime():
    try:
        return importlib.import_module("vbg_tools.graph_runtime")
    except Exception as e:
        pytest.skip(f"Cannot import runtime module: {e!r}")

def test_canonical_excludes_constraints_tokens():
    gr = _import_runtime()
    # If the module exposes a helper, use it; otherwise skip this unit
    harvest = getattr(gr, "harvest_and_canonicalize", None)
    tokenize = getattr(gr, "tokenize", None)
    build_lexicon = getattr(gr, "build_lexicon", None)
    if not all([harvest, tokenize, build_lexicon]):
        pytest.skip("harvest_and_canonicalize/tokenize/build_lexicon not available; skipping.")
    # Minimal faux inputs (adjust to your schema terms if needed)
    raw = "display sum of price from sales price > 10"
    tokens = tokenize(raw)
    vocab, connectors = build_lexicon({"keywords": {}}, {})  # tolerate minimal inputs if function allows
    # Shim maps (empty; harvest should still avoid injecting comparator/logical into canonical)
    tables_by_lc = {"sales": "sales"}
    columns_by_lc = {"price": "sales.price"}
    spans = []  # if your span builder exists, you could generate spans; otherwise we just exercise the contract
    res = harvest(raw, tokens, spans, tables_by_lc, columns_by_lc, connectors)
    canon = " ".join(res.canonical_tokens or [])
    forbidden = ("greater_than", "less_than", "between", "and", "or", "not")
    assert not any(tok in canon.lower() for tok in forbidden), f"Constraint/logical token leaked into canonical: {canon}"
