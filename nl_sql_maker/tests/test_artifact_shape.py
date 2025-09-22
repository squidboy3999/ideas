# tests/test_artifact_shape.py
from __future__ import annotations
import re
from pathlib import Path

def test_vocabulary_minimal_contract(vocab):
    # Required sections present
    assert vocab, "Vocabulary YAML is empty."
    kw = vocab.get("keywords") or {}
    assert kw, "Missing 'keywords' block in vocabulary."
    # Connectors present (at least these five)
    connectors = (kw.get("connectors") or kw.get("CONNECTORS") or {})
    needed = {"AND", "OR", "FROM", "OF", "COMMA"}
    missing = [k for k in needed if k not in connectors]
    assert not missing, f"Missing connectors in vocabulary: {missing}"
    # There should be some sql_actions with aliases
    sql_actions = kw.get("sql_actions") or kw.get("SQL_ACTIONS") or {}
    assert sql_actions, "Missing 'sql_actions' in vocabulary."
    some = next(iter(sql_actions.values()))
    assert "aliases" in some, "sql_actions entries must include 'aliases'."

def test_binder_minimal_contract(binder):
    assert binder, "Binder YAML is empty."
    cats = binder.get("catalogs") or {}
    for key in ("tables", "columns", "functions", "connectors"):
        assert key in cats, f"Binder missing catalogs.{key}"
    # Ascending & descending order_by support (either function names or aliases)
    funcs = cats["functions"]
    fn_names = set(funcs.keys())
    has_desc = any(n in fn_names for n in ("order_by_desc", "orderby_desc"))
    has_asc  = any(n in fn_names for n in ("order_by", "orderby"))
    assert has_desc, "Binder missing a descending order_by function (e.g., 'order_by_desc')."
    assert has_asc,  "Binder missing an ascending order_by function (e.g., 'order_by')."

def test_grammar_text_contract(grammar_text: str):
    assert grammar_text.strip(), "Grammar file is empty."
    # No quoted placeholders like "table"/"columns"/"value"
    banned = [r'"table"', r'"columns"', r'"value"']
    for b in banned:
        assert re.search(b, grammar_text) is None, f'Grammar contains quoted placeholder {b}'
    # No ellipses (either unicode … or triple dots ...)
    assert "…" not in grammar_text, "Grammar contains unicode ellipsis (…)."
    assert "..." not in grammar_text, "Grammar contains '...' ellipsis."
