# tests/test_synth_artifacts_unit.py
from __future__ import annotations

import pytest

from vbg_tools.synth_artifacts import (
    build_vocabulary,
    build_binder,
    build_grammar,
)


def _keywords_top_level_and_legacy():
    return {
        "keywords": {
            "select_verbs": {"select": {"aliases": ["show", "list"]}},
            "comparison_operators": {
                "greater_than": {"aliases": [">", "greater than"]},
                "between": {"aliases": ["between"]},
            },
            "filler_words": {"_skip": {"aliases": ["the", "a"]}},
            "connectors": {"AND": "and", "OR": "or", "FROM": "from", "OF": "of", "COMMA": ","},
            "global_templates": {"select_template": "SELECT {columns} FROM {table} {constraints}"},
            "sql_actions": {
                "sum": {"aliases": ["sum"], "template": "SUM({column})", "applicable_types": {"column": ["numeric"]}},
            },
        },
        "sql_actions": {
            "count": {"aliases": ["count"], "template": "COUNT({column})", "applicable_types": {"column": ["any"]}},
        },
    }


def _schema_with_top_and_nested_broken():
    # Both top-level and nested dict-like column keys
    return {
        "tables": {
            "users": {"columns": [{"name": "age", "type": "INT"}, {"name": "username", "type": "VARCHAR(50)"}]},
            "regions": {
                "columns": {
                    "{'aliases': ['identifier','label','name','names','title'], 'type': 'VARCHAR(50)', 'labels': []}":
                        "{'aliases': ['identifier','label','name','names','title'], 'type': 'VARCHAR(50)', 'labels': []}"
                }
            },
        },
        "columns": {
            "regions.{'aliases': ['identifier','label','name','names','title'], 'type': 'VARCHAR(50)', 'labels': []}":
                "{'aliases': ['identifier','label','name','names','title'], 'type': 'VARCHAR(50)', 'labels': []}"
        },
    }


def test_build_vocabulary_merges_actions_and_connectors_present():
    vocab = build_vocabulary(_keywords_top_level_and_legacy())
    kw = vocab["keywords"]
    assert "select" in kw["select_verbs"]
    assert "show" in kw["select_verbs"]["select"]["aliases"]
    for k in ("AND", "OR", "FROM", "OF", "COMMA"):
        assert k in kw["connectors"]
    actions = kw["sql_actions"]
    assert {"count", "sum"}.issubset(actions.keys())


def test_build_binder_normalizes_all_brace_polluted_keys():
    vocab = build_vocabulary(_keywords_top_level_and_legacy())
    binder = build_binder(_schema_with_top_and_nested_broken(), vocab)
    cols = binder["catalogs"]["columns"]

    # No FQN may contain curly braces at all
    offenders = [k for k in cols if "{" in k or "}" in k]
    assert not offenders, f"Curly-brace FQNs must not appear. Offenders: {offenders}"

    # Must contain normalized regions column and users columns
    assert any(k.startswith("regions.") for k in cols)
    assert "users.age" in cols and "users.username" in cols

    # Slot-types derived from DB type
    assert set(binder["catalogs"]["columns"]["users.age"]["slot_types"]) == {"numeric"}
    assert set(binder["catalogs"]["columns"]["users.username"]["slot_types"]) == {"text"}


def test_build_grammar_compiles_if_lark_installed_and_contract_holds():
    vocab = build_vocabulary(_keywords_top_level_and_legacy())
    binder = build_binder(_schema_with_top_and_nested_broken(), vocab)
    grammar = build_grammar(vocab, binder)

    # Contract: one 'query' rule; no quoted placeholders
    assert grammar.count("query:") == 1
    assert '"table"' not in grammar and '"value"' not in grammar and '"columns"' not in grammar

    try:
        from lark import Lark
    except Exception:
        pytest.skip("Lark not available")
    Lark(grammar, start="query", parser="lalr")


def test_build_binder_regression_guard_nested_dictlike_keys():
    """
    Focused regression guard: nested dict-like keys under tables.<table>.columns
    must not leak to binder FQNs.
    """
    vocab = build_vocabulary(_keywords_top_level_and_legacy())
    binder = build_binder(_schema_with_top_and_nested_broken(), vocab)
    cols = binder["catalogs"]["columns"]
    assert not any("{" in k or "}" in k for k in cols)
