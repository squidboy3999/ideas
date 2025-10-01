# tests/test_artifact_helpers_unit.py
from __future__ import annotations

import pytest

from vbg_tools.artifact_helpers import (
    normalize_aliases,
    ensure_core_connectors,
    coerce_column_row,
    collect_table_rows,
    collect_column_rows,
    collect_functions_from_schema,
)


def test_normalize_aliases_dedup_and_strip():
    ali = normalize_aliases(["  foo ", "Foo", "bar", "", "bar "])
    # keep original case, dedup + strip, sorted for determinism
    assert ali == ["Foo", "bar", "foo"]


def test_ensure_core_connectors_adds_defaults_and_upcases_keys():
    inp = {"and": "AND-word", "from": "from"}
    out = ensure_core_connectors(inp)
    assert set(out.keys()) >= {"AND", "OR", "NOT", "FROM", "OF", "COMMA"}
    assert out["AND"] == "AND-word"
    assert out["COMMA"] == ","


def test_coerce_column_row_accepts_string_column():
    r = coerce_column_row("users", "age")
    assert r == {"fqn": "users.age", "table": "users", "name": "age", "types": []}


def test_coerce_column_row_accepts_explicit_dict_with_types_list():
    r = coerce_column_row("users", {"name": "age", "types": ["INTEGER", "text"]})
    assert r["fqn"] == "users.age"
    assert set(r["types"]) == {"INTEGER", "text"}


def test_coerce_column_row_single_key_style_dict_with_meta():
    r = coerce_column_row("sales", {"price": {"type": "DECIMAL(10,2)"}})
    assert r == {"fqn": "sales.price", "table": "sales", "name": "price", "types": ["DECIMAL(10,2)"]}


def test_collect_table_rows_from_list_and_dict():
    schema_list = {"tables": ["users", "sales"]}
    schema_dict = {"tables": {"users": {"columns": []}, "sales": {}}}
    rows_list = collect_table_rows(schema_list)
    rows_dict = collect_table_rows(schema_dict)
    assert {"n": "users"} in rows_list and {"n": "sales"} in rows_list
    assert {"n": "users"} in rows_dict and {"n": "sales"} in rows_dict


def test_collect_column_rows_handles_top_level_broken_keys_and_dictlike_strings():
    # Reproduces the “regions.{...}” broken top-level key case
    schema = {
        "tables": {"regions": {"columns": []}},
        "columns": {
            "regions.{'aliases': ['identifier','label','name','names','title'], 'type': 'VARCHAR(50)', 'labels': []}":
                "{'aliases': ['identifier','label','name','names','title'], 'type': 'VARCHAR(50)', 'labels': []}",
            "users.age": "{'aliases': ['age'], 'type': 'INT'}",
        },
    }
    rows = collect_column_rows(schema)
    fqns = {r["fqn"] for r in rows}

    # No curly braces in any FQN
    assert not any("{" in f for f in fqns), f"Found brace-polluted FQNs: {sorted(fqns)}"

    # Expect a sensible regions.<name> entry
    assert any(f.startswith("regions.") for f in fqns)
    assert "users.age" in fqns


def test_collect_column_rows_handles_nested_dictlike_column_keys_regression():
    """
    Regression for nested columns mapping with dict-like key, e.g.:
      tables.regions.columns: {"{...dict-like...}": "{...dict-like...}"}
    This used to leak 'regions.{...}' into binder.
    """
    schema = {
        "tables": {
            "regions": {
                "columns": {
                    "{'aliases': ['identifier','label','name','names','title'], 'type': 'VARCHAR(50)', 'labels': []}":
                        "{'aliases': ['identifier','label','name','names','title'], 'type': 'VARCHAR(50)', 'labels': []}"
                }
            },
            "users": {"columns": [{"name": "age", "type": "INT"}]},
        }
    }
    rows = collect_column_rows(schema)
    fqns = {r["fqn"] for r in rows}

    assert "users.age" in fqns
    # Must not produce any brace-polluted key
    assert not any("{" in f or "}" in f for f in fqns), f"Found brace-polluted FQNs: {sorted(fqns)}"
    # Should normalize to some regions.<identifier>, prefer 'name' if present
    expect_any = {"regions.name", "regions.identifier", "regions.label", "regions.title", "regions.names"}
    assert fqns & expect_any, f"Expected a normalized regions column; got: {sorted(fqns)}"


def test_collect_functions_from_schema_normalizes_requirements():
    schema = {
        "functions": {
            "order_by": {
                "template": "{column}",
                "aliases": ["order by"],
                "requirements": [{"arg": "column", "st": "any"}],
                "placement": "clause",
                "bind_style": "of",
            },
            "limit": {
                "template": "LIMIT {value}",
                "requirements": [{"arg": "value", "st": "numeric"}],
                "placement": "clause",
            },
        }
    }
    rows = collect_functions_from_schema(schema)
    by_name = {r["name"]: r for r in rows}
    assert set(by_name.keys()) == {"order_by", "limit"}
    assert by_name["order_by"]["reqs"] == [{"arg": "column", "st": "any"}]
    assert by_name["limit"]["reqs"] == [{"arg": "value", "st": "numeric"}]
