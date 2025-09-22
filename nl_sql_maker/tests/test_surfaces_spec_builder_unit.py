import pytest
from vbg_tools.surfaces_spec_builder import enumerate_specs, SQLSpec, column_slot_types

def _minimal_vocab_no_applicable():
    return {
        "keywords": {
            "connectors": {"AND":"and","OR":"or","FROM":"from","OF":"of","COMMA":",","NOT":"not"},
            "select_verbs": {"select": {"aliases": ["show", "display", "fetch"]}}
        },
        "sql_actions": {
            "count": {"placement": "projection", "bind_style": "of", "aliases": ["count"], "template": "COUNT({column})"},
            "sum":   {"placement": "projection", "bind_style": "of", "aliases": ["sum"],   "template": "SUM({column})"},
        }
    }

def _minimal_vocab_with_applicable():
    v = _minimal_vocab_no_applicable()
    v["sql_actions"]["count"]["applicable_types"] = {"column": ["any"]}
    v["sql_actions"]["sum"]["applicable_types"]   = {"column": ["numeric"]}
    return v

def _binder_numeric_and_text():
    return {
        "catalogs": {
            "tables": {"users": {}, "sales": {}},
            "columns": {
                "users.user_id": {"table": "users", "name": "user_id", "type": "integer"},
                "users.age":     {"table": "users", "name": "age",     "type": "integer"},
                "users.name":    {"table": "users", "name": "name",    "type": "text"},
                "sales.price":   {"table": "sales", "name": "price",   "type": "decimal"},
            },
            "connectors": {"AND":"and","OR":"or","FROM":"from","OF":"of","COMMA":",","NOT":"not"},
            "functions": {}
        }
    }

def test_applicability_gate_requires_types():
    vocab = _minimal_vocab_no_applicable()
    binder = _binder_numeric_and_text()
    specs = enumerate_specs(binder, vocab, max_specs=100)
    assert specs == [] or len(specs) == 0

def test_specs_appear_with_applicable_types_any_and_numeric():
    vocab = _minimal_vocab_with_applicable()
    binder = _binder_numeric_and_text()
    specs = enumerate_specs(binder, vocab, max_specs=100)
    assert specs
    funcs = {s.func for s in specs}
    assert "count" in funcs and "sum" in funcs
    cols = {s.column for s in specs}
    assert "users.age" in cols or "sales.price" in cols

def test_column_slot_types_from_db_type_and_explicit():
    binder = _binder_numeric_and_text()
    assert "numeric" in column_slot_types(binder, "users.age")
    assert "text" in column_slot_types(binder, "users.name")
    assert column_slot_types(binder, "users.missing") == set()
