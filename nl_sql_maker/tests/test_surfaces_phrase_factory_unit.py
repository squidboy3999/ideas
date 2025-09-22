import pytest
from vbg_tools.surfaces_spec_builder import SQLSpec
from vbg_tools.surfaces_phrase_factory import render_predicate_phrases, render_projection_phrases

def _vocab():
    return {
        "keywords": {
            "connectors": {"AND":"and","OR":"or","FROM":"from","OF":"of","COMMA":",","NOT":"not"},
            "select_verbs": {"select": {"aliases": ["show", "display"]}},
            "comparison_operators": {
                "between": {"aliases": ["between"]},
                "greater_than": {"aliases": [">", "greater than"]},
                "less_than": {"aliases": ["<", "less than"]},
            },
        },
        "sql_actions": {
            "count": {"placement":"projection","bind_style":"of","aliases":["count"],"template":"COUNT({column})","applicable_types":{"column":["any"]}},
            "sum":   {"placement":"projection","bind_style":"of","aliases":["sum"],  "template":"SUM({column})",  "applicable_types":{"column":["numeric"]}},
        }
    }

def _binder_numeric_date_text():
    return {
        "catalogs": {
            "tables": {"users": {}, "sales": {}},
            "columns": {
                "users.age":   {"table":"users","name":"age","type":"integer"},
                "sales.price": {"table":"sales","name":"price","type":"decimal"},
                "users.name":  {"table":"users","name":"name","type":"text"},
                "users.dob":   {"table":"users","name":"dob","type":"date"},
            },
            "connectors": {"AND":"and","OR":"or","FROM":"from","OF":"of","COMMA":",","NOT":"not"},
            "functions": {}
        }
    }

def _spec(func="sum", table="sales", column="sales.price"):
    return SQLSpec(func=func, arg_key="column", table=table, column=column,
                   expression_sql=f'SELECT {func.upper()}("sales"."price") FROM "sales"')

def test_predicate_phrases_for_numeric_include_between_gt_lt():
    vocab = _vocab()
    binder = _binder_numeric_date_text()
    spec = _spec(func="sum", table="sales", column="sales.price")
    preds = render_predicate_phrases(spec, vocab, binder)
    joined = " || ".join(preds).lower()
    assert " between " in joined
    assert " > " in joined or " greater than " in joined
    assert " < " in joined or " less than " in joined

def test_predicate_phrases_for_date_include_between():
    vocab = _vocab()
    binder = _binder_numeric_date_text()
    spec = SQLSpec(func="count", arg_key="column", table="users", column="users.dob",
                   expression_sql='SELECT COUNT("users"."dob") FROM "users"')
    preds = render_predicate_phrases(spec, vocab, binder)
    assert any("between" in p.lower() for p in preds)

def test_no_predicates_for_text_columns():
    vocab = _vocab()
    binder = _binder_numeric_date_text()
    spec = SQLSpec(func="count", arg_key="column", table="users", column="users.name",
                   expression_sql='SELECT COUNT("users"."name") FROM "users"')
    preds = render_predicate_phrases(spec, vocab, binder)
    assert preds == [] or len(preds) == 0
