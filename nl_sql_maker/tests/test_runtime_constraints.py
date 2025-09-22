from __future__ import annotations
from typing import Dict, Any
from vbg_tools.graph_runtime import map_text
from vbg_tools.sql_helpers import build_select_sql_from_slots

TEST_VOCAB: Dict[str, Any] = {
    "keywords": {
        "connectors": {"AND": "and", "OR": "or", "NOT": "not", "FROM": "from", "OF": "of", "COMMA": ","},
        "select_verbs": {"select": {"aliases": ["show"]}},
        "logical_operators": {
            "and": {"aliases": ["and"]},
            "or": {"aliases": ["or"]},
            "not": {"aliases": ["not"]},
        },
        "comparison_operators": {
            "greater_than": {"aliases": [">", "greater than"]},
            "less_than": {"aliases": ["<", "less than"]},
            "between": {"aliases": ["between", "in the range of"]},
        },
    },
    "sql_actions": {
        "count": {"aliases": ["count"], "template": "COUNT({column})", "placement": "projection",
                  "applicable_types": {"column": ["any"]}},
        "sum":   {"aliases": ["sum", "total"], "template": "SUM({column})", "placement": "projection",
                  "applicable_types": {"column": ["numeric"]}},
    }
}

TEST_BINDER: Dict[str, Any] = {
    "catalogs": {
        "tables": {"users": {}, "sales": {}},
        "columns": {
            "users.age": {"table": "users", "name": "age", "type": "integer"},
            "sales.price": {"table": "sales", "name": "price", "type": "decimal"},
        },
        "functions": {},
        "connectors": {"AND": "and", "OR": "or", "FROM": "from", "OF": "of", "COMMA": ",", "NOT": "not"},
    }
}

TEST_GRAMMAR = """
SELECT: "SELECT"
FROM: "FROM"
AND: "AND"
OR: "OR"
OF: "OF"
COMMA: ","
start: query
action: "count" | "sum"
VALUE: "VALUE"
projection: action [OF] VALUE
query: SELECT FROM | SELECT projection FROM
%import common.WS
%ignore WS
"""

def test_between_constraint_is_harvested_and_sql_builds():
    text = "show count of age from users age between 18 and 30"
    rr = map_text(text, TEST_VOCAB, TEST_BINDER, TEST_GRAMMAR, want_tree=False)
    assert rr.parse_ok, f"parse failed: {rr.parse_error}"
    cs = rr.slots.get("constraints") or []
    assert len(cs) == 1
    c = cs[0]
    assert c["column"] == "users.age"
    assert c["op"] == "between"
    assert c["values"] == ["18", "30"]
    sql = build_select_sql_from_slots(rr.slots, binder_yaml=TEST_BINDER, limit=100)
    assert "WHERE" in sql and ' "users"."age" BETWEEN 18 AND 30' in sql

def test_binary_comparator_and_negation_and_join():
    text = "show sum of price from sales price > 5 and not price < 100"
    rr = map_text(text, TEST_VOCAB, TEST_BINDER, TEST_GRAMMAR, want_tree=False)
    assert rr.parse_ok
    cs = rr.slots.get("constraints") or []
    assert len(cs) >= 2
    c1, c2 = cs[0], cs[1]
    assert c1["column"] == "sales.price" and c1["op"] == "greater_than" and c1["values"] == ["5"]
    assert c1.get("join_next") == "and"
    assert c2["column"] == "sales.price" and c2["op"] == "less_than" and c2.get("negated") is True and c2["values"] == ["100"]
    sql = build_select_sql_from_slots(rr.slots, binder_yaml=TEST_BINDER, limit=50)
    assert "WHERE" in sql and "AND NOT (" in sql
