# tests/test_constraints_in_operator_forms.py
from __future__ import annotations
from typing import Dict, Any
from vbg_tools.graph_runtime import map_text
from vbg_tools.sql_helpers import build_select_sql_from_slots

TEST_VOCAB: Dict[str, Any] = {
    "keywords": {
        "connectors": {"AND": "and", "OR": "or", "NOT": "not", "FROM": "from", "OF": "of", "COMMA": ","},
        "select_verbs": {"select": {"aliases": ["show"]}},
        "logical_operators": {"and": {"aliases": ["and"]}, "or": {"aliases": ["or"]}, "not": {"aliases": ["not"]}},
        "comparison_operators": {
            "in": {"aliases": ["in", "is in"]},
            "like": {"aliases": ["like", "matches"]},
            "is_null": {"aliases": ["is null"]},
            "is_not_null": {"aliases": ["is not null"]},
        },
    },
    "sql_actions": {
        "count": {"aliases": ["count"], "template": "COUNT({column})", "placement": "projection",
                  "applicable_types": {"column": ["any"]}},
    }
}
TEST_BINDER: Dict[str, Any] = {
    "catalogs": {
        "tables": {"users": {}},
        "columns": {
            "users.user_id": {"table": "users", "name": "user_id", "type": "integer"},
            "users.age": {"table": "users", "name": "age", "type": "integer"},
            "users.name": {"table": "users", "name": "name", "type": "text"},
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
action: "count"
VALUE: "VALUE"
projection: action [OF] VALUE
query: SELECT FROM | SELECT projection FROM
%import common.WS
%ignore WS
"""

def test_in_list_and_like_and_null_checks():
    # IN list
    rr1 = map_text("show count of age from users age in 5, 10", TEST_VOCAB, TEST_BINDER, TEST_GRAMMAR, want_tree=False)
    assert rr1.parse_ok
    cs1 = rr1.slots.get("constraints") or []
    assert cs1 and cs1[0]["op"] == "in" and cs1[0]["values"] == ["5", "10"]

    # LIKE
    rr2 = map_text("show count of name from users name like 'a%'", TEST_VOCAB, TEST_BINDER, TEST_GRAMMAR, want_tree=False)
    cs2 = rr2.slots.get("constraints") or []
    assert cs2 and cs2[0]["op"] == "like" and cs2[0]["values"] == ["a%"]

    # IS NULL
    rr3 = map_text("show count of age from users age is null", TEST_VOCAB, TEST_BINDER, TEST_GRAMMAR, want_tree=False)
    cs3 = rr3.slots.get("constraints") or []
    assert cs3 and cs3[0]["op"] == "is_null" and cs3[0]["values"] == []

    # IS NOT NULL
    rr4 = map_text("show count of age from users age is not null", TEST_VOCAB, TEST_BINDER, TEST_GRAMMAR, want_tree=False)
    cs4 = rr4.slots.get("constraints") or []
    assert cs4 and cs4[0]["op"] == "is_not_null" and cs4[0]["values"] == []

    # Sanity SQL
    sql = build_select_sql_from_slots(rr2.slots, binder_yaml=TEST_BINDER, limit=25)
    assert "WHERE" in sql and "LIKE" in sql
