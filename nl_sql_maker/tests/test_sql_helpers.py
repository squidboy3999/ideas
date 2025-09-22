# tests/test_sql_helpers.py
from __future__ import annotations

import re
import pytest

from vbg_tools.sql_helpers import build_select_sql_from_slots

# Minimal binder with functions & columns your builder expects
BINDER = {
    "catalogs": {
        "tables": {
            "users": {},
            "sales": {},
        },
        "columns": {
            "users.user_id": {"table": "users", "name": "user_id", "type": "integer"},
            "users.age": {"table": "users", "name": "age", "type": "integer"},
            "users.username": {"table": "users", "name": "username", "type": "varchar"},
            "sales.price": {"table": "sales", "name": "price", "type": "decimal"},
            "sales.product_name": {"table": "sales", "name": "product_name", "type": "varchar"},
        },
        "functions": {
            # projection actions
            "count": {"template": "COUNT({column})"},
            "sum": {"template": "SUM({column})"},
            # clause actions
            "order_by_asc": {"template": "ORDER BY {column} ASC", "phase_index": 30},
            "order_by_desc": {"template": "ORDER BY {column} DESC", "phase_index": 30},
            "limit": {"template": "LIMIT {value}", "phase_index": 40},
            "limit_one": {"template": "LIMIT 1", "phase_index": 40},
        },
        "connectors": {"AND": "and", "OR": "or", "FROM": "from", "OF": "of", "COMMA": ","},
    }
}

def _squash_ws(s: str) -> str:
    return re.sub(r"\s+", " ", s.strip())

def test_projection_only_count_default_limit():
    slots = {
        "table": "users",
        "columns": ["users.age"],
        "actions": ["count"],
        "values": [],
        "constraints": [],
        "clause_actions": [],
    }
    sql = build_select_sql_from_slots(slots, BINDER, limit=50)
    assert _squash_ws(sql) == 'SELECT COUNT("users"."age") FROM "users" LIMIT 50'

def test_constraints_render_and_value_consumption_for_limit():
    # values: first (10) consumed by WHERE, second (99) consumed by LIMIT {value}
    slots = {
        "table": "sales",
        "columns": ["sales.price"],
        "actions": ["count"],           # projection is fine
        "clause_actions": ["limit"],    # LIMIT {value}
        "values": ["10", "99"],
        "constraints": [
            {
                "column": "sales.price",
                "op": "greater_than",
                "values": ["10"],
                "negated": False,
            }
        ]
    }
    sql = build_select_sql_from_slots(slots, BINDER, limit=1000)
    # WHERE uses 10; LIMIT uses remaining 99
    want = 'SELECT COUNT("sales"."price") FROM "sales" WHERE "sales"."price" > 10 LIMIT 99'
    assert _squash_ws(sql) == _squash_ws(want)

def test_negation_and_or_joining_in_where():
    slots = {
        "table": "users",
        "columns": ["users.username"],
        "actions": [],
        "clause_actions": [],
        "values": ["foo", "bar%"],
        "constraints": [
            {"column": "users.username", "op": "equal", "values": ["foo"], "negated": True, "join_next": "or"},
            {"column": "users.username", "op": "like", "values": ["bar%"], "negated": False},
        ],
    }
    sql = build_select_sql_from_slots(slots, BINDER, limit=5)
    want = 'SELECT "users"."username" FROM "users" WHERE NOT ("users"."username" = \'foo\') OR "users"."username" LIKE \'bar%\' LIMIT 5'
    assert _squash_ws(sql) == _squash_ws(want)

def test_clause_ordering_order_by_before_limit():
    slots = {
        "table": "sales",
        "columns": ["sales.price"],
        "actions": [],
        "clause_actions": ["order_by_asc", "limit"],
        "values": ["100"],   # consumed by LIMIT {value}
        "constraints": [],
    }
    sql = build_select_sql_from_slots(slots, BINDER, limit=10)
    # ORDER BY must precede LIMIT
    want_prefix = 'SELECT "sales"."price" FROM "sales" ORDER BY "sales"."price" ASC LIMIT 100'
    assert _squash_ws(sql).startswith(_squash_ws(want_prefix))

def test_limit_one_prevents_default_limit_duplication():
    slots = {
        "table": "users",
        "columns": ["users.age"],
        "actions": ["count"],
        "clause_actions": ["limit_one"],  # emits LIMIT 1
        "values": [],
        "constraints": [],
    }
    sql = build_select_sql_from_slots(slots, BINDER, limit=999)
    # Only one LIMIT should appear
    s = _squash_ws(sql)
    assert s.endswith("LIMIT 1")
    assert s.count("LIMIT") == 1

def test_between_and_in_constraints():
    slots = {
        "table": "sales",
        "columns": ["sales.price"],
        "actions": ["sum"],
        "clause_actions": [],
        "values": [],
        "constraints": [
            {"column": "sales.price", "op": "between", "values": ["5", "100"], "negated": False, "join_next": "and"},
            {"column": "sales.product_name", "op": "in", "values": ["A", "B", "C"], "negated": False},
        ],
    }
    sql = build_select_sql_from_slots(slots, BINDER, limit=25)
    want = (
        'SELECT SUM("sales"."price") FROM "sales" '
        'WHERE "sales"."price" BETWEEN 5 AND 100 AND "sales"."product_name" IN (\'A\', \'B\', \'C\') '
        'LIMIT 25'
    )
    assert _squash_ws(sql) == _squash_ws(want)
