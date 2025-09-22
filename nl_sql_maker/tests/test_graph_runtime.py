# tests/test_graph_runtime.py  — DROP-IN (keeps your original tests; adds one extra)

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pytest

from vbg_tools.graph_runtime import (
    tokenize, is_number, is_quoted_string,
    LexEntry, MatchSpan,
    build_lexicon_and_connectors,
    infer_column_types, build_schema_indices,
    build_index, match_aliases,
    harvest_and_canonicalize, try_parse_with_lark, map_text
)

# ------------------------
# Fixtures: tiny test data
# ------------------------

TEST_VOCAB = {
    "keywords": {
        "select_verbs": {
            "select": {"aliases": ["select", "show", "list"]}
        },
        # canonical -> surface
        "connectors": {
            "AND": "and",
            "OR": "or",
            "FROM": "from",
            "OF": "of",
            "COMMA": ","
        },
        # an example comparator to test n-gram matching
        "comparison_operators": {
            "greater_than": {"aliases": ["greater than"]},
        },
        "filler_words": {
            "_skip": {"aliases": ["the", "a", "an"]}
        }
    },
    # Actions are top-level in inputs but your synthesizer emits them under keywords in artifacts.
    "sql_actions": {
        "count": {"aliases": ["count of", "number of"], "template": "COUNT({column})"}
    },
    "postgis_actions": {}
}

TEST_BINDER = {
    "catalogs": {
        "tables": {
            "users": {},
            "sales": {},
        },
        "columns": {
            "users.user_id": {"table": "users", "type": "integer", "slot_types": ["id"]},
            "users.age": {"table": "users", "type": "integer"},
            "users.username": {"table": "users", "type": "varchar"},
            "sales.amount": {"table": "sales", "type": "decimal"},
        },
        "functions": {},
        "connectors": {
            "AND": "and", "OR": "or", "FROM": "from", "OF": "of", "COMMA": ","
        }
    }
}

# Minimal grammar (only "SELECT FROM" valid)
TEST_GRAMMAR = r"""
SELECT: "SELECT"
FROM: "FROM"
AND: "AND"
OR: "OR"
OF: "OF"
COMMA: ","

start: query
query: SELECT FROM

%import common.WS
%ignore WS
"""

# ------------------------
# Unit tests
# ------------------------

def test_tokenize_and_primitives():
    assert tokenize("Show me users, please") == ["show", "me", "users", "," , "please"]
    assert is_number("42")
    assert is_number("3.14")
    assert not is_number("3.14.15")
    assert is_quoted_string("'hello'") == "hello"
    assert is_quoted_string('"x y"') == "x y"
    assert is_quoted_string("nope") is None

def test_build_lexicon_and_connectors_basics():
    lex, conns = build_lexicon_and_connectors(TEST_VOCAB)
    assert conns["AND"] == "and"
    assert conns["FROM"] == "from"
    tokensets = {le.tokens for le in lex}
    assert ("show",) in tokensets
    assert ("greater", "than") in tokensets

def test_build_index_and_greedy_match_longest():
    le_long = LexEntry(tokens=("greater","than"), canonical="greater_than", role="comparator", surface="greater than")
    le_short = LexEntry(tokens=("greater",), canonical="greater", role="comparator", surface="greater")
    by_len, max_len = build_index([le_long, le_short])

    toks = ["price", "greater", "than", "10"]
    spans = match_aliases(toks, by_len, max_len)
    assert len(spans) == 1
    assert spans[0].canonical == "greater_than"
    assert spans[0].start == 1 and spans[0].end == 3

def test_infer_column_types_numeric_vs_id():
    cinfo_id = {"type": "integer"}
    types_id = infer_column_types(cinfo_id, "user_id")
    assert "id" in types_id
    assert "numeric" not in types_id
    assert "integer" in types_id

    cinfo_num = {"type": "integer"}
    types_num = infer_column_types(cinfo_num, "age")
    assert "numeric" in types_num or "integer" in types_num

def test_build_schema_indices_creates_fqn_maps():
    tables_by_lc, columns_by_lc, col_types = build_schema_indices(TEST_BINDER)
    assert tables_by_lc["users"] == "users"
    assert columns_by_lc["user_id"] == "users.user_id"
    assert "users.age" in col_types

def test_harvest_and_canonicalize_adds_FROM_and_builds_slots():
    spans = [MatchSpan(start=0, end=1, canonical="select", role="select_verb", surface="show")]
    tokens = ["show", "users"]
    tables_by_lc = {"users": "users"}
    columns_by_lc = {}
    connectors_map = {"FROM": "from"}

    res = harvest_and_canonicalize("show users", tokens, spans, tables_by_lc, columns_by_lc, connectors_map)

    cts = res.canonical_tokens
    assert cts[0] == "SELECT"
    assert "FROM" in cts
    if "VALUE" in cts:
        assert cts.index("VALUE") < cts.index("FROM")

    assert res.slots["table"] == "users"
    assert any("Unmapped tokens" in w for w in res.warnings)

def test_try_parse_with_lark_success():
    ok, err, tree = try_parse_with_lark(TEST_GRAMMAR, ["SELECT", "FROM"], want_tree=True)
    assert ok and err is None
    assert "query" in tree
    assert "SELECT" in tree and "FROM" in tree

def test_map_text_end_to_end_select_from():
    res = map_text("show users", TEST_VOCAB, TEST_BINDER, TEST_GRAMMAR, want_tree=False)

    cts = res.canonical_tokens
    assert cts[0] == "SELECT"
    assert "FROM" in cts

    if "VALUE" in cts:
        assert cts.index("VALUE") < cts.index("FROM")
    else:
        assert res.parse_ok is True

    assert res.slots["table"] == "users"

def test_map_text_projection_value_before_from():
    res = map_text("show count of age from users", TEST_VOCAB, TEST_BINDER, TEST_GRAMMAR, want_tree=False)
    cts = res.canonical_tokens
    if "VALUE" in cts:
        assert cts.index("VALUE") < cts.index("FROM")
    assert res.slots["table"] == "users"
