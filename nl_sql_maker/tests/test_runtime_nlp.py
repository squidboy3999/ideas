# tests/test_runtime_nlp.py
from __future__ import annotations
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pytest

from vbg_tools.runtime_nlp import build_lexicon_and_connectors
from vbg_tools.graph_runtime import build_index, match_aliases

TEST_VOCAB = {
    "keywords": {
        "connectors": {"AND": "and", "OR": "or", "NOT": "not", "FROM": "from", "OF": "of", "COMMA": ","},
        "select_verbs": {"select": {"aliases": ["show", "display", "list"]}},
        "comparison_operators": {"greater_than": {"aliases": ["greater than"]}},
    },
    # Top-level actions per the new contract
    "sql_actions": {
        "count": {
            "aliases": ["count", "count of", "number of"],
            "template": "COUNT({column})",
            "placement": "projection",
        },
        "order_by_desc": {
            "aliases": ["order by desc", "order by descending"],
            "template": "{column}",
            "placement": "clause",
        },
        "order_by_asc": {
            "aliases": ["order by", "order"],
            "template": "{column}",
            "placement": "clause",
        },
    },
}

def _tokensets(lex):
    return {le.tokens for le in lex}

def _roles_by_canonical(lex):
    out = {}
    for le in lex:
        out.setdefault(le.canonical, set()).add(le.role)
    return out

def test_build_lexicon_includes_select_verbs_connectors_and_actions():
    lex, conns = build_lexicon_and_connectors(TEST_VOCAB)

    # Connectors map has all core items
    for k in ("AND", "OR", "NOT", "FROM", "OF", "COMMA"):
        assert k in conns and conns[k], f"Missing connector {k}"

    # Select verb aliases present
    ts = _tokensets(lex)
    assert ("show",) in ts
    assert ("display",) in ts
    assert ("list",) in ts

    # Connectors also present as lexicon entries
    assert ("and",) in ts
    assert ("from",) in ts
    assert (",",) in ts

    # Actions appear with the correct role
    roles = _roles_by_canonical(lex)
    assert "count" in roles and "sql_action" in roles["count"]
    assert "order_by_desc" in roles and "clause_action" in roles["order_by_desc"]
    assert "order_by_asc" in roles and "clause_action" in roles["order_by_asc"]

def test_greedy_match_prefers_longer_action_alias():
    # Build lexicon & index
    lex, _ = build_lexicon_and_connectors(TEST_VOCAB)
    by_len, max_len = build_index(lex)

    # "order by" vs "order" → should match the 2-token alias
    toks = ["please", "order", "by", "value"]
    spans = match_aliases(toks, by_len, max_len)

    # Expect one match covering "order by" (positions 1..3)
    assert any(s.canonical == "order_by_asc" and s.start == 1 and s.end == 3 for s in spans), \
        f"Expected longest match for 'order by'; got: {[(s.canonical, s.start, s.end) for s in spans]}"
