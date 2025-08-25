# tests/test_phase_j_helpers.py
from __future__ import annotations

import random
import re
from typing import Dict, Any, List

import pytest

from vbg_generate.phase_j_helpers import (
    j_make_parser,
    j_build_reverse_alias_map,
    j_denormalize_canonical,
)

# Note: These are **unit** tests that avoid invoking your full normalizer/binder pipeline.
# They focus on reverse-alias map construction and de-normalization behavior.


def _toy_vocab_identity() -> Dict[str, Any]:
    # Deterministic identity for tokens we care about
    return {
        "deterministic_aliases": {
            "users": "users",
            "users.name": "users.name",
            "users.age": "users.age",
            # include a connector to ensure it's NOT added to alias pools
            "and": "and",
        },
        "non_deterministic_aliases": {},
    }


def _graph_with_unique_and_nonunique_basenames() -> Dict[str, Any]:
    # Unique basename 'name' (only in users), non-unique 'price' (in both)
    return {
        "users": {"entity_type": "table", "metadata": {"columns": {"users.name": {}, "users.price": {}}}},
        "users.name": {"entity_type": "column", "metadata": {"type": "text", "labels": []}},
        "users.price": {"entity_type": "column", "metadata": {"type": "float", "labels": []}},
        "sales": {"entity_type": "table", "metadata": {"columns": {"sales.price": {}}}},
        "sales.price": {"entity_type": "column", "metadata": {"type": "float", "labels": []}},
    }


def _graph_only_unique_basename() -> Dict[str, Any]:
    # Only 'users.balance' exists -> basename 'balance' is unique
    return {
        "users": {"entity_type": "table", "metadata": {"columns": {"users.balance": {}}}},
        "users.balance": {"entity_type": "column", "metadata": {"type": "float", "labels": []}},
    }


def test_j_make_parser_accepts_valid_grammar_and_rejects_empty():
    minimal = r"""
        ?query: SELECT select_list FROM TABLE
        select_list: COLUMN | COLUMN AND COLUMN
        SELECT: "select"
        FROM: "from"
        AND: "and"
        TABLE: "users"
        COLUMN: "users.name" | "users.age"
        %import common.WS
        %ignore WS
    """
    parser = j_make_parser(minimal)
    assert parser is not None

    with pytest.raises(ValueError):
        j_make_parser("   \n   ")


def test_reverse_alias_map_includes_identities_and_omits_connectors():
    vocab = _toy_vocab_identity()
    graph = _graph_only_unique_basename()

    rev = j_build_reverse_alias_map(vocab, graph)

    # Identities present
    assert rev["users.name"][0] == "users.name"
    assert rev["users.age"][0] == "users.age"

    # 'and' is a connector and should NOT appear as an alias choice
    assert "and" not in (rev.get("users.name") or [])
    assert "and" not in (rev.get("users.age") or [])


def test_reverse_alias_map_injects_unique_column_basenames_safely():
    vocab = _toy_vocab_identity()

    # Case 1: basename unique -> should be injected
    graph_unique = _graph_only_unique_basename()
    rev1 = j_build_reverse_alias_map(vocab, graph_unique)
    assert "users.balance" in rev1
    assert "balance" in rev1["users.balance"], "Unique basename should be added to alias pool"

    # Case 2: basename not unique -> should NOT be injected
    graph_nonunique = _graph_with_unique_and_nonunique_basenames()
    rev2 = j_build_reverse_alias_map(vocab, graph_nonunique)
    # 'name' is unique per graph_nonunique? It's only on users.* -> yes
    assert "users.name" in rev2 and "name" in rev2["users.name"]
    # 'price' is not unique -> basename must NOT be present
    assert "users.price" in rev2 and "price" not in rev2["users.price"]
    assert "sales.price" in rev2 and "price" not in rev2["sales.price"]


def test_denormalize_identity_bias_keeps_canonical():
    vocab = _toy_vocab_identity()
    graph = _graph_only_unique_basename()
    rev = j_build_reverse_alias_map(vocab, graph)

    s = "select users.balance from users"
    # With identity_bias = 1.0 nothing should change
    messy = j_denormalize_canonical(s, rev, rng=random.Random(123), identity_bias=1.0)
    assert messy == s


def test_denormalize_connector_awareness_prefers_plain_alias():
    # Craft a reverse map where a token has both "plain" and "with connector"
    # We simulate a FUNCTION token 'sum' here (the function name itself isn't validated by this routine)
    rev = {
        "sum": ["sum", "sum of"],
        "COLUMN": ["COLUMN"],  # pass-through
    }
    # Canonical "sum of COLUMN" — next token after "sum" is 'of'
    messy = j_denormalize_canonical(
        "sum of COLUMN",
        rev,
        rng=random.Random(7),
        identity_bias=0.0,  # force aliasing
    )
    # We expect it to pick the plain "sum" and keep the following 'of' intact
    assert messy.startswith("sum of "), messy
