# tests/test_grammar_validator.py
from __future__ import annotations

import random
from typing import Dict, Any, List

import pytest
from lark import Lark

from vbg_generate.grammar_validator import GrammarAnalyzer, SmartGenerator
from vbg_shared.schema_utils import is_geometry_col, table_to_columns


def _mini_grammar() -> str:
    # Minimal canonical grammar that aligns with tokens the analyzer treats as terminals
    return r"""
    ?query: SELECT select_list FROM TABLE
    select_list: COLUMN
               | COLUMN AND COLUMN
               | COLUMN (COMMA COLUMN)* (AND COLUMN)?

    SELECT: "select"
    FROM: "from"
    AND: "and"
    COMMA: ","

    TABLE: "users"
    COLUMN: "users.name" | "users.age"

    %import common.WS
    %ignore WS
    """


def _mini_graph() -> Dict[str, Any]:
    # Simple graph with one table and two columns
    return {
        "users": {"entity_type": "table", "metadata": {"columns": {"users.name": {}, "users.age": {}}}},
        "users.name": {"entity_type": "column", "metadata": {"type": "text", "labels": []}},
        "users.age": {"entity_type": "column", "metadata": {"type": "int", "labels": []}},
        # Include a clause-like function and a regular function to test deny-listing and pool building
        "order_by_asc": {
            "entity_type": "sql_actions",
            "binder": {"class": "ordering", "clause": "order_by"},
            "metadata": {"aliases": []},
        },
        "sum": {
            "entity_type": "sql_actions",
            "binder": {"class": "aggregate", "clause": "select"},
            "metadata": {"aliases": []},
        },
    }

def _graph_dotted_with_basename_table_meta():
    # Column nodes are dotted; table metadata lists basenames (buggy case)
    return {
        "regions": {"entity_type": "table", "metadata": {"columns": {"boundaries": {}, "name": {}}}},
        "regions.boundaries": {"entity_type": "column", "metadata": {"table": "regions", "type": "geometry"}},
        "regions.name": {"entity_type": "column", "metadata": {"table": "regions", "type": "text"}},
        # minimal grammar terminals
        "select": {"entity_type": "prepositions", "metadata": {"aliases": ["select"]}},
        "of": {"entity_type": "prepositions", "metadata": {"aliases": ["of"]}},
        "from": {"entity_type": "prepositions", "metadata": {"aliases": ["from"]}},
        "and": {"entity_type": "prepositions", "metadata": {"aliases": ["and"]}},
        ",": {"entity_type": "prepositions", "metadata": {"aliases": [","]}},
    }

_MIN_GRAMMAR = r"""
?query: "select" select_list "from" TABLE
select_list: item
           | item "and" item
           | item "," "and" item
item: COLUMN
COLUMN: /[A-Za-z0-9_.]+/
TABLE: /[A-Za-z0-9_]+/
%ignore " "
"""

def test_smart_generator_uses_dotted_columns_even_if_table_meta_has_basenames():
    graph = _graph_dotted_with_basename_table_meta()
    parser = Lark(_MIN_GRAMMAR, start="query")
    analyzer = GrammarAnalyzer(parser)
    gen = SmartGenerator(parser, graph, analyzer)

    canonical, info = gen.generate(graph)
    assert canonical, "generator should emit a canonical string"
    # must contain dotted column ids, never bare 'boundaries' or 'name'
    assert "regions.boundaries" in canonical or "regions.name" in canonical
    assert "select " in canonical and " from regions" in canonical
    
def test_analyzer_min_depth_converges():
    parser = Lark(_mini_grammar(), start="query")
    ga = GrammarAnalyzer(parser)

    assert "query" in ga.min_depths
    assert "select_list" in ga.min_depths

    # Depths should be finite and reasonably small for this tiny grammar
    assert ga.min_depths["query"] < 20
    assert ga.min_depths["select_list"] < 20


def test_smart_generator_function_pool_filters_clause_functions():
    parser = Lark(_mini_grammar(), start="query")
    ga = GrammarAnalyzer(parser)
    graph = _mini_graph()

    gen = SmartGenerator(parser, graph, ga)

    # Clause-like functions should not appear in FUNCTION pool; 'sum' should.
    fn_pool = gen.vocab.get("FUNCTION", [])
    assert "order_by_asc" not in fn_pool
    assert "sum" in fn_pool


def test_table_to_columns_maps_dict_shape():
    parser = Lark(_mini_grammar(), start="query")
    ga = GrammarAnalyzer(parser)
    graph = _mini_graph()
    gen = SmartGenerator(parser, graph, ga)

    mapping = table_to_columns(graph)
    assert mapping == {"users": ["users.name", "users.age"]}


def test_is_geometry_col_via_type_or_label():
    parser = Lark(_mini_grammar(), start="query")
    ga = GrammarAnalyzer(parser)
    # Start with a base graph and add geometry columns in two ways
    graph = _mini_graph()
    graph["regions"] = {"entity_type": "table", "metadata": {"columns": {"regions.boundaries": {}, "regions.geom2": {}}}}
    graph["regions.boundaries"] = {
        "entity_type": "column",
        "metadata": {"type": "geometry", "type_category": "geometry", "labels": []},
    }
    graph["regions.geom2"] = {
        "entity_type": "column",
        "metadata": {"type": "text", "labels": ["postgis"]},  # label implies spatial
    }

    gen = SmartGenerator(parser, graph, ga)

    assert is_geometry_col(graph, "regions.boundaries") is True
    assert is_geometry_col(graph, "regions.geom2") is True
    assert is_geometry_col(graph, "users.age") is False


def test_generate_returns_basic_canonical_when_vocab_present():
    parser = Lark(_mini_grammar(), start="query")
    ga = GrammarAnalyzer(parser)
    graph = _mini_graph()

    gen = SmartGenerator(parser, graph, ga)
    # Ensure it can produce a canonical string (it may vary)
    s, info = gen.generate(graph)
    assert isinstance(s, str)
    assert s.startswith("select ")
    assert " from " in s
