# tests/test_grammar_compile.py
from __future__ import annotations
import pytest

def test_grammar_compiles(grammar_text):
    try:
        from lark import Lark
    except Exception as e:
        pytest.skip(f"Lark not available: {e!r}")
    # Try compiling with common start rule 'query'
    Lark(grammar_text, start="query", parser="lalr")
