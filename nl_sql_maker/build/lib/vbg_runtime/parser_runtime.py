# vbg_runtime/parser_runtime.py
from __future__ import annotations

from typing import Any
from lark import Lark


class ParserBuildError(ValueError):
    """Raised when the grammar fails to compile into a Lark parser."""


def make_parser(grammar_text: str) -> Lark:
    """
    Build a Lark parser with start='query'.
    Raises ParserBuildError with a clear message if compilation fails.
    """
    if not isinstance(grammar_text, str) or not grammar_text.strip():
        raise ParserBuildError("Grammar text must be a non-empty string.")
    try:
        return Lark(grammar_text, start="query")
    except Exception as e:
        raise ParserBuildError(f"Failed to compile grammar: {e}") from e


def parse_canonical(parser: Lark, canonical_text: str) -> bool:
    """
    Attempt to parse a canonical string with the provided parser.
    Contract: returns True/False (never raises on parse).
    """
    if not isinstance(canonical_text, str) or not canonical_text.strip():
        return False
    try:
        parser.parse(canonical_text)
        return True
    except Exception:
        return False
