# tests/test_diagnostics_runtime.py
from __future__ import annotations

import re

from vbg_runtime.diagnostics import format_result, format_error


def test_format_result_debug_shows_sanitized_note():
    # Simulated success result including normalizer stats
    res = {
        "ok": True,
        "serialized_canonical": "select users.user_id, users.username from users",
        "sql": 'SELECT "users"."user_id", "users"."username" FROM "users";',
        "warnings": ["using sqlite; PostGIS functions may not execute"],
        "stats": {
            "candidates": ["select users.user_id, users.username from users"],
            "considered": 1,
            "bound": 1,
            "parsed": 1,
            "normalizer": {"sanitized_count": 1, "raw_candidates": 1},
        },
    }
    out = format_result(res, emit_mode="both", debug=True)
    assert "lists sanitized" in out  # explicit note requested
    assert "diagnostics" in out
    assert "warnings:" in out


def test_format_error_summarizes_without_stacktrace():
    err = {
        "ok": False,
        "fail_category": "binder_fail",
        "stats": {
            "candidates": ["select nope from users"],
            "considered": 1,
            "binder_errors": [
                "token 'nope' is neither a column nor a function\nTraceback (most recent call last):\n  ..."
            ],
            "normalizer": {"sanitized_count": 0, "raw_candidates": 1},
        },
    }
    out = format_error(err)
    # No raw traceback leaked
    assert "Traceback" not in out
    # Has the bucket and a short detail
    assert "[FAIL:binder_fail]" in out
    assert "Binding failed:" in out
