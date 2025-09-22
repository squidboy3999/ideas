# tests/test_create_cli_test.py
from __future__ import annotations

import os
import stat
from pathlib import Path
import textwrap
import yaml

import pytest

from vbg_tools.create_cli_test import generate_cli_test

def _write_yaml(path: Path, obj):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        yaml.safe_dump(obj, f, sort_keys=False)

def _minimal_artifacts(tmp: Path):
    # vocabulary with connectors + select verbs + basic actions
    vocab = {
        "keywords": {
            "connectors": {"AND": "and", "OR": "or", "NOT": "not", "FROM": "from", "OF": "of", "COMMA": ","},
            "select_verbs": {"select": {"aliases": ["show", "display"]}},
        },
        "sql_actions": {
            "limit_one": {"aliases": ["first"]},
            "limit": {"aliases": ["limit"]},
        },
    }
    binder = {
        "catalogs": {
            "tables": {"users": {}, "sales": {}},
            "columns": {
                "users.user_id": {"table": "users", "name": "user_id", "type": "integer", "slot_types": ["id"]},
                "users.age": {"table": "users", "name": "age", "type": "integer", "slot_types": ["integer", "numeric"]},
                "sales.price": {"table": "sales", "name": "price", "type": "decimal", "slot_types": ["numeric"]},
            },
            "connectors": {"AND": "and", "OR": "or", "FROM": "from", "OF": "of", "COMMA": ","},
        }
    }
    _write_yaml(tmp / "graph_vocabulary.yaml", vocab)
    _write_yaml(tmp / "graph_binder.yaml", binder)

def _surfaces_files(tmp: Path):
    gold = [
        {"natural_language": "show from users", "sql_expression": "SELECT * FROM users"},
        {"natural_language": "display first from users", "sql_expression": "SELECT * FROM users LIMIT 1"},
    ]
    multi = [
        {"natural_language": "show from sales", "sql_expressions": ["SELECT * FROM sales", "SELECT price FROM sales"]},
        {"natural_language": "display from users", "sql_paths": ["SELECT * FROM users", "SELECT age FROM users"]},
    ]
    invalid = [
        {"natural_language": "frobnicate the widgets"},  # invalid on purpose
    ]
    _write_yaml(tmp / "gold_surfaces.yml", gold)
    _write_yaml(tmp / "multipath_surfaces.yml", multi)
    _write_yaml(tmp / "invalid_surfaces.yml", invalid)

def test_generates_executable_script_and_sections(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    out_dir = tmp_path / "out"
    _minimal_artifacts(out_dir)
    _surfaces_files(out_dir)

    # disable predicate smoke to keep assertions deterministic
    monkeypatch.setenv("CLI_TEST_INCLUDE_PREDS", "0")

    script_path = generate_cli_test(art_dir=out_dir)
    assert script_path.exists(), "cli_test.sh should be created"

    # Executable bit set
    mode = script_path.stat().st_mode
    assert mode & stat.S_IXUSR, "cli_test.sh should be executable by user"

    text = script_path.read_text(encoding="utf-8")

    # Featured section: must include a 'first' example (limit_one) against an existing table.
    assert "featured: " in text
    assert "first" in text
    assert "from users" in text or "from sales" in text

    # Gold surfaces present and echoed
    assert "Gold surfaces" in text or "Gold surfaces".lower() in text.lower()
    assert "show from users" in text

    # Multipath NL lines present with expected-path comments
    assert "Multipath surfaces" in text or "Multipath surfaces".lower() in text.lower()
    assert "display from users" in text
    assert "# expected path 1:" in text
    assert "# expected path 2:" in text

    # Invalid section contains warning logic
    assert "Invalid surfaces" in text or "Invalid surfaces".lower() in text.lower()
    assert "frobnicate the widgets" in text

def test_single_quote_escaping(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    out_dir = tmp_path / "out"
    _minimal_artifacts(out_dir)
    _write_yaml(out_dir / "gold_surfaces.yml", [
        {"natural_language": "show user's data from users", "sql_expression": "SELECT * FROM users"}
    ])
    _write_yaml(out_dir / "multipath_surfaces.yml", [])
    _write_yaml(out_dir / "invalid_surfaces.yml", [])

    monkeypatch.setenv("CLI_TEST_INCLUDE_PREDS", "0")

    script_path = generate_cli_test(art_dir=out_dir)
    text = script_path.read_text(encoding="utf-8")

    # Ensure NL is wrapped with single quotes and inner quote escaped safely for bash
    assert '"$RUNTIME" --sql --db "${DB_PATH}"' in text
    assert "'show user'\"'\"'s data from users'" in text

def test_max_items_limits_gold(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    out_dir = tmp_path / "out"
    _minimal_artifacts(out_dir)
    _write_yaml(out_dir / "gold_surfaces.yml", [
        {"natural_language": f"query {i}", "sql_expression": f"SELECT {i}"} for i in range(10)
    ])
    _write_yaml(out_dir / "multipath_surfaces.yml", [])
    _write_yaml(out_dir / "invalid_surfaces.yml", [])

    monkeypatch.setenv("CLI_TEST_INCLUDE_PREDS", "0")

    script_path = generate_cli_test(art_dir=out_dir, max_items=3)
    text = script_path.read_text(encoding="utf-8")

    # Only first three gold items should be present
    assert "query 0" in text and "query 1" in text and "query 2" in text
    assert "query 3" not in text
