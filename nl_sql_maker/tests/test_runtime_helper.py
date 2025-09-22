# tests/test_runtime_helper.py
from __future__ import annotations
from pathlib import Path
import os
import yaml
import pytest

from vbg_tools.runtime_helper import (
    resolve_artifact_paths,
    artifacts_exist,
    ensure_artifacts,
    build_artifacts_inproc,
)

def _write_yaml(path: Path, obj):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        yaml.safe_dump(obj, f, sort_keys=False, allow_unicode=True)

def _minimal_inputs(dirpath: Path):
    """
    Enriched minimal inputs that satisfy the new generator:
      - keywords: connectors, select_verbs, logical_operators, comparison_operators
      - top-level sql_actions with at least one projection (count/sum/avg)
      - simple schema with integer/decimal/text
      - (optional) global_templates.select_template to keep downstream helpers happy
    """
    keywords = {
        "keywords": {
            "connectors": {
                "AND": "and",
                "OR": "or",
                "NOT": "not",
                "FROM": "from",
                "OF": "of",
                "COMMA": ",",
            },
            "select_verbs": {
                "select": {"aliases": ["show", "display", "list"]},
            },
            # very small logical/comparison set so synthesizer/runtime helpers have data
            "logical_operators": {
                "and": {"aliases": ["and"]},
                "or":  {"aliases": ["or"]},
                "not": {"aliases": ["not"]},
            },
            "comparison_operators": {
                "greater_than": {"aliases": ["greater than"]},
                "less_than":    {"aliases": ["less than"]},
                "between":      {"aliases": ["between"]},
                "equal":        {"aliases": ["equals"]},
            },
            # optional, but keeps some helpers deterministic
            "filler_words": {"_skip": {"aliases": ["the", "a", "an", "are", "for"]}},
        },
        # Top-level sql_actions (include at least one projection action with a {column} arg)
        "sql_actions": {
            "count": {
                "placement": "projection",
                "bind_style": "of",
                "aliases": ["count", "number of"],
                "template": "COUNT({column})",
                "applicable_types": {"column": ["any"]},
            },
            "sum": {
                "placement": "projection",
                "bind_style": "of",
                "aliases": ["sum", "total"],
                "template": "SUM({column})",
                "applicable_types": {"column": ["numeric"]},
            },
            "avg": {
                "placement": "projection",
                "bind_style": "of",
                "aliases": ["avg", "average"],
                "template": "AVG({column})",
                "applicable_types": {"column": ["numeric"]},
            },
            # clause actions are fine to include but aren’t strictly required here
            "limit_one": {
                "placement": "clause",
                "aliases": ["first"],
                "template": "LIMIT 1",
            },
            "limit": {
                "placement": "clause",
                "aliases": ["limit"],
                "template": "LIMIT {value}",
            },
        },
        # optional global template to keep any template-based helpers content
        "global_templates": {
            "select_template": "SELECT {columns} FROM {table} {constraints}"
        },
    }

    schema = {
        "tables": {
            "users": {
                "columns": [
                    {"name": "user_id",  "type": "integer"},
                    {"name": "age",      "type": "integer"},
                    {"name": "username", "type": "text"},
                ]
            },
            "sales": {
                "columns": [
                    {"name": "sale_id",  "type": "integer"},
                    {"name": "price",    "type": "decimal"},
                ]
            },
        }
    }

    _write_yaml(dirpath / "keywords_and_functions.yaml", keywords)
    _write_yaml(dirpath / "schema.yaml", schema)

def test_resolve_artifact_paths_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    adir = str(tmp_path / "my_artifacts")
    monkeypatch.setenv("ARTIFACTS_DIR", adir)
    ap = resolve_artifact_paths()
    assert ap.art_dir == adir
    assert ap.vocab_path.endswith("graph_vocabulary.yaml")
    assert ap.binder_path.endswith("graph_binder.yaml")
    assert ap.grammar_path.endswith("graph_grammar.lark")

def test_build_artifacts_inproc_missing_inputs(tmp_path: Path):
    # No input files, builder should fail gracefully
    ap = resolve_artifact_paths(str(tmp_path))
    res = build_artifacts_inproc(ap)
    assert res.ok is False
    assert "error" in res.info

def test_ensure_artifacts_builds_when_missing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    # Prepare a temp artifacts dir with enriched minimal inputs
    adir = tmp_path / "artifacts"
    adir.mkdir(parents=True, exist_ok=True)
    _minimal_inputs(adir)

    ap = resolve_artifact_paths(str(adir))
    # Sanity: artifacts don't exist yet
    assert not artifacts_exist(ap)

    # Ensure will auto-build using the new generator
    res = ensure_artifacts(ap, auto_build=True)
    assert res.ok, f"ensure_artifacts failed: {res.info}"

    # Now artifacts should be present and non-empty
    assert artifacts_exist(ap), "Expected generator to write all artifact files."
    for fn in ("graph_vocabulary.yaml", "graph_binder.yaml", "graph_grammar.lark"):
        p = adir / fn
        assert p.exists(), f"Missing generated artifact: {fn}"
        assert p.stat().st_size > 0, f"Generated artifact is empty: {fn}"
