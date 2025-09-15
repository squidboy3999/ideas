#!/usr/bin/env python3
# vbg_tools/runtime_helper.py
from __future__ import annotations
import os
import re
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Tuple

# -------------------------
# Constants / Paths
# -------------------------
ART_DIR_DEFAULT = "out"

VOCAB_NAME   = "graph_vocabulary.yaml"
BINDER_NAME  = "graph_binder.yaml"
GRAMMAR_NAME = "graph_grammar.lark"

# -------------------------
# Data classes
# -------------------------
@dataclass
class ArtifactPaths:
    """Absolute or relative paths to NL→SQL artifacts."""
    art_dir: str
    vocab_path: str
    binder_path: str
    grammar_path: str

@dataclass
class StepResult:
    """Uniform result for a labeled helper step."""
    step: str
    ok: bool
    info: Dict[str, Any]
    warnings: List[str]

@dataclass
class CaseExpectations:
    """Expectations for a parse-only or SQL-enabled case."""
    tokens_regex: str
    want_parse_ok: Optional[bool] = None       # None = don’t check
    min_rows: Optional[int] = None             # None = don’t check

# -------------------------
# Artifact management
# -------------------------
def resolve_artifact_paths(art_dir: Optional[str] = None) -> ArtifactPaths:
    """Label: artifacts.resolve_paths"""
    base = art_dir or os.environ.get("ARTIFACTS_DIR", ART_DIR_DEFAULT)
    return ArtifactPaths(
        art_dir=base,
        vocab_path=os.path.join(base, VOCAB_NAME),
        binder_path=os.path.join(base, BINDER_NAME),
        grammar_path=os.path.join(base, GRAMMAR_NAME),
    )

def artifacts_exist(ap: ArtifactPaths) -> bool:
    """Label: artifacts.exist"""
    return all(os.path.isfile(p) for p in [ap.vocab_path, ap.binder_path, ap.grammar_path])

def build_artifacts_inproc(ap: ArtifactPaths) -> StepResult:
    """
    Label: artifacts.build
    Build artifacts by importing your builder in-process (no bash).
    """
    warnings: List[str] = []
    try:
        # Allow import without packaging
        import sys
        sys.path.insert(0, os.getcwd())
        from vbg_tools.graph_to_artifacts import main as build_main  # type: ignore
    except Exception as e:
        return StepResult(
            step="artifacts.build",
            ok=False,
            info={"error": f"import_error: {e!r}", "art_dir": ap.art_dir},
            warnings=warnings,
        )

    try:
        rc = build_main([])  # type: ignore
        if isinstance(rc, int) and rc != 0:
            return StepResult(step="artifacts.build", ok=False,
                              info={"error": f"builder returned {rc}", "art_dir": ap.art_dir},
                              warnings=warnings)
        # Validate
        ok = artifacts_exist(ap)
        return StepResult(step="artifacts.build", ok=ok, info={"art_dir": ap.art_dir}, warnings=warnings)
    except Exception as e:
        return StepResult(step="artifacts.build", ok=False,
                          info={"error": f"builder_exception: {e!r}", "art_dir": ap.art_dir},
                          warnings=warnings)

def ensure_artifacts(ap: ArtifactPaths, auto_build: bool = True) -> StepResult:
    """
    Label: artifacts.ensure
    Ensures artifacts exist; if not and auto_build=True, attempt to build them.
    """
    if artifacts_exist(ap):
        return StepResult(step="artifacts.ensure", ok=True, info={"built": False, "art_dir": ap.art_dir}, warnings=[])
    if not auto_build:
        return StepResult(step="artifacts.ensure", ok=False, info={"built": False, "reason": "missing"}, warnings=[])
    build_res = build_artifacts_inproc(ap)
    # If builder said OK but files still missing, mark failure
    final_ok = build_res.ok and artifacts_exist(ap)
    return StepResult(step="artifacts.ensure", ok=final_ok,
                      info={"built": True, "builder_ok": build_res.ok, "art_dir": ap.art_dir},
                      warnings=build_res.warnings)

# -------------------------
# Case validation helpers
# -------------------------
def validate_parse_case_payload(
    payload: Dict[str, Any],
    expectations: CaseExpectations
) -> StepResult:
    """
    Label: validate.parse_case
    Given a runtime payload dict (the same one graph_runtime prints when --json),
    check token regex and parse_ok flag. Returns a StepResult.
    """
    warnings = list(payload.get("warnings") or [])
    tokens = payload.get("canonical_tokens") or []
    joined = " ".join(tokens)
    parse_ok = bool(payload.get("parse_ok", False))

    ok = True
    info: Dict[str, Any] = {
        "tokens": tokens,
        "joined_tokens": joined,
        "parse_ok": parse_ok,
        "slots": payload.get("slots"),
    }

    if not re.search(expectations.tokens_regex, joined or ""):
        ok = False
        warnings.append(f"tokens_regex_mismatch: expected ~ {expectations.tokens_regex!r}")

    if expectations.want_parse_ok is not None and parse_ok != expectations.want_parse_ok:
        ok = False
        warnings.append(f"parse_ok_mismatch: expected {expectations.want_parse_ok}, got {parse_ok}")

    return StepResult(step="validate.parse_case", ok=ok, info=info, warnings=warnings)

def validate_sql_case_payload(
    payload: Dict[str, Any],
    expectations: CaseExpectations
) -> StepResult:
    """
    Label: validate.sql_case
    Checks parse_ok, sql presence, and min_rows.
    """
    base = validate_parse_case_payload(payload, CaseExpectations(
        tokens_regex=expectations.tokens_regex, want_parse_ok=True
    ))
    warnings = list(base.warnings)

    sql_block = payload.get("sql") or {}
    sql_query = sql_block.get("query") or ""
    rowcount = int(sql_block.get("rowcount") or 0)

    ok = base.ok
    info = dict(base.info)
    info.update({"sql_query": sql_query, "rowcount": rowcount})

    if not sql_query:
        ok = False
        warnings.append("missing_sql_query")

    if expectations.min_rows is not None and rowcount < expectations.min_rows:
        ok = False
        warnings.append(f"min_rows_not_met: wanted ≥ {expectations.min_rows}, got {rowcount}")

    return StepResult(step="validate.sql_case", ok=ok, info=info, warnings=warnings)

# -------------------------
# Thin execution adapters
# -------------------------
def execute_parse(
    *,
    map_text: Callable[[str, Dict[str, Any], Dict[str, Any], str, bool], Any],
    text: str,
    vocab_yaml: Dict[str, Any],
    binder_yaml: Dict[str, Any],
    grammar_text: str,
    want_tree: bool = False
) -> Tuple[StepResult, Dict[str, Any]]:
    """
    Label: exec.parse
    Calls your map_text() and returns (StepResult, payload_dict).
    The payload mirrors graph_runtime's JSON structure.
    """
    res = map_text(text, vocab_yaml, binder_yaml, grammar_text, want_tree)  # your existing function
    payload = {
        "canonical_tokens": res.canonical_tokens,
        "slots": res.slots,
        "spans": res.spans,
        "parse_ok": res.parse_ok,
        "warnings": list(res.warnings or []),
    }
    if want_tree and res.tree:
        payload["tree"] = res.tree
    return StepResult(step="exec.parse", ok=True, info={"text": text}, warnings=list(res.warnings or [])), payload

def attach_sql_if_requested(
    *,
    payload: Dict[str, Any],
    parse_ok: bool,
    want_sql: bool,
    db_path: Optional[str],
    limit: int,
    binder_yaml: Dict[str, Any],
    build_select_sql_from_slots: Callable[..., str],
    execute_sqlite: Callable[..., Dict[str, Any]]
) -> StepResult:
    """
    Label: exec.sql_optional
    If parse_ok and (want_sql or db_path is set), build SQL and optionally execute.
    """
    warnings: List[str] = list(payload.get("warnings") or [])
    if not parse_ok or not (want_sql or db_path):
        return StepResult(step="exec.sql_optional", ok=True, info={"skipped": True}, warnings=warnings)

    try:
        sql_stmt = build_select_sql_from_slots(payload["slots"], binder_yaml, limit=limit)
        sql_block: Dict[str, Any] = {"query": sql_stmt}
        if db_path:
            exec_res = execute_sqlite(db_path, sql_stmt)
            sql_block.update(exec_res)
            sql_block["db_path"] = db_path
        payload["sql"] = sql_block
        return StepResult(step="exec.sql_optional", ok=True, info={"executed": bool(db_path)}, warnings=warnings)
    except Exception as e:
        warnings.append(f"SQL error: {e}")
        payload["warnings"] = warnings
        return StepResult(step="exec.sql_optional", ok=False, info={"error": repr(e)}, warnings=warnings)
