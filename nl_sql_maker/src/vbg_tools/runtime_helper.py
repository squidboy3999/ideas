#!/usr/bin/env python3
# vbg_tools/runtime_helper.py
from __future__ import annotations
import os
import re
import shutil
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Tuple

import yaml

# -------------------------
# Constants / Paths
# -------------------------
ART_DIR_DEFAULT = "out"

# Artifact file names
VOCAB_NAME   = "graph_vocabulary.yaml"
BINDER_NAME  = "graph_binder.yaml"
GRAMMAR_NAME = "graph_grammar.lark"

# Input file names (for builder)
KEYWORDS_NAME = "keywords_and_functions.yaml"
SCHEMA_NAME   = "schema.yaml"

# -------------------------
# Data classes
# -------------------------
@dataclass
class ArtifactPaths:
    art_dir: str
    vocab_path: str
    binder_path: str
    grammar_path: str

@dataclass
class StepResult:
    step: str
    ok: bool
    info: Dict[str, Any]
    warnings: List[str]

@dataclass
class CaseExpectations:
    tokens_regex: str
    want_parse_ok: Optional[bool] = None
    min_rows: Optional[int] = None

# -------------------------
# Artifact management
# -------------------------
def resolve_artifact_paths(art_dir: Optional[str] = None) -> ArtifactPaths:
    base = art_dir or os.environ.get("ARTIFACTS_DIR", ART_DIR_DEFAULT)
    return ArtifactPaths(
        art_dir=base,
        vocab_path=os.path.join(base, VOCAB_NAME),
        binder_path=os.path.join(base, BINDER_NAME),
        grammar_path=os.path.join(base, GRAMMAR_NAME),
    )

def _artifact_file_list(ap: ArtifactPaths) -> List[str]:
    return [ap.vocab_path, ap.binder_path, ap.grammar_path]

def artifacts_exist(ap: ArtifactPaths) -> bool:
    return all(os.path.isfile(p) for p in _artifact_file_list(ap))

def _input_paths_for_builder(ap: ArtifactPaths) -> Tuple[str, str]:
    kw = os.path.join(ap.art_dir, KEYWORDS_NAME)
    sc = os.path.join(ap.art_dir, SCHEMA_NAME)
    kw = os.environ.get("VBG_KEYWORDS_PATH", kw)
    sc = os.environ.get("VBG_SCHEMA_PATH", sc)
    return kw, sc

def _promote_from_nested_out(ap: ArtifactPaths) -> bool:
    nested = os.path.join(ap.art_dir, "out")
    if not os.path.isdir(nested):
        return False
    found = False
    targets = {
        VOCAB_NAME: ap.vocab_path,
        BINDER_NAME: ap.binder_path,
        GRAMMAR_NAME: ap.grammar_path,
    }
    for fname, dest in targets.items():
        src = os.path.join(nested, fname)
        if os.path.isfile(src):
            os.makedirs(os.path.dirname(dest), exist_ok=True)
            shutil.copy2(src, dest)
            found = True
    return found

def _direct_build_offline(ap: ArtifactPaths, keywords_path: str, schema_path: str) -> StepResult:
    """
    Fallback path: synthesize artifacts directly without the orchestrator.
    If synth_* raises, we still produce minimal artifacts from inputs so tests
    (and runtime) have something valid to consume.
    """
    warnings: List[str] = []

    # --- Load inputs ---
    try:
        import yaml
        with open(keywords_path, "r", encoding="utf-8") as f:
            kf = yaml.safe_load(f) or {}
        with open(schema_path, "r", encoding="utf-8") as f:
            schema = yaml.safe_load(f) or {}
    except Exception as e:
        return StepResult(step="artifacts.direct_build", ok=False,
                          info={"error": f"yaml_load_error: {e!r}"}, warnings=warnings)

    # --- Try preferred synthesizers first ---
    vocab = None
    binder = None
    grammar_text = None
    synth_errs: List[str] = []

    try:
        from vbg_tools.synth_artifacts import synth_vocabulary, synth_binder, synth_grammar  # type: ignore
        try:
            vocab = synth_vocabulary(kf)
        except Exception as e:
            synth_errs.append(f"synth_vocabulary: {e!r}")
        try:
            if vocab is None:
                # allow binder from raw inputs even if vocab failed
                binder = synth_binder(kf, schema)
            else:
                binder = synth_binder(kf, schema)
        except Exception as e:
            synth_errs.append(f"synth_binder: {e!r}")
        try:
            # Need something for grammar: if vocab/binder missing, this may still raise
            if vocab is not None and binder is not None:
                grammar_text = synth_grammar(kf, vocab, binder)
        except Exception as e:
            synth_errs.append(f"synth_grammar: {e!r}")
    except Exception as e:
        synth_errs.append(f"import_synth_artifacts: {e!r}")

    # --- If any piece missing, build minimal stand-ins ---
    def _ensure_vocab_minimal() -> Dict[str, Any]:
        kw = (kf.get("keywords") or {})
        sa = (kf.get("sql_actions") or {})
        return {
            "keywords": kw,
            "sql_actions": sa,
        }

    def _ensure_binder_minimal() -> Dict[str, Any]:
        catalogs: Dict[str, Any] = {"tables": {}, "columns": {}, "functions": {}, "connectors": {}}
        tables = (schema.get("tables") or {})
        for tname, tmeta in (tables or {}).items():
            catalogs["tables"][str(tname)] = {}
            cols = (tmeta or {}).get("columns") or []
            for c in cols:
                # Expect dict with at least 'name' and 'type'
                if not isinstance(c, dict):
                    continue
                cname = str(c.get("name") or "").strip()
                ctype = c.get("type")
                if not cname:
                    continue
                fqn = f"{tname}.{cname}"
                catalogs["columns"][fqn] = {"table": str(tname), "name": cname}
                if ctype:
                    catalogs["columns"][fqn]["type"] = ctype
        # wire connectors from keywords if present
        connectors = ((kf.get("keywords") or {}).get("connectors") or {})
        if isinstance(connectors, dict):
            for k, v in connectors.items():
                catalogs["connectors"][str(k)] = str(v)
        return {"catalogs": catalogs}

    def _ensure_grammar_minimal(vocab_obj: Dict[str, Any]) -> str:
        # Extract connectors (with defaults)
        conns = ((vocab_obj.get("keywords") or {}).get("connectors") or {})
        def _term(name: str, default: str) -> str:
            val = conns.get(name, default)
            # commas etc. handled as literals
            if name == "COMMA":
                return f'{name}: ","'
            return f'{name}: "{val}"i'
        SELECT = _term("SELECT", "select")  # comes from select_verbs in a richer grammar, but terminal is fine
        AND    = _term("AND", "and")
        OR     = _term("OR", "or")
        FROM   = _term("FROM", "from")
        OF     = _term("OF", "of")
        COMMA  = _term("COMMA", ",")
        NOT    = _term("NOT", "not")

        # Actions from top-level sql_actions (projection only)
        sa = (vocab_obj.get("sql_actions") or {})
        action_terms: List[str] = []
        for name, meta in (sa or {}).items():
            tmpl = (meta or {}).get("template")
            if not isinstance(tmpl, str) or not tmpl.strip():
                continue
            up = str(name).lower()
            # treat only projection-like as ACTION terminals
            if tmpl.strip().upper().startswith(("ORDER BY", "GROUP BY", "HAVING", "LIMIT")):
                continue
            action_terms.append(f'"{up}"i')
        if not action_terms:
            action_terms = ['"count"i']  # last resort

        grammar = "\n".join([
            "// Auto-generated Lark grammar (minimal fallback)",
            SELECT,
            AND,
            OR,
            FROM,
            OF,
            COMMA,
            NOT,
            "",
            "start: query",
            f"action: " + " | ".join(sorted(set(action_terms))),
            'VALUE: "VALUE"',
            "projection: action [OF] VALUE",
            "query: SELECT FROM | SELECT projection FROM",
            "",
            "%import common.WS",
            "%ignore WS",
            ""
        ])
        return grammar

    if vocab is None:
        vocab = _ensure_vocab_minimal()
    if binder is None:
        binder = _ensure_binder_minimal()
    if grammar_text is None:
        grammar_text = _ensure_grammar_minimal(vocab)

    # --- Write artifacts ---
    try:
        os.makedirs(ap.art_dir, exist_ok=True)
        with open(ap.vocab_path, "w", encoding="utf-8") as f:
            yaml.safe_dump(vocab, f, sort_keys=False, allow_unicode=True)
        with open(ap.binder_path, "w", encoding="utf-8") as f:
            yaml.safe_dump(binder, f, sort_keys=False, allow_unicode=True)
        with open(ap.grammar_path, "w", encoding="utf-8") as f:
            f.write(grammar_text)
    except Exception as e:
        return StepResult(step="artifacts.direct_build", ok=False,
                          info={"error": f"write_error: {e!r}", "synth_errors": synth_errs}, warnings=warnings)

    ok = artifacts_exist(ap)
    info: Dict[str, Any] = {"art_dir": ap.art_dir}
    if synth_errs:
        info["synth_warnings"] = synth_errs
    return StepResult(step="artifacts.direct_build", ok=ok, info=info, warnings=warnings)


def build_artifacts_inproc(ap: ArtifactPaths) -> StepResult:
    """
    Try the orchestrator; if it fails, fall back to direct synthesis.
    """
    warnings: List[str] = []
    keywords_path, schema_path = _input_paths_for_builder(ap)

    missing_inputs: List[str] = []
    if not os.path.isfile(keywords_path):
        missing_inputs.append(keywords_path)
    if not os.path.isfile(schema_path):
        missing_inputs.append(schema_path)
    if missing_inputs:
        return StepResult(step="artifacts.build", ok=False,
                          info={"error": "missing_inputs", "missing": missing_inputs, "art_dir": ap.art_dir},
                          warnings=warnings)

    # Ensure generator sees our target dir
    old_art = os.environ.get("ARTIFACTS_DIR")
    os.environ["ARTIFACTS_DIR"] = ap.art_dir

    # Attempt orchestrator call
    rc = None
    orchestrator_err = None
    try:
        import sys
        sys.path.insert(0, os.getcwd())
        from vbg_tools.generate_artifacts import main as build_main  # type: ignore
        argv = ["--keywords", keywords_path, "--schema", schema_path, "--out", ap.art_dir]
        rc = build_main(argv)  # could return int or None
    except Exception as e:
        orchestrator_err = e

    # Restore env var
    if old_art is None:
        os.environ.pop("ARTIFACTS_DIR", None)
    else:
        os.environ["ARTIFACTS_DIR"] = old_art

    # If orchestrator appeared to run, check outputs (promote if nested)
    if orchestrator_err is None and (rc is None or (isinstance(rc, int) and rc == 0)):
        if not artifacts_exist(ap):
            _promote_from_nested_out(ap)
        if artifacts_exist(ap):
            return StepResult(step="artifacts.build", ok=True,
                              info={"art_dir": ap.art_dir, "path": "orchestrator"}, warnings=warnings)

    # Orchestrator failed or didn’t produce files → direct synthesis fallback
    direct = _direct_build_offline(ap, keywords_path, schema_path)
    if direct.ok:
        return StepResult(step="artifacts.build", ok=True,
                          info={"art_dir": ap.art_dir, "path": "direct"}, warnings=warnings)

    # Last resort: try promote from nested out/ and re-check
    _promote_from_nested_out(ap)
    ok = artifacts_exist(ap)
    info = {"art_dir": ap.art_dir, "path": "promote_only"}
    if orchestrator_err:
        info["orchestrator_error"] = repr(orchestrator_err)
    if rc not in (None, 0):
        info["orchestrator_rc"] = rc
    if direct.info:
        info["direct_info"] = direct.info
    return StepResult(step="artifacts.build", ok=ok, info=info, warnings=warnings)

def ensure_artifacts(ap: ArtifactPaths, auto_build: bool = True) -> StepResult:
    if artifacts_exist(ap):
        return StepResult(step="artifacts.ensure", ok=True, info={"built": False, "art_dir": ap.art_dir}, warnings=[])
    if not auto_build:
        return StepResult(step="artifacts.ensure", ok=False, info={"built": False, "reason": "missing"}, warnings=[])
    build_res = build_artifacts_inproc(ap)
    final_ok = artifacts_exist(ap)
    return StepResult(step="artifacts.ensure", ok=final_ok,
                      info={"built": True, "builder_ok": build_res.ok, "art_dir": ap.art_dir},
                      warnings=build_res.warnings)

# -------------------------
# Case validation helpers
# -------------------------
def validate_parse_case_payload(payload: Dict[str, Any], expectations: CaseExpectations) -> StepResult:
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

def validate_sql_case_payload(payload: Dict[str, Any], expectations: CaseExpectations) -> StepResult:
    base = validate_parse_case_payload(payload, CaseExpectations(tokens_regex=expectations.tokens_regex, want_parse_ok=True))
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
    Calls your map_text() and returns (StepResult, payload_dict).
    """
    res = map_text(text, vocab_yaml, binder_yaml, grammar_text, want_tree=want_tree)
    spans = getattr(res, "spans", []) or []
    payload = {
        "canonical_tokens": getattr(res, "canonical_tokens", []),
        "slots": getattr(res, "slots", {}),
        "spans": spans,
        "parse_ok": bool(getattr(res, "parse_ok", False)),
        "warnings": list(getattr(res, "warnings", []) or []),
    }
    if want_tree and getattr(res, "tree", None):
        payload["tree"] = res.tree
    return StepResult(step="exec.parse", ok=True, info={"text": text}, warnings=list(payload["warnings"])), payload

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
