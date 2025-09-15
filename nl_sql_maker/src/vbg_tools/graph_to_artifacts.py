#!/usr/bin/env python3
# vbg_tools/graph_to_artifacts.py
from __future__ import annotations
import os, sys, yaml, re
from pathlib import Path
from typing import Any, Dict, List
from lark import Lark, UnexpectedInput

from .cypher_helper import (
    get_driver, with_session,
    wipe_graph, apply_schema,
    ingest_schema, ingest_keywords, ingest_rules_from_templates,
    synth_vocabulary, synth_binder, synth_grammar,
)

# ----------- env / paths -----------
ART_DIR = Path(os.environ.get("ARTIFACTS_DIR", "out"))
ORIG_KEYWORDS = Path(os.environ.get("ORIGINALS_KEYWORDS", "keywords_and_functions.yaml"))
ORIG_SCHEMA   = Path(os.environ.get("ORIGINALS_SCHEMA", "schema.yaml"))

NEO4J_URI  = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.environ.get("NEO4J_USER", "neo4j")
NEO4J_PASS = os.environ.get("NEO4J_PASSWORD", "test")
NEO4J_DB   = os.environ.get("NEO4J_DATABASE", None)  # optional

OUT_VOCAB   = ART_DIR / "graph_vocabulary.yaml"
OUT_BINDER  = ART_DIR / "graph_binder.yaml"
OUT_GRAMMAR = ART_DIR / "graph_grammar.lark"

# ------------- grammar helpers (augmentation layer) -------------
_TERMINAL_LINE_RE = re.compile(r'^([A-Z][A-Z0-9_]*)\s*:\s*".*"', re.M)

def _has_terminal(grammar_text: str, name: str) -> bool:
    pat = re.compile(rf'^\s*{re.escape(name)}\s*:', re.M)
    return bool(pat.search(grammar_text))

def _insert_terminal(grammar_text: str, line: str) -> str:
    """
    Insert a terminal definition line near the top, before the first %import line
    or before the first token (NAME:/...) block if %import is absent.
    """
    lines = grammar_text.splitlines()
    insert_at = None
    for i, ln in enumerate(lines):
        if ln.strip().startswith("%import "):
            insert_at = i
            break
    if insert_at is None:
        # try before first non-comment, non-empty rule line
        for i, ln in enumerate(lines):
            s = ln.strip()
            if s and not s.startswith("//"):
                insert_at = i
                break
    insert_at = insert_at if insert_at is not None else 0
    lines.insert(insert_at, line)
    return "\n".join(lines)

def _ensure_ws_directives(grammar_text: str) -> str:
    out = grammar_text
    if "%import common.WS" not in out:
        out += ("\n%import common.WS")
    if "%ignore WS" not in out:
        out += ("\n%ignore WS\n")
    return out

def _ensure_value_token(grammar_text: str) -> str:
    if re.search(r'^\s*VALUE\s*:', grammar_text, re.M):
        return grammar_text
    # add a sensible default (matches your existing generator)
    return grammar_text.rstrip() + '\n\nVALUE: /[^,\\)\\(]+/\n'

def _extract_arity1_functions_from_binder(binder: Dict[str, Any]) -> List[str]:
    fn = []
    try:
        functions = ((binder.get("catalogs") or {}).get("functions") or {})
        for name, meta in functions.items():
            if isinstance(meta, dict) and int(meta.get("arity") or 0) == 1:
                fn.append(str(name).lower())
    except Exception:
        pass
    # keep stable order
    return sorted(set(fn))

def _augment_expression_rule(grammar_text: str, arity1_fns: List[str]) -> str:
    if not arity1_fns:
        return grammar_text

    # build alts: "min" ["OF"] VALUE | "max" ["OF"] VALUE | ...
    alts = [f'"{fn}" ["OF"] VALUE' for fn in arity1_fns]

    lines = grammar_text.splitlines()
    # find existing expression: rule
    expr_idx = None
    for i, ln in enumerate(lines):
        if ln.strip().startswith("expression:"):
            expr_idx = i
            break

    if expr_idx is None:
        # append a new rule
        block = "\n// generic arity-1 SQL action expressions\nexpression: " + " | ".join(alts) + "\n"
        return grammar_text.rstrip() + "\n" + block

    # collect existing RHS (current line + subsequent lines starting with '|')
    existing_rhs: List[str] = []
    head = lines[expr_idx].split("expression:", 1)[1].strip()
    if head:
        existing_rhs.append(head)
    j = expr_idx + 1
    while j < len(lines) and lines[j].lstrip().startswith("|"):
        existing_rhs.append(lines[j].lstrip()[1:].strip())
        j += 1

    # merge
    existing_set = [s for s in (p.strip() for p in " | ".join(existing_rhs).split("|")) if s]
    merged = existing_set[:]
    for alt in alts:
        if alt not in merged:
            merged.append(alt)

    # rebuild block
    new_line = "expression: " + " | ".join(merged)
    new_lines = lines[:expr_idx] + [new_line] + lines[j:]
    return "\n".join(new_lines)

def _augment_query_rule(grammar_text: str) -> str:
    """
    Ensure query has alternative:  | SELECT expression FROM constraints clauses
    (we keep SELECT FROM as a minimal parse).
    """
    lines = grammar_text.splitlines()
    q_idx = None
    for i, ln in enumerate(lines):
        if ln.strip().startswith("query:"):
            q_idx = i
            break
    if q_idx is None:
        # No query rule? Add a minimal one
        lines.append("start: query")
        lines.append("query: SELECT expression FROM constraints clauses | SELECT FROM")
        return "\n".join(lines)

    rhs = lines[q_idx].split("query:", 1)[1]
    target = "SELECT expression FROM constraints clauses"
    if target in rhs:
        return grammar_text
    # append alternative cleanly
    rhs_stripped = rhs.rstrip()
    if rhs_stripped and not rhs_stripped.endswith(" "):
        rhs_stripped += " "
    lines[q_idx] = "query:" + rhs_stripped + f"| {target}"
    return "\n".join(lines)

def _ensure_of_terminal(grammar_text: str) -> str:
    if _has_terminal(grammar_text, "OF"):
        return grammar_text
    return _insert_terminal(grammar_text, 'OF: "of"i')

def _ensure_not_terminal(grammar_text: str) -> str:
    if _has_terminal(grammar_text, "NOT"):
        return grammar_text
    return _insert_terminal(grammar_text, 'NOT: "not"i')

def _augment_constraints_rule(grammar_text: str) -> str:
    """
    If there is a 'predicate:' rule, replace/ensure constraints allow:
       pred_atom: (NOT)? predicate
       constraints: (pred_atom ((AND|OR) pred_atom)*)?
    """
    lines = grammar_text.splitlines()
    has_predicate = any(ln.strip().startswith("predicate:") for ln in lines)
    if not has_predicate:
        return grammar_text

    # Remove existing constraints/pred_atom definitions (we’ll add our versions)
    out: List[str] = []
    for ln in lines:
        s = ln.strip()
        if s.startswith("constraints:") or s.startswith("pred_atom:"):
            continue
        out.append(ln)

    out.append("\npred_atom: (NOT)? predicate")
    out.append("\nconstraints: (pred_atom ((AND|OR) pred_atom)*)?\n")
    return "\n".join(out)

def _strip_value_leak_between_table_and_constraints(grammar_text: str) -> str:
    """
    Some synth_grammar rewrites may inject a stray VALUE token right after TABLE,
    e.g.,  query: SELECT COLUMNS FROM TABLE VALUE constraints clauses | ...
    That 'VALUE' greedily eats the predicate tail. Remove it when it's exactly
    between TABLE and (constraints|clauses) in any query line.
    """
    def _fix_line(ln: str) -> str:
        if not ln.strip().startswith("query:"):
            return ln
        # Use capture + lookahead (no lookbehind) so Python re is happy.
        # Replace "FROM TABLE VALUE <constraints|clauses>" -> "FROM TABLE <constraints|clauses>"
        return re.sub(
            r'(\bFROM\s+TABLE)\s+VALUE(?=\s+(?:constraints|clauses)\b)',
            r'\1',
            ln
        )
    return "\n".join(_fix_line(ln) for ln in grammar_text.splitlines())

def _augment_grammar_with_arity1_functions(grammar_text: str, binder: Dict[str, Any]) -> str:
    """
    Adds / ensures:
      - OF & NOT terminals (if missing)
      - VALUE token (if missing)
      - WS directives (if missing)
      - expression: "min" ["OF"] VALUE | ...
      - query: ... | SELECT expression FROM constraints clauses
      - constraints: allow NOT and OR
      - strip spurious VALUE that leaks between TABLE and constraints/clauses
    """
    out = grammar_text
    out = _ensure_of_terminal(out)
    out = _ensure_not_terminal(out)
    out = _ensure_value_token(out)
    out = _ensure_ws_directives(out)
    # Clean up any VALUE leak in the query rule produced upstream
    out = _strip_value_leak_between_table_and_constraints(out)
    # Add expression alts & expression-path query form
    arity1 = _extract_arity1_functions_from_binder(binder)
    if arity1:
        out = _augment_expression_rule(out, arity1)
        out = _augment_query_rule(out)
    # Strengthen constraints to support NOT/OR chaining
    out = _augment_constraints_rule(out)
    return out

# ----------- validation helpers -----------
def _validate_grammar(grammar_text: str) -> None:
    """
    Compile-time and smoke-parse checks for the generated grammar.
    Produces no output on success. On failure, prints actionable diagnostics.
    """
    # 1) Compile the grammar
    try:
        parser = Lark(grammar_text, parser="earley", lexer="dynamic_complete")
    except Exception as e:
        msg = str(e)
        print("\n[GRAMMAR VALIDATION ERROR] Grammar failed to compile.", file=sys.stderr)
        print(msg, file=sys.stderr)

        # Try to extract the missing symbol and show context from expression/predicate
        m = re.search(r"[Rr]ule '([^']+)' used but not defined", msg)
        missing = m.group(1) if m else None
        if missing:
            print(f"  -> Missing symbol: {missing}", file=sys.stderr)
            print("     (UPPERCASE usually means a terminal; lowercase a rule.)", file=sys.stderr)
            lines = grammar_text.splitlines()
            for i, line in enumerate(lines):
                if re.search(rf"\b{re.escape(missing)}\b", line):
                    start = max(0, i-2)
                    end = min(len(lines), i+3)
                    print("\n--- Nearby grammar lines ---", file=sys.stderr)
                    for j in range(start, end):
                        prefix = ">>" if j == i else "  "
                        print(f"{prefix} {lines[j]}", file=sys.stderr)
                    break
        print("", file=sys.stderr)
        return  # Don’t attempt smoke-parse if compile failed

    # 2) Minimal smoke-parse(s)
    try:
        parser.parse("SELECT FROM")
    except UnexpectedInput as e:
        print("\n[GRAMMAR VALIDATION WARNING] Grammar compiled, but failed to parse minimal 'SELECT FROM'.", file=sys.stderr)
        print(str(e), file=sys.stderr)
        print("  -> Ensure 'query' has an alternative like:  | SELECT FROM", file=sys.stderr)
    except Exception as e:
        print("\n[GRAMMAR VALIDATION WARNING] Grammar compiled, but parse raised:", type(e).__name__, file=sys.stderr)
        print(str(e), file=sys.stderr)

    # Optional smoke-parse for expression form if we seem to have one
    if "SELECT expression FROM" in grammar_text and re.search(r'^expression\s*:', grammar_text, re.M):
        try:
            # use a generic function literal that should exist after augmentation if any arity-1 fn exists
            parser.parse('SELECT min OF VALUE FROM')
        except Exception:
            # don't fail generation; just warn
            print("\n[GRAMMAR VALIDATION WARNING] 'SELECT min OF VALUE FROM' did not parse; "
                  "ensure at least one arity-1 function is present or the function literal matches.", file=sys.stderr)

def _validate_vocab_vs_grammar(vocab: Dict[str, Any], grammar_text: str) -> None:
    """
    Sanity-check that connectors defined in vocab are present as terminals in grammar.
    Produces no output when aligned; prints a warning listing mismatches otherwise.
    """
    term_names = set()
    for line in grammar_text.splitlines():
        line = line.strip()
        m = re.match(r"^([A-Z][A-Z0-9_]*)\s*:\s*\".*?\"", line)
        if m:
            term_names.add(m.group(1))

    kw = (vocab.get("keywords") or {})
    conns = kw.get("connectors") or {}
    if not isinstance(conns, dict):
        return

    missing = []
    for can_name, surface in conns.items():
        if str(can_name).upper() not in term_names:
            missing.append((can_name, surface))

    if missing:
        print("\n[VOCAB/GRAMMAR VALIDATION WARNING] Some connectors appear in vocab but not as terminals:", file=sys.stderr)
        for name, surf in missing:
            print(f"  - {name} -> expected terminal like: {str(name).upper()}: \"{surf}\"i", file=sys.stderr)
        print("", file=sys.stderr)

# ----------- small utils -----------
def safe_load_yaml(p: Path) -> Dict[str, Any]:
    if not p.exists():
        return {}
    with open(p, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}

# ----------- main -----------
def main() -> int:
    ART_DIR.mkdir(parents=True, exist_ok=True)

    schema_yaml = safe_load_yaml(ORIG_SCHEMA)
    kw_yaml     = safe_load_yaml(ORIG_KEYWORDS)

    driver = get_driver(NEO4J_URI, NEO4J_USER, NEO4J_PASS)
    try:
        with with_session(driver, NEO4J_DB) as sess:
            # Build/refresh graph
            wipe_graph(sess)
            apply_schema(sess)
            ingest_schema(sess, schema_yaml)
            ingest_keywords(sess, kw_yaml)
            ingest_rules_from_templates(sess, kw_yaml)

            # Synthesize artifacts FROM graph
            vocab   = synth_vocabulary(sess)
            binder  = synth_binder(sess)
            grammar = synth_grammar(sess)

        # ---- Augment grammar for arity-1 functions & constraints on expression path ----
        grammar_aug = _augment_grammar_with_arity1_functions(grammar, binder)

        # Validate & write
        _validate_grammar(grammar_aug)
        _validate_vocab_vs_grammar(vocab, grammar_aug)

        with open(OUT_VOCAB, "w", encoding="utf-8") as f:
            yaml.safe_dump(vocab, f, sort_keys=False)
        with open(OUT_BINDER, "w", encoding="utf-8") as f:
            yaml.safe_dump(binder, f, sort_keys=False)
        OUT_GRAMMAR.write_text(grammar_aug, encoding="utf-8")

        print(f"Wrote: {OUT_VOCAB}")
        print(f"Wrote: {OUT_BINDER}")
        print(f"Wrote: {OUT_GRAMMAR}")
        return 0
    finally:
        driver.close()

if __name__ == "__main__":
    sys.exit(main())
