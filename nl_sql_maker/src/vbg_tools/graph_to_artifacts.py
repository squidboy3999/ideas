#!/usr/bin/env python3
# vbg_tools/graph_to_artifacts.py
from __future__ import annotations
import os, sys, yaml
from pathlib import Path
from typing import Any, Dict
from lark import Lark, UnexpectedInput
import re
import sys
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
            # Show context lines that reference the symbol
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

    # 2) Minimal smoke-parse: canonical stream the runtime produces for the simplest case
    try:
        parser.parse("SELECT FROM")
    except UnexpectedInput as e:
        print("\n[GRAMMAR VALIDATION WARNING] Grammar compiled, but failed to parse minimal 'SELECT FROM'.", file=sys.stderr)
        print(str(e), file=sys.stderr)
        print("  -> Ensure 'query' has an alternative like:  | SELECT FROM", file=sys.stderr)
    except Exception as e:
        print("\n[GRAMMAR VALIDATION WARNING] Grammar compiled, but parse raised:", type(e).__name__, file=sys.stderr)
        print(str(e), file=sys.stderr)


def _validate_vocab_vs_grammar(vocab: Dict[str, Any], grammar_text: str) -> None:
    """
    Sanity-check that connectors defined in vocab are present as terminals in grammar.
    Produces no output when aligned; prints a warning listing mismatches otherwise.
    """
    # Extract terminals defined in grammar
    term_names = set()
    for line in grammar_text.splitlines():
        line = line.strip()
        # Match TERMINAL_NAME: "literal"i   (case-insensitive flag optional)
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
            
        _validate_grammar(grammar)
        _validate_vocab_vs_grammar(vocab, grammar)
        # Write files
        with open(OUT_VOCAB, "w", encoding="utf-8") as f:
            yaml.safe_dump(vocab, f, sort_keys=False)
        with open(OUT_BINDER, "w", encoding="utf-8") as f:
            yaml.safe_dump(binder, f, sort_keys=False)
        OUT_GRAMMAR.write_text(grammar, encoding="utf-8")

        print(f"Wrote: {OUT_VOCAB}")
        print(f"Wrote: {OUT_BINDER}")
        print(f"Wrote: {OUT_GRAMMAR}")
        return 0
    finally:
        driver.close()

if __name__ == "__main__":
    sys.exit(main())
