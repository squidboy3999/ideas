# vbg_tools/generate_artifacts.py
from __future__ import annotations

import argparse
from pathlib import Path
import sys

from .artifact_helpers import (
    load_yaml_file,
    write_yaml_file,
    write_text_file,
)
from .synth_artifacts import (
    build_vocabulary,
    build_binder,
    build_grammar,
)

from .input_structure_validation import (
    validate_keywords_and_functions,
    validate_schema
)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        prog="vbg_generate_offline",
        description="Generate vocabulary/binder/grammar artifacts from YAML inputs (no DB).",
    )
    ap.add_argument("--keywords", "-k", required=True, help="Path to keywords_and_functions.yaml")
    ap.add_argument("--schema", "-s", required=True, help="Path to schema.yaml")
    ap.add_argument("--out", "-o", required=True, help="Output directory for artifacts")
    ap.add_argument(
        "--quiet",
        action="store_true",
        help="Reduce console output",
    )
    args = ap.parse_args(argv)

    out_dir = Path(args.out).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    if not args.quiet:
        print(f"[generate] loading YAMLs…")

    kf = load_yaml_file(args.keywords)
    schema = load_yaml_file(args.schema)

    validate_keywords_and_functions(kf)
    validate_schema(schema)

    if not args.quiet:
        print(f"[generate] synthesizing artifacts…")

    vocab = build_vocabulary(kf)
    binder = build_binder(schema, vocab)
    grammar = build_grammar(vocab, binder)

    # Write artifacts
    vocab_path = out_dir / "graph_vocabulary.yaml"
    binder_path = out_dir / "graph_binder.yaml"
    grammar_path = out_dir / "graph_grammar.lark"

    write_yaml_file(vocab_path, vocab)
    write_yaml_file(binder_path, binder)
    write_text_file(grammar_path, grammar)

    if not args.quiet:
        print(f"[generate] wrote:\n  - {vocab_path}\n  - {binder_path}\n  - {grammar_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
