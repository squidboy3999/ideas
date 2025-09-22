from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, Any, List, Tuple
from pathlib import Path
import yaml, argparse, sys, json

from .surfaces_spec_builder import enumerate_specs, SQLSpec, column_slot_types

# =========================
# Config & result bundle
# =========================

@dataclass
class PipelineConfig:
    max_specs: int = 200
    per_spec_max: int = 6
    per_spec_unconstrained_max: int = 2
    global_unconstrained_budget: int = 10
    order: str = "predicates_first"   # or "bases_first"

@dataclass
class SurfacesBundle:
    gold: List[Dict[str, Any]] = field(default_factory=list)
    multipath: List[Dict[str, Any]] = field(default_factory=list)
    invalid: List[Dict[str, Any]] = field(default_factory=list)
    report: Dict[str, Any] = field(default_factory=dict)

# =========================
# Diagnostics
# =========================

def _probe_actions(vocab: Dict[str, Any]) -> Dict[str, Any]:
    top = vocab.get("sql_actions")
    legacy = ((vocab.get("keywords") or {}).get("sql_actions") or {})
    actions = top if isinstance(top, dict) and top else (legacy if isinstance(legacy, dict) else {})
    proj = {k:v for k,v in actions.items() if isinstance(v, dict) and (v.get("placement") or "").lower()=="projection"}
    missing_app = [k for k,v in proj.items() if not ((v.get("applicable_types") or {}).get("column"))]
    return {
        "found_top_level": bool(top),
        "found_legacy_keywords": bool(legacy),
        "total_actions": len(actions),
        "projection_actions": list(proj.keys()),
        "projection_count": len(proj),
        "projection_missing_applicable": missing_app,
    }

def _probe_columns(binder: Dict[str, Any], sample: int = 5) -> Dict[str, Any]:
    cats = binder.get("catalogs") or {}
    cols = cats.get("columns") or {}
    keys = list(cols.keys())
    preview = []
    for k in keys[:sample]:
        meta = cols.get(k) or {}
        preview.append({
            "fq": k,
            "table": meta.get("table"),
            "name": meta.get("name"),
            "type_raw": meta.get("type"),
            "slot_types_raw": meta.get("slot_types"),
            "slot_types_norm": list(column_slot_types(binder, k)),
        })
    return {"count": len(keys), "sample": preview}

def diagnostics_for_artifacts(vocab: Dict[str, Any], binder: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "actions": _probe_actions(vocab),
        "columns": _probe_columns(binder),
    }

# =========================
# Helpers
# =========================

def _is_unconstrained(nl: str) -> bool:
    s = nl.lower()
    return not ((" between " in s) or (" > " in s) or (" < " in s) or ("greater than" in s) or ("less than" in s))

def _resolve_paths(out_dir: Path) -> Dict[str, Path]:
    return {
        "gold": out_dir / "gold_surfaces.yml",
        "multipath": out_dir / "multipath_surfaces.yml",
        "invalid": out_dir / "invalid_surfaces.yml",
        "report": out_dir / "surfaces_report.yml",
    }

# =========================
# Core pipeline
# =========================

def generate_surfaces(
    vocab: Dict[str, Any],
    binder: Dict[str, Any],
    grammar_text: str,
    *,
    config: PipelineConfig,
    debug: bool=False
) -> SurfacesBundle:
    # Diagnostics first, so "specs_total == 0" is explainable
    diag = diagnostics_for_artifacts(vocab, binder)
    if debug:
        print("[surfaces.debug] actions:", json.dumps(diag["actions"], indent=2))
        print("[surfaces.debug] columns:", json.dumps(diag["columns"], indent=2))

    # Specs
    specs = enumerate_specs(binder, vocab, max_specs=config.max_specs)

    # Phrase assembly
    from .surfaces_phrase_factory import assemble_surfaces
    global_uncon_left = config.global_unconstrained_budget
    candidates: List[Tuple[SQLSpec, str]] = []

    for spec in specs:
        per = assemble_surfaces(
            spec, vocab, binder,
            per_spec_max=config.per_spec_max,
            per_spec_unconstrained_max=config.per_spec_unconstrained_max,
            order=config.order,
            require_min_predicates=True,
        )
        for nl in per:
            if _is_unconstrained(nl):
                if global_uncon_left <= 0:
                    continue
                global_uncon_left -= 1
            candidates.append((spec, nl))

    # Resolve & classify with runtime
    from vbg_tools.graph_runtime import map_text
    try:
        from vbg_tools.sql_helpers import build_sql
    except Exception:
        build_sql = None

    gold: List[Dict[str, Any]] = []
    multipath: List[Dict[str, Any]] = []
    invalid: List[Dict[str, Any]] = []

    for spec, nl in candidates:
        rr = map_text(nl, vocab, binder, grammar_text, want_tree=False)
        if not getattr(rr, "parse_ok", False):
            invalid.append({"natural_language": nl, "original_sql": spec.expression_sql, "sql_expressions": []})
            continue

        sqls: List[str] = []
        try:
            if build_sql and rr.slots:
                sql = build_sql(rr.slots)
                if sql:
                    sqls = [sql]
        except Exception:
            sqls = []

        if not sqls:
            sqls = [spec.expression_sql]

        if len(sqls) == 1:
            gold.append({"natural_language": nl, "sql_expression": sqls[0]})
        else:
            multipath.append({"natural_language": nl, "original_sql": spec.expression_sql, "sql_expressions": sqls})

    bundle = SurfacesBundle(gold=gold, multipath=multipath, invalid=invalid)
    bundle.report = {
        "specs_total": len(specs),
        "candidates_total": len(candidates),
        "validated_total": len(gold) + len(multipath) + len(invalid),
        "gold": len(gold),
        "multipath": len(multipath),
        "invalid": len(invalid),
        "unconstrained_budget_remaining": global_uncon_left,
        "diagnostics": diag,
    }

    if debug and not specs:
        print("[surfaces.debug] No specs generated.")
        if not diag["actions"]["projection_actions"]:
            print("[surfaces.debug]   Reason: No projection actions discovered. Ensure vocabulary has top-level 'sql_actions' (or legacy 'keywords.sql_actions') with 'placement: projection' and 'applicable_types'.")
        if diag["columns"]["count"] == 0:
            print("[surfaces.debug]   Reason: No columns in binder catalogs.columns.")
        missing = diag["actions"]["projection_missing_applicable"]
        if missing:
            print(f"[surfaces.debug]   Warning: projection actions missing applicable_types: {missing}")

    return bundle


def write_surfaces(bundle: SurfacesBundle, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    paths = _resolve_paths(out_dir)
    with paths["gold"].open("w", encoding="utf-8") as f:
        yaml.safe_dump(bundle.gold, f, sort_keys=False)
    with paths["multipath"].open("w", encoding="utf-8") as f:
        yaml.safe_dump(bundle.multipath, f, sort_keys=False)
    with paths["invalid"].open("w", encoding="utf-8") as f:
        yaml.safe_dump(bundle.invalid, f, sort_keys=False)
    with paths["report"].open("w", encoding="utf-8") as f:
        yaml.safe_dump(bundle.report, f, sort_keys=False)

# =========================
# CLI
# =========================

def _load_artifacts(vp: Path, bp: Path, gp: Path):
    return (
        yaml.safe_load(vp.read_text(encoding="utf-8")),
        yaml.safe_load(bp.read_text(encoding="utf-8")),
        gp.read_text(encoding="utf-8"),
    )

def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Generate NL surfaces and optionally cli_test.sh")
    # Your requested defaults:
    parser.add_argument("--art-dir", default="out")
    parser.add_argument("--vocab", dest="vocab_path", default=None)
    parser.add_argument("--binder", dest="binder_path", default=None)
    parser.add_argument("--grammar", dest="grammar_path", default=None)
    parser.add_argument("--max-specs", type=int, default=200)
    parser.add_argument("--per-spec-max", type=int, default=6)
    parser.add_argument("--per-spec-unconstrained-max", type=int, default=2)
    parser.add_argument("--global-unconstrained", type=int, default=10)
    parser.add_argument("--order", choices=["predicates_first", "bases_first"], default="predicates_first")
    parser.add_argument("--emit-cli-test", action="store_true", default=True)
    parser.add_argument("--debug", action="store_true", default=True)
    args = parser.parse_args(argv)

    out_dir = Path(args.art_dir)
    vp = Path(args.vocab_path) if args.vocab_path else out_dir / "graph_vocabulary.yaml"
    bp = Path(args.binder_path) if args.binder_path else out_dir / "graph_binder.yaml"
    gp = Path(args.grammar_path) if args.grammar_path else out_dir / "graph_grammar.lark"

    # Print an example command to tweak defaults easily
    example = (
        "vbg_surfaces "
        f"--art-dir {out_dir} "
        f"--max-specs {args.max_specs} "
        f"--per-spec-max {args.per_spec_max} "
        f"--per-spec-unconstrained-max {args.per_spec_unconstrained_max} "
        f"--global-unconstrained {args.global_unconstrained} "
        f"--order {args.order} "
        f"{'--emit-cli-test ' if args.emit_cli_test else ''}"
        f"{'--debug' if args.debug else ''}"
    )
    print("[surfaces] example args:", example)
    print(f"[surfaces] loading artifacts from: {out_dir}")

    try:
        vocab, binder, grammar = _load_artifacts(vp, bp, gp)
    except Exception as e:
        print(f"[surfaces] error reading artifacts: {e}", file=sys.stderr)
        return 2

    cfg = PipelineConfig(
        max_specs=args.max_specs,
        per_spec_max=args.per_spec_max,
        per_spec_unconstrained_max=args.per_spec_unconstrained_max,
        global_unconstrained_budget=args.global_unconstrained,
        order=args.order,
    )

    try:
        bundle = generate_surfaces(vocab, binder, grammar, config=cfg, debug=args.debug)
        write_surfaces(bundle, out_dir)
    except Exception as e:
        print(f"[surfaces] generation failed: {e}", file=sys.stderr)
        return 3

    if args.emit_cli_test:
        try:
            from vbg_tools.create_cli_test import generate_cli_test
            p = generate_cli_test(art_dir=out_dir)
            print(f"[surfaces] cli test script: {p}")
        except Exception as e:
            print(f"[surfaces] warning: failed to generate cli_test.sh: {e}", file=sys.stderr)

    rpt = bundle.report or {}
    print(f"[surfaces] wrote files to: {out_dir}")
    print(f"[surfaces] counts: gold={rpt.get('gold',0)}, multipath={rpt.get('multipath',0)}, invalid={rpt.get('invalid',0)}")
    print(
        f"[surfaces] specs_total={rpt.get('specs_total',0)}, "
        f"candidates_total={rpt.get('candidates_total',0)}, "
        f"validated_total={rpt.get('validated_total',0)}, "
        f"unconstrained_budget_remaining={rpt.get('unconstrained_budget_remaining',0)}"
    )
    if args.debug:
        print("[surfaces.debug] report:", json.dumps(rpt, indent=2))
    return 0

if __name__ == "__main__":
    sys.exit(main())
