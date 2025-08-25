# src/vbg_tools/phrase_explorer.py
from __future__ import annotations

import argparse
import random
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import yaml

from vbg_runtime.artifacts_loader import load_artifacts, validate_artifacts
from vbg_shared.schema_utils import (
    table_to_columns,
    is_geometry_col,
    is_numeric_col,
    get_global_unique_basenames,
    select_friendly_functions,
)

# -----------------------------
# Planning dims
# -----------------------------

LIST_STYLES = ("comma", "and", "oxford")
CONTENT_MIXES_BY_SIZE = {
    1: [["col"], ["fn"]],
    2: [["col", "col"], ["col", "fn"]],
    3: [["col", "col", "col"], ["fn", "col", "col"], ["fn", "fn", "col"]],
}

NUMERIC_FN_HINTS = {"sum", "avg", "min", "max", "count"}  # heuristic; binder metadata is the source of truth


def _pick_fn_args_for_table(
    table: str,
    tcols: List[str],
    fn: str,
    arg_count: int,
    graph: Dict[str, Any],
) -> Optional[List[str]]:
    needs_geom = fn.lower().startswith("st_")
    needs_numeric = (fn.lower() in NUMERIC_FN_HINTS)

    pool = list(tcols)
    if needs_geom:
        pool = [c for c in pool if is_geometry_col(graph, c)]
    elif needs_numeric:
        pool = [c for c in pool if is_numeric_col(graph, c)]
    if len(pool) < arg_count:
        pool = list(tcols)
    if not pool:
        return None

    out: List[str] = []
    for _ in range(arg_count):
        out.append(random.choice(pool))
    return out


def _fn_arg_count(binder_artifact: Dict[str, Any], fn: str, default: int = 1) -> int:
    catalogs = binder_artifact.get("catalogs") or {}
    meta = (catalogs.get("functions") or {}).get(fn, {}) or {}
    args = meta.get("args")
    if isinstance(args, list) and len(args) > 0:
        return len(args)
    return default


def _join_items(items: List[str], list_style: str) -> str:
    if not items:
        return ""
    if len(items) == 1:
        return items[0]
    if list_style == "and":
        return " and ".join(items)
    if list_style == "oxford":
        return ", ".join(items[:-1]) + ", and " + items[-1]
    return ", ".join(items)


def _basename_or_dotted(col: str, use_basenames: bool) -> str:
    if use_basenames and "." in col:
        return col.split(".", 1)[1]
    return col


def _items_to_nl_variants(
    table: str,
    items: List[str],
    *,
    allow_basenames: bool,
    list_style: str,
) -> List[str]:
    variants: List[str] = []

    def _mk(style: str, use_basenames: bool) -> str:
        conv_items: List[str] = []
        for it in items:
            if " of " in it:
                fn, rest = it.split(" of ", 1)
                parts = [p.strip() for p in rest.split(",")]
                parts2 = []
                for p in parts:
                    p = p.strip()
                    if " and " in p:
                        for sub in p.split(" and "):
                            sub = sub.strip()
                            parts2.append(_basename_or_dotted(sub, use_basenames))
                    else:
                        parts2.append(_basename_or_dotted(p, use_basenames))
                conv_items.append(f"{fn} of {', '.join(parts2)}")
            else:
                conv_items.append(_basename_or_dotted(it, use_basenames))
        return f"select {_join_items(conv_items, style)} from {table}"

    variants.append(_mk(list_style, allow_basenames))
    for alt in LIST_STYLES:
        if alt != list_style:
            variants.append(_mk(alt, allow_basenames))
    if allow_basenames:
        variants.append(_mk(list_style, False))
    variants.append(_mk(list_style, allow_basenames).upper())

    seen = set()
    out = []
    for v in variants:
        if v not in seen:
            seen.add(v)
            out.append(v)
    return out


def realize_plan_row(
    table: str,
    list_size: int,
    list_style: str,
    mix: List[str],
    *,
    tcols: List[str],
    fns_select: List[str],
    binder_artifact: Dict[str, Any],
    graph: Dict[str, Any],
    unique_bases: set[str],
) -> Optional[Dict[str, Any]]:
    if not tcols:
        return None

    items: List[str] = []
    used_functions: List[str] = []
    for kind in mix:
        if kind == "col":
            items.append(random.choice(tcols))
        else:
            if not fns_select:
                return None
            fn = random.choice(fns_select)
            argc = _fn_arg_count(binder_artifact, fn, default=1)
            args = _pick_fn_args_for_table(table, tcols, fn, argc, graph)
            if not args:
                return None
            if argc == 1:
                items.append(f"{fn} of {args[0]}")
            else:
                items.append(f"{fn} of {' and '.join(args)}")
            used_functions.append(fn)

    canonical_seed = f"select {_join_items(items, list_style)} from {table}"

    allow_basenames = True
    dotted_refs: List[str] = []
    for it in items:
        if " of " in it:
            _, rest = it.split(" of ", 1)
            parts = [p.strip() for p in rest.replace(",", " and ").split(" and ") if p.strip()]
            dotted_refs.extend([p for p in parts if "." in p])
        else:
            if "." in it:
                dotted_refs.append(it)

    for ref in dotted_refs:
        base = ref.split(".", 1)[1]
        if base not in unique_bases:
            allow_basenames = False
            break

    nl_variants = _items_to_nl_variants(
        table=table,
        items=items,
        allow_basenames=allow_basenames,
        list_style=list_style,
    )

    return {
        "table": table,
        "list_size": list_size,
        "list_style": list_style,
        "content_mix": list(mix),
        "functions_used": used_functions,
        "columns_from_table": tcols[:],
        "items": items,
        "canonical_seed": canonical_seed,
        "nl_variants": nl_variants,
        "allow_basenames": bool(allow_basenames),
    }


def build_plan_records(
    graph: Dict[str, Any],
    binder_artifact: Dict[str, Any],
    *,
    per_table: int = 20,
    list_styles: Tuple[str, ...] = LIST_STYLES,
) -> List[Dict[str, Any]]:
    tmap = table_to_columns(graph)
    fns = select_friendly_functions(binder_artifact)
    unique_bases = get_global_unique_basenames(graph)
    tables = sorted([t for t, cols in tmap.items() if cols])

    records: List[Dict[str, Any]] = []
    rng = random.Random(1337)

    for t in tables:
        tcols = tmap.get(t, [])
        if not tcols:
            continue

        bag = []
        for _ in range(per_table):
            size = rng.choice([1, 1, 2, 2, 3])
            style = rng.choice(list_styles)
            mix = rng.choice(CONTENT_MIXES_BY_SIZE[size])
            bag.append((size, style, mix))

        for size, style, mix in bag:
            rec = realize_plan_row(
                table=t,
                list_size=size,
                list_style=style,
                mix=mix,
                tcols=tcols,
                fns_select=fns,
                binder_artifact=binder_artifact,
                graph=graph,
                unique_bases=unique_bases,
            )
            if rec:
                records.append(rec)

    return records


def write_yaml(out_path: str, artifacts_dir: str, records: List[Dict[str, Any]]) -> None:
    doc = {
        "run_info": {
            "created_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
            "artifacts_dir": artifacts_dir,
            "total_cases": len(records),
        },
        "cases": records,
    }
    with open(out_path, "w", encoding="utf-8") as f:
        yaml.safe_dump(doc, f, sort_keys=False, allow_unicode=True)


# -----------------------------
# NEW: Golden prompts builder
# -----------------------------

def collect_nl_variants(records: List[Dict[str, Any]]) -> List[str]:
    """
    Flatten and de-duplicate all NL variants across records.
    Order is preserved by first appearance.
    """
    seen = set()
    out: List[str] = []
    for rec in records:
        for nl in rec.get("nl_variants", []) or []:
            if not isinstance(nl, str):
                continue
            if nl not in seen:
                seen.add(nl)
                out.append(nl)
    return out


def _chunk(seq: List[str], size: int) -> List[List[str]]:
    return [seq[i:i + size] for i in range(0, len(seq), size)]


def write_golden_prompts_md(out_md_path: str, artifacts_dir: str, nl_list: List[str], batch_size: int = 120) -> None:
    """
    Create GoldenPhrasePrompts.md with 1..N prompts instructing an LLM
    to produce a goldens.yaml file for the provided NL candidates.
    """
    total = len(nl_list)
    batches = _chunk(nl_list, max(1, batch_size))

    header = [
        "# Golden Phrase Review Prompts",
        "",
        f"- Generated: **{datetime.utcnow().isoformat(timespec='seconds')}Z**",
        f"- Artifacts: `{artifacts_dir}`",
        f"- Total NL candidates: **{total}** (batch size: {batch_size}; prompts: {len(batches)})",
        "",
        "---",
        "",
        "### Output format (return exactly YAML, nothing else):",
        "```yaml",
        "cases:",
        "  - nl: \"<copy one candidate exactly>\"",
        "    expect_ok: true   # or false",
        "    reason: \"short justification (type compatibility, ordering clause, etc.)\"",
        "```",
        "",
        "**Guidance:**",
        "- Mark `expect_ok: true` if the NL should bind & parse under our runtime (single FROM table).",
        "- Mark `expect_ok: false` for clause-like constructs in SELECT (e.g., `order_by_asc`, `having`, `group_by`).",
        "- `st_*` usually require geometry columns; `sum/avg/min/max/count` typically require numeric columns.",
        "- Mixed tables in SELECT are usually **not** allowed (single FROM).",
        "- When unsure, lean `true` but note the ambiguity in `reason`.",
        "",
        "---",
        "",
    ]

    lines: List[str] = []
    lines.extend(header)

    for idx, batch in enumerate(batches, start=1):
        lines.append(f"## Prompt {idx} of {len(batches)}")
        lines.append("")
        lines.append("You are curating NL→SQL candidates. Review the list below and return **only** the YAML schema above.")
        lines.append("")
        lines.append("**Candidates:**")
        lines.append("")
        for n, nl in enumerate(batch, start=1):
            lines.append(f"{n}. {nl}")
        lines.append("")
        lines.append("---")
        lines.append("")

    with open(out_md_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


# -----------------------------
# CLI + Orchestration
# -----------------------------

def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Generate canonical seeds + NL variants (YAML) for manual review / golden selection."
    )
    p.add_argument("--artifacts", required=True, help="Directory with Phase-H artifacts (out/).")
    p.add_argument("--out", required=True, help="Path to write YAML (e.g., phrase_explorer.yaml).")
    p.add_argument("--per-table", type=int, default=20, help="Approx. cases per table (default 20).")
    p.add_argument(
        "--styles",
        default="comma,and,oxford",
        help="Comma-separated list styles to include (comma,and,oxford).",
    )
    # No new flags needed; prompts file is produced automatically next to --out
    return p.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    graph, vocabulary, binder_artifact, grammar_text = load_artifacts(args.artifacts)
    validate_artifacts(graph, vocabulary, binder_artifact, grammar_text)

    styles = tuple([s.strip() for s in args.styles.split(",") if s.strip()]) or LIST_STYLES
    records = build_plan_records(
        graph,
        binder_artifact,
        per_table=int(args.per_table),
        list_styles=styles,  # type: ignore[arg-type]
    )
    write_yaml(args.out, args.artifacts, records)

    # Build GoldenPhrasePrompts.md right after YAML
    nl_all = collect_nl_variants(records)
    md_path = args.out.rsplit("/", 1)[0] + "/GoldenPhrasePrompts.md" if "/" in args.out else "GoldenPhrasePrompts.md"
    write_golden_prompts_md(md_path, args.artifacts, nl_all, batch_size=120)

    print(f"Wrote {len(records)} cases → {args.out}")
    print(f"Wrote {len(nl_all)} NL candidates across prompts → {md_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
