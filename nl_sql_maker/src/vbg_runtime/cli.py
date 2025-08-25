# src/vbg_runtime/cli.py
from __future__ import annotations

import argparse
import os
import re
import sys
from typing import Any, Dict, Tuple, Optional

# Runtime pieces
from vbg_runtime.artifacts_loader import load_artifacts, validate_artifacts  # Step 1
from vbg_runtime.parser_runtime import make_parser                      # Step 3
from vbg_runtime.nl2sql_engine import nl2sql_once                       # Step 7
from vbg_runtime.diagnostics import format_result                       # Step 9
from vbg_runtime.config import RuntimeConfig, apply_config_defaults     # Step 10
from vbg_runtime.db_exec import run_sqlite                              # Step 6 (optional)
from vbg_generate.canonical_core import canon_tokenize                  # tokens view for :tokens

APP_DESC = "VBG NL→SQL runtime CLI (normalize → bind → parse → SQL, optional execution)."

# --- auto-Oxford helper (always on) ---
_FN_OXFORD_RE = re.compile(r'(\bof\b[^,]+?)\s+and\s+([A-Za-z_][A-Za-z0-9_]*)\s+\bof\b')

def _auto_oxfordize_functions(text: str) -> Tuple[str, bool]:
    """
    Turn '... of X and fn of Y' into '... of X, and fn of Y' repeatedly.
    Returns (new_text, changed).
    """
    changed = False
    prev = None
    new = text
    while prev != new:
        prev = new
        newer = _FN_OXFORD_RE.sub(r'\1, and \2 of', new)
        if newer != new:
            changed = True
        new = newer
    return new, changed

# -----------------------------
# Arg parsing
# -----------------------------
def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=APP_DESC)
    p.add_argument(
        "--artifacts", required=True,
        help="Directory containing h_graph_with_artifacts.yaml, h_vocabulary.yaml, h_binder.yaml, h_grammar.lark"
    )
    p.add_argument("--db", help="Database path (e.g., test.db for sqlite).")
    p.add_argument("--engine", choices=["sqlite", "postgres"], default="sqlite", help="SQL engine (default inferred).")
    p.add_argument("--topk", type=int, default=5, help="Max candidates to try (default 5).")
    p.add_argument("--emit", choices=["canonical", "sql", "both", "tokens"], default="both",
                   help="What to print for each query (default both).")
    p.add_argument("--casefold", action="store_true", help="Case-insensitive normalization.")
    p.add_argument("--strict", dest="strict", action="store_true", help="Strict binder typing (default).")
    p.add_argument("--no-strict", dest="strict", action="store_false", help="Lenient binder typing.")
    p.set_defaults(strict=True)

    p.add_argument("--execute", action="store_true", help="Execute SQL against --db if provided.")
    p.add_argument("--limit-rows", type=int, default=None, help="Row limit for execution (default 1000 when --execute).")
    p.add_argument("--debug", action="store_true", help="Show diagnostics.")

    # One-shot (non-REPL) mode:
    p.add_argument("--oneshot", help='One-shot NL query to process (e.g., "price and date from sales").')

    return p.parse_args(argv)


# -----------------------------
# Console helpers
# -----------------------------
def _print(s: str) -> None:
    sys.stdout.write(s + ("\n" if not s.endswith("\n") else ""))
    sys.stdout.flush()


def _format_exec_result(columns: list[str], rows: list[tuple], limit: Optional[int]) -> str:
    if not rows:
        return "(no rows)"
    # Simple text table
    col_line = " | ".join(columns)
    sep_line = "-+-".join("-" * len(c) for c in columns)
    data_lines = []
    for r in rows[: (limit or len(rows))]:
        data_lines.append(" | ".join("" if v is None else str(v) for v in r))
    tail = ""
    if limit is not None and len(rows) > limit:
        tail = f"\n… ({len(rows) - limit} more)"
    return "\n".join([col_line, sep_line, *data_lines]) + tail


# -----------------------------
# One-shot invocation
# -----------------------------
def _run_oneshot(
    text: str,
    *,
    graph: Dict[str, Any],
    vocabulary: Dict[str, Any],
    binder_artifact: Dict[str, Any],
    parser,
    cfg: RuntimeConfig,
    emit_mode: str,
    debug: bool,
) -> int:
    # 1) Always try to Oxfordize function lists in the *input* text
    oxford_note = ""
    fixed_text, changed = _auto_oxfordize_functions(text)
    if changed:
        text = fixed_text
        oxford_note = "(auto: oxford)"

    # 2) First attempt (respect --strict / --no-strict)
    res = nl2sql_once(
        text,
        graph=graph,
        vocabulary=vocabulary,
        binder_artifact=binder_artifact,
        parser=parser,
        engine=cfg.engine,
        topk=cfg.topk,
        case_insensitive=cfg.case_insensitive,
        strict_binder=cfg.strict_binding,
    )

    # 3) Auto-relax if the strict pass failed with binder-only issues
    relaxed_note = ""
    if (not res.get("ok")) and res.get("fail_category") == "binder_fail" and cfg.strict_binding:
        res2 = nl2sql_once(
            text,
            graph=graph,
            vocabulary=vocabulary,
            binder_artifact=binder_artifact,
            parser=parser,
            engine=cfg.engine,
            topk=cfg.topk,
            case_insensitive=cfg.case_insensitive,
            strict_binder=False,
        )
        if res2.get("ok"):
            res = res2
            relaxed_note = "(auto: lenient)"

    # Primary output
    msg = format_result(res, emit_mode=emit_mode, debug=debug)
    if debug and (oxford_note or relaxed_note):
        notes = " ".join(x for x in [oxford_note, relaxed_note] if x)
        msg += f"\n{notes}"
    _print(msg)

    # Optional execution
    if cfg.execute_sql and cfg.engine == "sqlite" and cfg.db and res.get("ok") and res.get("sql"):
        columns, rows, err = run_sqlite(cfg.db, res["sql"], limit=cfg.limit_rows)
        if err:
            _print(f"[SQL ERROR] {err}")
        else:
            _print(_format_exec_result(columns, rows, cfg.limit_rows))

    return 0 if (res.get("ok") is True) else 2


# -----------------------------
# REPL loop
# -----------------------------
def repl(ctx: Dict[str, Any]) -> int:
    """
    Supported meta-commands:
      :quit / :q            — exit
      :mode sql|canonical|both|tokens
      :topk N
      :engine sqlite|postgres
      :db PATH
      :execute on|off
      :tokens               — show tokens of last winning canonical
    """
    last_serialized: Optional[str] = None
    emit_mode = ctx["emit_mode"]
    cfg: RuntimeConfig = ctx["config"]
    graph, vocabulary, binder_artifact, parser = ctx["graph"], ctx["vocabulary"], ctx["binder"], ctx["parser"]
    debug = ctx.get("debug", False)

    while True:
        try:
            line = input("nl2sql> ").strip()
        except (EOFError, KeyboardInterrupt):
            _print("")
            return 0

        if not line:
            continue

        # Meta-commands
        if line.startswith(":"):
            parts = line[1:].strip().split()
            cmd = parts[0].lower()
            args = parts[1:]

            if cmd in {"quit", "q"}:
                return 0

            elif cmd == "mode" and args:
                if args[0] in {"sql", "canonical", "both", "tokens"}:
                    emit_mode = args[0]
                    ctx["emit_mode"] = emit_mode
                    _print(f"(mode set to {emit_mode})")
                else:
                    _print("usage: :mode sql|canonical|both|tokens")

            elif cmd == "topk" and args:
                try:
                    topk = int(args[0])
                    cfg = apply_config_defaults(RuntimeConfig(
                        engine=cfg.engine, topk=topk, case_insensitive=cfg.case_insensitive,
                        strict_binding=cfg.strict_binding, execute_sql=cfg.execute_sql,
                        limit_rows=cfg.limit_rows, db=cfg.db
                    ))
                    ctx["config"] = cfg
                    _print(f"(topk set to {cfg.topk})")
                except Exception:
                    _print("usage: :topk N")

            elif cmd == "engine" and args:
                eng = args[0].lower()
                if eng in {"sqlite", "postgres"}:
                    cfg = RuntimeConfig(
                        engine=eng, topk=cfg.topk, case_insensitive=cfg.case_insensitive,
                        strict_binding=cfg.strict_binding, execute_sql=cfg.execute_sql,
                        limit_rows=cfg.limit_rows, db=cfg.db
                    )
                    cfg = apply_config_defaults(cfg)
                    ctx["config"] = cfg
                    _print(f"(engine set to {cfg.engine})")
                else:
                    _print("usage: :engine sqlite|postgres")

            elif cmd == "db" and args:
                path = " ".join(args)
                cfg = RuntimeConfig(
                    engine=cfg.engine, topk=cfg.topk, case_insensitive=cfg.case_insensitive,
                    strict_binding=cfg.strict_binding, execute_sql=cfg.execute_sql,
                    limit_rows=cfg.limit_rows, db=path
                )
                cfg = apply_config_defaults(cfg)
                ctx["config"] = cfg
                _print(f"(db set to {cfg.db}, engine={cfg.engine})")

            elif cmd == "execute" and args:
                on = args[0].lower() in {"on", "true", "1", "yes"}
                cfg = RuntimeConfig(
                    engine=cfg.engine, topk=cfg.topk, case_insensitive=cfg.case_insensitive,
                    strict_binding=cfg.strict_binding, execute_sql=on,
                    limit_rows=cfg.limit_rows, db=cfg.db
                )
                cfg = apply_config_defaults(cfg)
                ctx["config"] = cfg
                _print(f"(execute set to {cfg.execute_sql}, limit_rows={cfg.limit_rows})")

            elif cmd == "tokens":
                if last_serialized:
                    toks = canon_tokenize(last_serialized)
                    _print("tokens: " + " ".join(toks))
                else:
                    _print("(no previous canonical to tokenize)")

            else:
                _print("unknown command. try :mode / :topk / :engine / :db / :execute / :tokens / :quit")
            continue

        # Regular NL path
        res = nl2sql_once(
            line,
            graph=graph,
            vocabulary=vocabulary,
            binder_artifact=binder_artifact,
            parser=parser,
            engine=cfg.engine,
            topk=cfg.topk,
            case_insensitive=cfg.case_insensitive,
            strict_binder=cfg.strict_binding,
        )
        _print(format_result(res, emit_mode=emit_mode, debug=debug))

        if res.get("ok"):
            last_serialized = res.get("serialized_canonical") or res.get("chosen_canonical")

            # Optional execution
            if cfg.execute_sql and cfg.engine == "sqlite" and cfg.db and res.get("sql"):
                columns, rows, err = run_sqlite(cfg.db, res["sql"], limit=cfg.limit_rows)
                if err:
                    _print(f"[SQL ERROR] {err}")
                else:
                    _print(_format_exec_result(columns, rows, cfg.limit_rows))

    # (unreachable)
    return 0

def _build_ctx(artifacts_dir: str, *, engine: str = "sqlite", topk: int = 5, emit: str = "both", db: Optional[str] = None, debug: bool = False) -> Dict[str, Any]:
    """
    Test helper: load artifacts, build parser and config, and return a CLI context dict
    identical in shape to what main() passes to repl().
    """
    graph, vocabulary, binder_artifact, grammar_text = load_artifacts(artifacts_dir)
    validate_artifacts(graph, vocabulary, binder_artifact, grammar_text)
    parser = make_parser(grammar_text)

    cfg = RuntimeConfig(
        engine=engine,
        topk=topk,
        case_insensitive=False,
        strict_binding=True,
        execute_sql=False,
        limit_rows=None,
        db=db,
    )
    cfg = apply_config_defaults(cfg)

    return {
        "graph": graph,
        "vocabulary": vocabulary,
        "binder": binder_artifact,  # artifact, not the CanonicalBinder instance
        "parser": parser,
        "config": cfg,
        "emit_mode": emit,
        "debug": bool(debug),
        "last_serialized": None,
    }


def run_command(ctx: Dict[str, Any], line: str) -> str:
    """
    Single-line command handler used by tests and can also back the REPL.
    Returns a formatted string (does not print).
    """
    emit_mode = ctx.get("emit_mode", "both")
    cfg: RuntimeConfig = ctx["config"]
    graph, vocabulary, binder_artifact, parser = ctx["graph"], ctx["vocabulary"], ctx["binder"], ctx["parser"]
    debug = bool(ctx.get("debug", False))
    last_serialized: Optional[str] = ctx.get("last_serialized")

    line = (line or "").strip()
    if not line:
        return ""

    # Meta-commands (unchanged)
    if line.startswith(":"):
        parts = line[1:].strip().split()
        cmd = parts[0].lower() if parts else ""
        args = parts[1:] if len(parts) > 1 else []

        if cmd == "mode" and args:
            if args[0] in {"sql", "canonical", "both", "tokens"}:
                emit_mode = args[0]
                ctx["emit_mode"] = emit_mode
                return f"mode={emit_mode}"
            return "usage: :mode sql|canonical|both|tokens"

        if cmd == "topk" and args:
            try:
                topk = int(args[0])
                cfg = apply_config_defaults(RuntimeConfig(
                    engine=cfg.engine, topk=topk, case_insensitive=cfg.case_insensitive,
                    strict_binding=cfg.strict_binding, execute_sql=cfg.execute_sql,
                    limit_rows=cfg.limit_rows, db=cfg.db
                ))
                ctx["config"] = cfg
                return f"topk={cfg.topk}"
            except Exception:
                return "usage: :topk N"

        if cmd == "engine" and args:
            eng = args[0].lower()
            if eng in {"sqlite", "postgres"}:
                cfg = apply_config_defaults(RuntimeConfig(
                    engine=eng, topk=cfg.topk, case_insensitive=cfg.case_insensitive,
                    strict_binding=cfg.strict_binding, execute_sql=cfg.execute_sql,
                    limit_rows=cfg.limit_rows, db=cfg.db
                ))
                ctx["config"] = cfg
                return f"engine={cfg.engine}"
            return "usage: :engine sqlite|postgres"

        if cmd == "db" and args:
            path = " ".join(args)
            cfg = apply_config_defaults(RuntimeConfig(
                engine=cfg.engine, topk=cfg.topk, case_insensitive=cfg.case_insensitive,
                strict_binding=cfg.strict_binding, execute_sql=True,
                limit_rows=(cfg.limit_rows or 1000), db=path
            ))
            ctx["config"] = cfg
            return f"db={cfg.db} engine={cfg.engine}"

        if cmd == "execute" and args:
            on = args[0].lower() in {"on", "true", "1", "yes"}
            cfg = apply_config_defaults(RuntimeConfig(
                engine=cfg.engine, topk=cfg.topk, case_insensitive=cfg.case_insensitive,
                strict_binding=cfg.strict_binding, execute_sql=on,
                limit_rows=(cfg.limit_rows or 1000), db=cfg.db
            ))
            ctx["config"] = cfg
            return f"execute={cfg.execute_sql} limit_rows={cfg.limit_rows}"

        if cmd == "tokens":
            if last_serialized:
                toks = canon_tokenize(last_serialized)
                return "tokens: " + " ".join(toks)
            return "(no previous canonical to tokenize)"

        return "unknown command. try :mode / :topk / :engine / :db / :execute / :tokens / :quit"

    # --- NL → SQL path with automatic Oxford + lenient fallback ---

    # 1) Always Oxfordize function-to-function lists in the raw NL
    text = line
    oxford_note = ""
    fixed_text, changed = _auto_oxfordize_functions(text)
    if changed:
        text = fixed_text
        oxford_note = "(auto: oxford)"

    # 2) First pass respects strict / no-strict from cfg
    res = nl2sql_once(
        text,
        graph=graph,
        vocabulary=vocabulary,
        binder_artifact=binder_artifact,
        parser=parser,
        engine=cfg.engine,
        topk=cfg.topk,
        case_insensitive=cfg.case_insensitive,
        strict_binder=cfg.strict_binding,
    )

    # 3) Auto-relax if strict failed due to binder errors
    relaxed_note = ""
    if (not res.get("ok")) and res.get("fail_category") == "binder_fail" and cfg.strict_binding:
        res2 = nl2sql_once(
            text,
            graph=graph,
            vocabulary=vocabulary,
            binder_artifact=binder_artifact,
            parser=parser,
            engine=cfg.engine,
            topk=cfg.topk,
            case_insensitive=cfg.case_insensitive,
            strict_binder=False,
        )
        if res2.get("ok"):
            res = res2
            relaxed_note = "(auto: lenient)"

    out = format_result(res, emit_mode=emit_mode, debug=debug)
    if debug and (oxford_note or relaxed_note):
        notes = " ".join(x for x in [oxford_note, relaxed_note] if x)
        out += f"\n{notes}"

    # Persist last serialized for :tokens + optional DB execution
    if res.get("ok"):
        ctx["last_serialized"] = res.get("serialized_canonical") or res.get("chosen_canonical")

        if cfg.execute_sql and cfg.engine == "sqlite" and cfg.db and res.get("sql"):
            cols, rows, err = run_sqlite(cfg.db, res["sql"], limit=cfg.limit_rows)
            if err:
                out += f"\n[DB ERROR] {err}"
            else:
                if not rows:
                    out += "\n-- results\n(no rows)"
                else:
                    header = " | ".join(cols)
                    sep = "-+-".join("-" * len(c) for c in cols)
                    lines = [" | ".join("" if v is None else str(v) for v in r) for r in rows[: (cfg.limit_rows or len(rows))]]
                    tail = ""
                    if cfg.limit_rows is not None and len(rows) > cfg.limit_rows:
                        tail = f"\n… ({len(rows) - cfg.limit_rows} more)"
                    out += "\n-- results\n" + "\n".join([header, sep, *lines]) + tail

    return out

# -----------------------------
# Entrypoint
# -----------------------------
def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args(argv)

    # Artifacts
    graph, vocabulary, binder_artifact, grammar_text = load_artifacts(args.artifacts)
    validate_artifacts(graph, vocabulary, binder_artifact, grammar_text)
    parser = make_parser(grammar_text)

    # Config
    cfg = RuntimeConfig(
        engine=(args.engine or "unknown"),
        topk=args.topk,
        case_insensitive=bool(args.casefold),
        strict_binding=bool(args.strict),
        execute_sql=bool(args.execute),
        limit_rows=args.limit_rows,
        db=args.db,
    )
    cfg = apply_config_defaults(cfg)

    ctx = {
        "graph": graph,
        "vocabulary": vocabulary,
        "binder": binder_artifact,
        "parser": parser,
        "config": cfg,
        "emit_mode": args.emit,
        "debug": bool(args.debug),
    }

    if args.oneshot:
        return _run_oneshot(
            args.oneshot,
            graph=graph, vocabulary=vocabulary, binder_artifact=binder_artifact, parser=parser,
            cfg=cfg, emit_mode=args.emit, debug=bool(args.debug),
        )

    # REPL
    _print("(enter :quit to exit; :help for commands)")
    return repl(ctx)


if __name__ == "__main__":
    raise SystemExit(main())
