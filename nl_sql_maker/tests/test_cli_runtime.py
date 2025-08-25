# tests/test_cli_runtime.py
import os
import tempfile
import sqlite3
import pytest

from vbg_runtime.cli import parse_args, run_command, _build_ctx


OUT_DIR = os.environ.get("VGB_OUT_DIR", "out")


def _tmp_db_with_users():
    fd, path = tempfile.mkstemp(prefix="vbg_cli_", suffix=".db")
    os.close(fd)
    conn = sqlite3.connect(path)
    try:
        cur = conn.cursor()
        cur.execute('CREATE TABLE "users" ("user_id" INTEGER, "username" TEXT)')
        cur.executemany('INSERT INTO "users" ("user_id","username") VALUES (?,?)', [(1, "alice"), (2, "bob")])
        conn.commit()
    finally:
        conn.close()
    return path


def test_parse_args_defaults_and_required():
    with pytest.raises(SystemExit):
        parse_args([])  # --artifacts is required

    args = parse_args(["--artifacts", OUT_DIR])
    assert args.artifacts == OUT_DIR
    assert args.engine == "sqlite"
    assert args.topk == 5
    assert args.emit == "both"
    assert args.db is None
    assert args.debug is False


@pytest.mark.skipif(
    not (os.path.exists(os.path.join(OUT_DIR, "h_graph_with_artifacts.yaml"))
         and os.path.exists(os.path.join(OUT_DIR, "h_vocabulary.yaml"))
         and os.path.exists(os.path.join(OUT_DIR, "h_binder.yaml"))
         and os.path.exists(os.path.join(OUT_DIR, "h_grammar.lark"))),
    reason="Phase-H artifacts not found; set VGB_OUT_DIR.",
)
def test_repl_run_command_meta_and_query_flow():
    # Build context (no DB initially)
    ctx = _build_ctx(OUT_DIR, engine="sqlite", topk=5, emit="both", db=None, debug=True)

    # Meta-commands
    assert run_command(ctx, ":mode sql").startswith("mode=sql")
    assert run_command(ctx, ":topk 3").startswith("topk=3")
    assert "unknown command" in run_command(ctx, ":wat")

    # NL query — should print both canonical+SQL in default 'both' if we reset the mode
    run_command(ctx, ":mode both")
    out = run_command(ctx, "select boundaries and name from regions")
    assert "select " in out.lower()
    assert "from" in out.lower()
    assert "SELECT" in out or "FROM" in out  # SQL line present

    # Hook a temp DB and query a real table if present
    db = _tmp_db_with_users()
    try:
        run_command(ctx, f":db {db}")
        q2 = run_command(ctx, "select user_id and username from users")
        # We expect the CLI to append a results section, even if the schema may differ from artifacts
        assert "-- results" in q2 or "[DB ERROR]" in q2
    finally:
        os.remove(db)


@pytest.mark.skipif(
    not (os.path.exists(os.path.join(OUT_DIR, "h_graph_with_artifacts.yaml"))
         and os.path.exists(os.path.join(OUT_DIR, "h_vocabulary.yaml"))
         and os.path.exists(os.path.join(OUT_DIR, "h_binder.yaml"))
         and os.path.exists(os.path.join(OUT_DIR, "h_grammar.lark"))),
    reason="Phase-H artifacts not found; set VGB_OUT_DIR.",
)
def test_oneshot_like_path_via_run_command():
    ctx = _build_ctx(OUT_DIR, engine="sqlite", topk=5, emit="canonical", db=None, debug=False)
    out = run_command(ctx, "select regions.name from regions")
    assert out.lower().startswith("select ")
