# tests/test_db_exec.py
import os
import sqlite3
import tempfile

from vbg_runtime.db_exec import run_sqlite


def _make_tmp_db() -> str:
    fd, path = tempfile.mkstemp(prefix="vbg_sql_", suffix=".db")
    os.close(fd)
    conn = sqlite3.connect(path)
    try:
        cur = conn.cursor()
        cur.execute('CREATE TABLE "users" ("user_id" INTEGER, "username" TEXT)')
        cur.executemany('INSERT INTO "users" ("user_id","username") VALUES (?,?)', [(1, "a"), (2, "b")])
        conn.commit()
    finally:
        conn.close()
    return path


def test_run_sqlite_happy_path_and_limit():
    db = _make_tmp_db()
    try:
        cols, rows, err = run_sqlite(db, 'SELECT "users"."user_id", "users"."username" FROM "users";', limit=1)
        assert err is None
        assert cols == ["user_id", "username"]
        assert len(rows) == 1
        assert rows[0][0] in (1, 2)
    finally:
        os.remove(db)


def test_run_sqlite_bad_sql_returns_error():
    db = _make_tmp_db()
    try:
        cols, rows, err = run_sqlite(db, 'SELECT * FROM "not_a_table";')
        assert cols is None and rows is None
        assert isinstance(err, str) and err  # has an error message
    finally:
        os.remove(db)
