#!/usr/bin/env bash
# cli_test.sh
set -euo pipefail

ART_DIR="${ARTIFACTS_DIR:-out}"
VOCAB="${ART_DIR}/graph_vocabulary.yaml"
BINDER="${ART_DIR}/graph_binder.yaml"
GRAMMAR="${ART_DIR}/graph_grammar.lark"
DB_PATH="_cli_test.db"

have_cmd() { command -v "$1" >/dev/null 2>&1; }

# -------- JSON helpers --------
is_json() {
  local s="$1"
  [[ "$s" =~ ^[[:space:]]*[\{\[] ]]
}

json_get_jq() { jq -r "$1"; }

json_get_py() {
  local key="$1"
  python3 -c '
import sys, json
data = json.loads(sys.stdin.read())
expr = sys.argv[1]

def dig(d, path):
    cur = d
    for part in path.split("."):
        if part == "" or cur is None:
            continue
        if isinstance(cur, dict) and part in cur:
            cur = cur[part]
        else:
            return None
    return cur

if expr == ".parse_ok":
    v = dig(data, "parse_ok"); print("true" if v else "false")
elif expr == ".canonical_tokens | join(\" \")":
    v = dig(data, "canonical_tokens") or []; print(" ".join(v))
elif expr == ".slots.table":
    v = dig(data, "slots") or {}; print((v or {}).get("table","") or "")
elif expr == ".warnings[]?":
    for w in (data.get("warnings") or []): print(w)
elif expr == ".sql":
    v = dig(data, "sql"); print(v or "")
elif expr == ".rowcount":
    v = dig(data, "rowcount")
    try:
        print(int(v))
    except Exception:
        print(0)
else:
    print(json.dumps(data))
' "$key"
}

json_get() {
  local key="$1"
  if have_cmd jq; then json_get_jq "$key"; else json_get_py "$key"; fi
}

# -------- artifact builder --------
ensure_artifacts() {
  if [[ -f "$VOCAB" && -f "$BINDER" && -f "$GRAMMAR" ]]; then return 0; fi
  echo "Artifacts not found in ${ART_DIR}. Building with vbg_tools/graph_to_artifacts.py..."
  python3 vbg_tools/graph_to_artifacts.py
}

# -------- test DB builder (from binder) --------
build_test_db() {
  rm -f "$DB_PATH"
  python3 - <<PY
import sqlite3, yaml
from pathlib import Path

binder_path = Path("$BINDER")
db_path = "$DB_PATH"

binder = yaml.safe_load(binder_path.read_text()) or {}
cats = (binder.get("catalogs") or {})
cols = (cats.get("columns") or {})
tables = (cats.get("tables") or {})

# Collect columns per table
cols_by_table = {t:{} for t in (tables.keys() if isinstance(tables, dict) else [])}
for fqn, meta in cols.items():
    if not isinstance(meta, dict): continue
    t = meta.get("table")
    c = meta.get("name")
    if not t or not c: continue
    cols_by_table.setdefault(t, {})
    cols_by_table[t][c] = "TEXT"  # keep it simple for tests

# Ensure each table has at least 1 column
for t in list(cols_by_table.keys()):
    if not cols_by_table[t]:
        cols_by_table[t]["id"] = "TEXT"

con = sqlite3.connect(db_path)
try:
    cur = con.cursor()
    for t, cmap in cols_by_table.items():
        defs = ", ".join([f'"{c}" {typ}' for c, typ in cmap.items()])
        cur.execute(f'CREATE TABLE "{t}" ({defs})')
        # Insert two simple rows per table
        columns = list(cmap.keys())
        placeholders = ", ".join(["?"]*len(columns))
        cols_quoted = ", ".join(['"{}"'.format(c) for c in columns])
        vals1 = [f"{c}_1" for c in columns]
        vals2 = [f"{c}_2" for c in columns]
        cur.execute(f'INSERT INTO "{t}" ({cols_quoted}) VALUES ({placeholders})', vals1)
        cur.execute(f'INSERT INTO "{t}" ({cols_quoted}) VALUES ({placeholders})', vals2)
    con.commit()
finally:
    con.close()
PY
}

# -------- runtime resolver --------
_probe_runtime_json() {
  local cmd="$1"
  local probe_out rc
  set +e
  probe_out=$(bash -lc "$cmd --json 'probe query'" 2>/dev/null)
  rc=$?
  set -e
  [[ $rc -eq 0 && -n "$probe_out" ]] && is_json "$probe_out"
}

resolve_runtime() {
  local cand1="python3 vbg_tools/graph_runtime.py"
  local cand2="vbg_runtime"
  if [[ -f "vbg_tools/graph_runtime.py" ]] && _probe_runtime_json "$cand1"; then echo "$cand1"; return; fi
  if have_cmd vbg_runtime && _probe_runtime_json "$cand2"; then echo "$cand2"; return; fi
  echo "ERROR: Could not find a JSON-capable runtime." >&2; exit 127
}


# -------- single-case runners --------
run_case() {
  local name="$1"; shift
  local utterance="$1"; shift
  local expect_regex="$1"; shift
  local want_ok="${1:-true}"

  local RT; RT="$(resolve_runtime)"

  echo
  echo "── Case: ${name}"
  echo "   NL:   ${utterance}"

  local out err rc
  set +e
  out="$(${RT} --json "${utterance}" 2>_cli_rt_err.txt)"
  rc=$?
  err="$(cat _cli_rt_err.txt || true)"
  rm -f _cli_rt_err.txt
  set -e

  if [[ $rc -ne 0 || -z "${out}" ]] || ! is_json "${out}"; then
    echo "   RAW STDOUT:"; printf '     %s\n' "${out//$'\n'/$'\n     '}"
    if [[ -n "${err}" ]]; then echo "   RAW STDERR:"; printf '     %s\n' "${err//$'\n'/$'\n     '}"; fi
    echo "   ❌ Not JSON output from runtime (rc=${rc}); failing case."
    echo "   ❌ FAIL"
    return 1
  fi

  local joined parse_ok table
  joined="$(printf '%s' "${out}" | json_get '.canonical_tokens | join(" ")' || true)"
  parse_ok="$(printf '%s' "${out}" | json_get '.parse_ok' || true)"
  table="$(printf '%s' "${out}" | json_get '.slots.table' || true)"

  echo "   TOKENS: ${joined}"
  echo "   TABLE:  ${table:-<none>}"
  echo "   PARSE:  ${parse_ok}"

  local ok=1
  if ! [[ "${joined}" =~ ${expect_regex} ]]; then echo "   ❌ Expected tokens ~ ${expect_regex}"; ok=0; fi
  if [[ "${want_ok}" == "true"  && "${parse_ok}" != "true" ]];  then echo "   ❌ Expected parse_ok=true";  ok=0; fi
  if [[ "${want_ok}" == "false" && "${parse_ok}" != "false" ]]; then echo "   ❌ Expected parse_ok=false"; ok=0; fi

  local warnings
  warnings="$(printf '%s' "${out}" | json_get '.warnings[]?' || true)"
  if [[ -n "${warnings}" ]]; then
    echo "   WARNINGS:"; printf '     - %s\n' ${warnings}
  fi

  if [[ $ok -eq 1 ]]; then echo "   ✅ PASS"; return 0; else echo "   ❌ FAIL"; return 1; fi
}

run_case_sql() {
  local name="$1"; shift
  local utterance="$1"; shift
  local expect_regex="$1"; shift
  local min_rows="${1:-1}"

  local RT; RT="$(resolve_runtime)"

  echo
  echo "── Case (SQL): ${name}"
  echo "   NL:         ${utterance}"

  local out err rc
  set +e
  out="$(${RT} --json --sql --db "${DB_PATH}" "${utterance}" 2>_cli_rt_err.txt)"
  rc=$?
  err="$(cat _cli_rt_err.txt || true)"
  rm -f _cli_rt_err.txt
  set -e

  if [[ $rc -ne 0 || -z "${out}" ]] || ! is_json "${out}"; then
    echo "   RAW STDOUT:"; printf '     %s\n' "${out//$'\n'/$'\n     '}"
    if [[ -n "${err}" ]]; then echo "   RAW STDERR:"; printf '     %s\n' "${err//$'\n'/$'\n     '}"; fi
    echo "   ❌ Not JSON output from runtime (rc=${rc}); failing case."
    echo "   ❌ FAIL"
    return 1
  fi

  local joined parse_ok table sql rowcount
  joined="$(printf '%s' "${out}" | json_get '.canonical_tokens | join(" ")' || true)"
  parse_ok="$(printf '%s' "${out}" | json_get '.parse_ok' || true)"
  table="$(printf '%s' "${out}" | json_get '.slots.table' || true)"
  sql="$(printf '%s' "${out}" | json_get '.sql' || true)"
  rowcount="$(printf '%s' "${out}" | json_get '.rowcount' || true)"

  echo "   TOKENS: ${joined}"
  echo "   TABLE:  ${table:-<none>}"
  echo "   PARSE:  ${parse_ok}"
  echo "   SQL:    ${sql:-<none>}"
  echo "   ROWS:   ${rowcount:-0}"

  local ok=1
  if ! [[ "${joined}" =~ ${expect_regex} ]]; then echo "   ❌ Expected tokens ~ ${expect_regex}"; ok=0; fi
  if [[ "${parse_ok}" != "true" ]]; then echo "   ❌ Expected parse_ok=true"; ok=0; fi
  if [[ -z "${sql}" ]]; then echo "   ❌ Expected SQL to be present"; ok=0; fi
  if [[ "${rowcount}" -lt "${min_rows}" ]]; then echo "   ❌ Expected at least ${min_rows} row(s)"; ok=0; fi

  local warnings
  warnings="$(printf '%s' "${out}" | json_get '.warnings[]?' || true)"
  if [[ -n "${warnings}" ]]; then
    echo "   WARNINGS:"; printf '     - %s\n' ${warnings}
  fi

  if [[ $ok -eq 1 ]]; then echo "   ✅ PASS"; return 0; else echo "   ❌ FAIL"; return 1; fi
}

# -------- main --------
ensure_artifacts
build_test_db

FAILS=0
# Parse-only cases (unchanged)
run_case "select-from users"   "show users"       "^SELECT[[:space:]]+FROM$" true   || FAILS=$((FAILS+1))
run_case "select-from sales"   "list the sales"   "^SELECT[[:space:]]+FROM$" true   || FAILS=$((FAILS+1))
run_case "select-from regions" "display regions"  "^SELECT[[:space:]]+FROM$" true   || FAILS=$((FAILS+1))
run_case "unrecognized text"   "blorp snorfle"    "^$"                         false || FAILS=$((FAILS+1))

# SQL execution cases (new)
run_case_sql "sql users basic"   "show users"        "^SELECT[[:space:]]+FROM$" 2 || FAILS=$((FAILS+1))
run_case_sql "sql sales basic"   "list the sales"    "^SELECT[[:space:]]+FROM$" 2 || FAILS=$((FAILS+1))
run_case_sql "sql regions basic" "display regions"   "^SELECT[[:space:]]+FROM$" 2 || FAILS=$((FAILS+1))

echo
if [[ $FAILS -eq 0 ]]; then
  echo "All CLI graph + SQL tests passed."
else
  echo "${FAILS} test(s) failed."
  exit 1
fi
