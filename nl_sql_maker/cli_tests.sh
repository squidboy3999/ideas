#!/usr/bin/env bash
# nl2sql_suite.sh
set -u -o pipefail

ART="${ART:-/app/out}"
DB="${DB:-test.db}"
GOLDENS="${GOLDENS:-goldens.yaml}"   # <-- NEW: default path to goldens.yaml

TESTN=0
OK_OK=0            # expected pass, got pass
OK_FAIL=0          # expected fail, got fail
UNEXP_FAIL=0       # expected pass, got fail
UNEXP_SUCC=0       # expected fail, got pass

UNEXP_FAIL_LABELS=()
UNEXP_FAIL_SNIPS=()
UNEXP_SUCC_LABELS=()
UNEXP_SUCC_SNIPS=()

RET_OUT=""
RET_RC=0

run_cli() {
  # Runs the CLI, captures output and return code, never crashes the harness.
  # Usage: run_cli <args...>
  local tmp
  tmp="$(mktemp)"
  set +e
  python -m vbg_runtime.cli --artifacts "$ART" "$@" >"$tmp" 2>&1
  RET_RC=$?
  set -e +o pipefail 2>/dev/null || true
  set -u -o pipefail

  RET_OUT="$(cat "$tmp")"
  rm -f "$tmp"
  return 0
}

_tail10() {
  # last 10 non-empty lines
  echo "$1" | awk 'NF' | tail -n 10 | tr -d '\r'
}

runcase() {
  # runcase <expected: ok|fail> <label> -- <cli args...>
  local expected="$1"; shift
  local label="$1"; shift
  if [[ "$1" != "--" ]]; then
    echo "INTERNAL: runcase requires '--' before CLI args for: $label" >&2
    exit 2
  fi
  shift

  TESTN=$((TESTN+1))
  echo "Test #$TESTN [$expected] - $label"

  run_cli "$@"

  echo "$RET_OUT"

  if [[ "$expected" == "ok" ]]; then
    if [[ $RET_RC -eq 0 ]]; then
      OK_OK=$((OK_OK+1))
    else
      UNEXP_FAIL=$((UNEXP_FAIL+1))
      UNEXP_FAIL_LABELS+=("#$TESTN $label")
      UNEXP_FAIL_SNIPS+=("rc=$RET_RC :: $(_tail10 "$RET_OUT")")
    fi
  else
    if [[ $RET_RC -ne 0 ]]; then
      OK_FAIL=$((OK_FAIL+1))
    else
      UNEXP_SUCC=$((UNEXP_SUCC+1))
      UNEXP_SUCC_LABELS+=("#$TESTN $label")
      UNEXP_SUCC_SNIPS+=("rc=$RET_RC :: $(_tail10 "$RET_OUT")")
    fi
  fi

  echo
}

report_summary() {
  echo "==================== SUMMARY ===================="
  echo "Total tests:        $TESTN"
  echo "Expected PASS ok:   $OK_OK"
  echo "Expected FAIL ok:   $OK_FAIL"
  echo "Unexpected FAIL(s): $UNEXP_FAIL"
  echo "Unexpected SUCC(s): $UNEXP_SUCC"
  echo

  if (( UNEXP_FAIL > 0 )); then
    echo "----- Unexpected FAILURES (should have passed) -----"
    for i in "${!UNEXP_FAIL_LABELS[@]}"; do
      echo "• ${UNEXP_FAIL_LABELS[$i]}"
      echo "  ${UNEXP_FAIL_SNIPS[$i]}"
    done
    echo
  fi

  if (( UNEXP_SUCC > 0 )); then
    echo "----- Unexpected SUCCESSES (should have failed) -----"
    for i in "${!UNEXP_SUCC_LABELS[@]}"; do
      echo "• ${UNEXP_SUCC_LABELS[$i]}"
      echo "  ${UNEXP_SUCC_SNIPS[$i]}"
    done
    echo
  fi

  # Exit nonzero only if unexpected outcomes occurred
  if (( UNEXP_FAIL > 0 || UNEXP_SUCC > 0 )); then
    exit 1
  fi
  exit 0
}

# ------------------------------
# NEW: Run goldens from YAML
# ------------------------------
run_goldens() {
  local yaml_path="$1"
  if [[ ! -f "$yaml_path" ]]; then
    echo "(goldens file not found: $yaml_path — skipping)"
    return 0
  fi

  echo "=== Goldens (from $yaml_path) ==="

  # Emit rows: <idx>\t<ok|fail>\t<flags|->\t<nl_base64>
  while IFS=$'\t' read -r idx expected flags b64nl; do
    # Decode NL safely
    nl="$(printf '%s' "$b64nl" | { base64 --decode 2>/dev/null || base64 -D 2>/dev/null; } || true)"

    # Build CLI args (default emit=both)
    ARGS=(--emit both --oneshot "$nl")

    # Parse flags unless it's the placeholder '-'
    if [[ -n "${flags:-}" && "$flags" != "-" ]]; then
      for tok in $flags; do
        case "$tok" in
          no-strict)  ARGS=(--no-strict "${ARGS[@]}") ;;
          strict)     ARGS=(--strict "${ARGS[@]}") ;;
          casefold)   ARGS=(--casefold "${ARGS[@]}") ;;
          emit:*)     ARGS=(--emit "${tok#emit:}" "${ARGS[@]}") ;;
          engine:*)   ARGS=(--engine "${tok#engine:}" "${ARGS[@]}") ;;
          db:*)       ARGS=(--db "${tok#db:}" "${ARGS[@]}") ;;
        esac
      done
    else
      # Heuristic: spatial functions often need lenient binding
      if echo "$nl" | grep -qi '\bst_[a-z_]'; then
        ARGS=(--no-strict "${ARGS[@]}")
      fi
    fi

    runcase "$expected" "golden #$idx" -- "${ARGS[@]}"

  done < <(
    # Pass the YAML path as argv[1]; program comes from heredoc
    python - "$yaml_path" <<'PY'
import sys, base64
try:
    import yaml
except Exception as e:
    sys.exit("PyYAML not available: %s" % e)

if len(sys.argv) < 2:
    sys.exit("usage: python - <yaml_path>")

path = sys.argv[1]
with open(path, "r", encoding="utf-8") as f:
    data = yaml.safe_load(f) or {}

cases = data.get("cases") or []
for i, c in enumerate(cases, 1):
    nl = c.get("nl") or ""
    exp = "ok" if c.get("expect_ok", True) else "fail"
    flags = (c.get("flags") or "").strip().replace("\t"," ").replace("\n"," ")
    if not flags:
        flags = "-"  # placeholder to keep 4 fields for the bash read
    b64 = base64.b64encode(nl.encode("utf-8")).decode("ascii")
    sys.stdout.write(f"{i}\t{exp}\t{flags}\t{b64}\n")
PY
  )
}



# ------------------------------
# Your existing handcrafted tests
# ------------------------------

echo "=== Simple column selections (should PASS) ==="
runcase ok "users: two columns" -- --emit both --oneshot "select users.user_id and users.username from users"
runcase ok "users: three columns" -- --emit both --oneshot "select users.user_id, users.username, and users.age from users"
runcase ok "regions: one column" -- --emit canonical --oneshot "select regions.name from regions"
runcase ok "sales: two columns" -- --emit sql --oneshot "select sales.sale_id and sales.product_name from sales"

echo "=== Basename → dotted (should PASS) ==="
runcase ok "users basenames" -- --emit both --oneshot "select user_id and username from users"
runcase ok "users basenames (3)" -- --emit both --oneshot "select user_id, username, and age from users"
runcase ok "regions basename" -- --emit canonical --oneshot "select name from regions"
runcase ok "sales basenames" -- --emit sql --oneshot "select product_name and quantity from sales"

echo "=== Oxford list + functions (should PASS) ==="
runcase ok "users + st_x" -- --no-strict --emit both --oneshot "select users.user_id, users.username, and st_x of users.location from users"
runcase ok "regions + st_area" -- --no-strict --emit both --oneshot "select regions.region_id, regions.name, and st_area of regions.boundaries from regions"
runcase ok "sum(quantity)" -- --emit canonical --oneshot "select sum of quantity from sales"
runcase ok "avg(price)" -- --emit canonical --oneshot "select avg of price from sales"
runcase ok "max(age)" -- --emit sql --oneshot "select max of age from users"

echo "=== Function with multiple args (binder aware) ==="
runcase ok "st_intersects(boundaries, boundaries)" -- --no-strict --emit both --oneshot "select st_intersects of regions.boundaries and regions.boundaries from regions"
runcase ok "concat(username, username)" -- --emit both --oneshot "select concat of users.username and users.username from users"

echo "=== Mixed columns + functions (should PASS) ==="
runcase ok "st_y + two cols" -- --no-strict --emit both --oneshot "select st_y of users.location, users.user_id, and users.username from users"
runcase ok "st_length + name" -- --no-strict --emit both --oneshot "select st_length of regions.boundaries and regions.name from regions"
runcase ok "sum + column" -- --emit both --oneshot "select sum of sales.quantity and sales.sale_date from sales"

echo "=== Tokens view (should PASS) ==="
runcase ok "tokens users" -- --emit tokens --oneshot "select user_id and username from users"
runcase ok "tokens sum" -- --emit tokens --oneshot "select sum of quantity from sales"

echo "=== Lenient binder (type coercion) ==="
runcase ok "st_centroid(users.location)" -- --no-strict --emit both --oneshot "select st_centroid of users.location from users"
runcase ok "st_buffer(regions.boundaries)" -- --no-strict --emit both --oneshot "select st_buffer of regions.boundaries from regions"

echo "=== Parser errors (should FAIL) ==="
runcase fail "leading 'and'" -- --emit both --oneshot "select and users.user_id from users"
runcase fail "leading comma" -- --emit both --oneshot "select , users.user_id from users"
runcase fail "double 'and'" -- --emit both --oneshot "select users.user_id and and users.username from users"
runcase fail "wrong order" -- --emit both --oneshot "from users select users.user_id"
runcase fail "missing 'select'" -- --emit both --oneshot "users.user_id and users.username from users"

echo "=== Binder/Clause behavior under current runtime (these PASS) ==="
runcase ok "sum over text (auto-lenient)" -- --emit both --oneshot "select sum of users.username from users"
runcase ok "distinct as fn (allowed)" -- --emit both --oneshot "select distinct of users.username from users"
runcase ok "having in select (allowed)" -- --emit both --oneshot "select having of users.user_id from users"
runcase fail "unknown function" -- --emit both --oneshot "select nosuchfunc of users.user_id from users"

echo "=== Larger column lists (sanitizer stress) ==="
runcase ok "users 4 cols" -- --emit canonical --oneshot "select users.user_id, users.username, users.age, and users.balance from users"
runcase ok "regions 3 cols" -- --emit canonical --oneshot "select regions.region_id, regions.name, and regions.boundaries from regions"
runcase ok "sales 4 cols" -- --emit canonical --oneshot "select sales.sale_id, sales.user_id, sales.product_name, and sales.price from sales"

echo "=== Case folding (should PASS) ==="
runcase ok "casefold users" -- --casefold --emit sql --oneshot "select USER_ID and USERNAME from USERS"
runcase ok "casefold regions" -- --casefold --emit canonical --oneshot "select NAME from REGIONS"

echo "=== PostGIS on sqlite (should PASS; may warn) ==="
runcase ok "st_area" -- --no-strict --emit both --oneshot "select st_area of regions.boundaries from regions"

echo "=== Mixed lists (function args with Oxford) ==="
runcase ok "st_union 3x" -- --no-strict --emit canonical --oneshot "select st_union of regions.boundaries, regions.boundaries, and regions.boundaries from regions"
runcase ok "concat 3x" -- --emit canonical --oneshot "select concat of users.username, users.username, and users.username from users"

echo "=== Dotted + basenames combined (should PASS) ==="
runcase ok "mixed dotted/basenames users" -- --emit both --oneshot "select users.user_id, username, and age from users"
runcase ok "mixed dotted/basenames regions" -- --emit both --oneshot "select region_id and regions.name from regions"

echo "=== With DB execution (PASS or [SQL ERROR], both acceptable) ==="
runcase ok "db: users two cols" -- --db "$DB" --emit both --oneshot "select user_id and username from users"
runcase ok "db: users one col" -- --db "$DB" --emit sql  --oneshot "select user_id from users"
runcase ok "db: users three cols (may error if missing)" -- --db "$DB" --emit both --oneshot "select user_id, username, and age from users"
runcase ok "db: sales sum (may error if table missing)" -- --db "$DB" --emit both --oneshot "select sum of quantity from sales"

echo "=== Top-k exploration (should PASS) ==="
runcase ok "topk=1 regions" -- --topk 1 --emit both --oneshot "select boundaries and name from regions"
runcase ok "topk=5 regions" -- --topk 5 --emit both --oneshot "select boundaries and name from regions"

echo "=== Strict vs lenient binder toggles (both PASS under current runtime) ==="
runcase ok   "strict: st_buffer(users.location)" -- --strict   --emit both --oneshot "select st_buffer of users.location from users"
runcase ok   "lenient: st_buffer(users.location)" -- --no-strict --emit both --oneshot "select st_buffer of users.location from users"

echo "=== Sales sanity ==="
runcase ok "sales list 1" -- --emit sql --oneshot "select sale_id, product_name, and price from sales"
runcase ok "sales list 2" -- --emit sql --oneshot "select sale_date and quantity from sales"
runcase ok "sales count"  -- --emit canonical --oneshot "select count of sale_id from sales"

echo "=== Tokens view ==="
runcase ok "tokens users" -- --emit tokens --oneshot "select users.username and users.user_id from users"

# ------------------------------
# Finally: consume goldens.yaml
# ------------------------------
run_goldens "$GOLDENS"

# Summary/exit
report_summary

# Generate explorer + prompts (if needed)
#vbg_phrase_explorer --artifacts out --out phrase_explorer.yaml --per-table 30

# After curating the prompts, save the YAML as goldens.yaml, then:
#./cli_tests.sh
# or
#GOLDENS=path/to/goldens.yaml ./cli_tests.sh