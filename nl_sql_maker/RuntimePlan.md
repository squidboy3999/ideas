## 1) Artifact loading

### Functions

* **`load_artifacts(artifacts_dir)`**

  * **Inputs:** path containing `h_graph_with_artifacts.yaml`, `h_vocabulary.yaml`, `h_binder.yaml`, `h_grammar.lark`.
  * **Behavior:** loads all four; returns `(graph, vocabulary, binder_artifact, grammar_text)`.
  * **Validation:**

    * `graph` must be a dict and include `_artifacts` (but runtime prefers the top-level graph topology).
    * `vocabulary` must contain `deterministic_aliases` and `non_deterministic_aliases` (possibly empty).
    * `binder_artifact.catalogs` must include `columns`, `functions`, `connectors`.
    * `grammar_text` must be a non-empty string.
* **`validate_artifacts(graph, vocabulary, binder_artifact, grammar_text)`**

  * **Behavior:** raises clear errors if any required shape is missing (e.g., no connectors).
  * **Side effects:** none.

### Tests

* Happy path: load from a real `out/` directory produced by Phase-H.
* Failure cases: missing file, empty file, malformed YAML; assert the error messages are actionable.
* Spot-check that `binder_artifact.catalogs.connectors` includes `"AND"`, `"COMMA"`, `"OF"`, `"FROM"` (whatever your Phase-F/H emit).

---

## 2) Normalization wrapper

### Functions

* **`sanitize_list_connectors(text)`**

  * **Inputs:** a **canonical-shaped** string.
  * **Behavior:** rewrites Oxford/AND lists into comma lists:

    * `A and B` → `A, B`
    * `A, B, and C` → `A, B, C`
    * Works inside function `of …` argument spans as well.
  * **Notes:** doesn’t touch `of/from`.
* **`normalize_nl(vocabulary, nl_text, *, case_insensitive=False, cap_results=50)`**

  * **Behavior:** calls your existing `normalize_text`, then applies `sanitize_list_connectors` to each candidate; de-dups; returns `candidates` and a small `stats` blob (`sanitized_count`, `raw_candidates`).
  * **Edge handling:** if zero candidates, return empty list; never raise.

### Tests

* NL “`boundaries and name from regions`” → candidates contain `select regions.boundaries, regions.name from regions`.
* NL with Oxford list → sanitized commas appear in outputs.
* Basename → dotted resolution works (thanks to Phase-H unique basenames in vocabulary).
* Case-folding optionality: same results with `case_insensitive=True`.

---

## 3) Parser construction

### Functions

* **`make_parser(grammar_text)`**

  * **Behavior:** builds a Lark parser with `start="query"`.
  * **Validation:** raises a clear error if `grammar_text` doesn’t compile.
* **`parse_canonical(parser, canonical_text)`**

  * **Behavior:** attempts to parse; returns a boolean or raises (choose one contract and stick to it).

### Tests

* Compiles from your `h_grammar.lark`.
* Parses valid: `select users.user_id, users.username from users`.
* Rejects invalid: `select and users.user_id from users`.

---

## 4) Binder adapter (runtime view)

### Functions

* **`make_runtime_binder(graph, binder_artifact, *, strict=True)`**

  * **Behavior:** constructs the **view** your `CanonicalBinder` expects:

    * Start from the **graph topology** (entity\_type nodes).
    * Inject **connectors** from `binder_artifact.catalogs.connectors` into both:

      * `view["_policy"]["connectors"] = {UPPER/lower variants → surface}`
      * `view["_binder_meta"]["connectors"] = [{"name": K, "surface": V}, …]`
    * Instantiate `CanonicalBinder(view, strict_types=strict, coerce_types=not strict, allow_ordering_funcs_in_args=False)`.
  * **Return:** the binder instance.

### Tests

* Binds simple column list with commas.
* Binds Oxford list **only** when preceded by the sanitizer (you can test the full path in Step 6/7).
* Binds `fn of column` based on binder metadata; fails cleanly for clause/ordering “functions”.

---

## 5) SQL emitter (canonical bound → SQL)

### Functions

* **`emit_select(bound, *, binder_artifact, engine="sqlite") -> str`**

  * **Behavior (MVP):**

    * FROM: one table → emit quoted identifier (e.g., `"table"`).
    * SELECT items:

      * Column: emit `"table"."column"`.
      * Function: if `binder_artifact.catalogs.functions[fn]['template']` exists, fill `{column}`, `{columns}`, `{table}`, `{value}`; else default to `FN(arg1, arg2, …)`.
    * Quoting policy per engine (`sqlite`: double quotes).
    * **Warnings (non-throwing):** if engine is `sqlite` and function looks PostGIS (`st_`), annotate/return a warning string for the CLI to show.
* **Helpers**

  * `quote_ident(ident, engine)`
  * `format_fn_call(fn, args, binder_meta, engine)`

### Tests

* One column → `SELECT "users"."user_id" FROM "users";`
* Two/three columns with Oxford sanitized → proper comma list.
* Function with one arg → `FN("t"."c")`.
* If template exists (e.g., `sum`), use it; if multiple args, fill `{columns}` list.
* PostGIS on sqlite: SQL still generated and a warning flag is surfaced.

---

## 6) Database execution (optional)

### Functions

* **`run_sqlite(db_path, sql, *, limit=None, timeout=30)`**

  * **Behavior:** opens SQLite connection, runs SQL, fetches rows (optionally limited), returns `(columns, rows)`.
  * **Safety:** parameterize only when values are needed; here you’re emitting SELECTs only, so direct execution is fine.
  * **Errors:** catch database errors and return an error object/string (don’t crash the REPL).

### Tests

* Create a small temp DB with a `users` table and sample rows; run a select and verify returned rows.
* Bad SQL → returns an error result without raising.

---

## 7) One-shot runtime pipeline (core engine)

### Functions

* **`nl2sql_once(nl_text, *, graph, vocabulary, binder_artifact, parser, engine="sqlite", topk=5)`**

  * **Behavior:**

    1. Normalize NL → canonical candidates (with sanitizer).
    2. For each candidate (up to `topk`):

       * Bind with runtime binder.
       * Serialize (for roundtrip inspection).
       * Parse canonical to ensure it matches grammar.
       * Emit SQL via `emit_select`.
       * Return first success along with diagnostics.
    3. If none succeed: return a structured failure (`normalizer_zero`, `binder_fail`, or `parser_fail` with collected reasons).
  * **Outputs:** result dict containing: chosen canonical, serialized canonical, SQL, any warnings, debug stats (candidates considered, which step failed).

### Tests

* Golden NL queries from Phase-J → success end-to-end.
* NL with Oxford list → sanitized and bound; SQL returned.
* NL producing function call → SQL with function call or templated form.
* Failure surfaces: intentionally ask for a non-existent column → `binder_fail` with helpful message.

---

## 8) CLI interface

### Functions

* **`parse_args(argv)`**

  * `--artifacts DIR` (required), `--db PATH`, `--engine sqlite|postgres`, `--topk N`, `--emit canonical|sql|both|tokens`, `--debug`, `--oneshot "text"`.
* **`repl(ctx)`**

  * Loop reading NL lines; supports meta-commands:

    * `:mode sql|canonical|both`
    * `:topk N`
    * `:db path`
    * `:engine sqlite|postgres`
    * `:tokens` (show `canon_tokenize` of the winning canonical)
    * `:ast` (pretty-print bound structure if you expose it)
    * `:quit`
* **`run_command(ctx, line)`**

  * Parses meta-commands vs NL; calls `nl2sql_once` and prints results (and executes against `--db` if provided).

### Tests

* Arg parsing: verify defaults and required flags.
* REPL: simulate a few lines through a command handler (no subprocess needed) and assert printed outputs contain canonical/SQL pieces.
* One-shot mode: call `--oneshot` with a sample NL and assert the SQL is printed/returned.

---

## 9) Diagnostics & observability

### Functions

* **`format_result(result, emit_mode)`**

  * Formats the runtime result for console output: show canonical, SQL, topk stats, and any warnings.
* **`format_error(error_result)`**

  * Human-readable errors with the right “bucket” (`normalizer_zero`, `binder_fail`, `parser_fail`) plus the most informative detail.

### Tests

* Ensure that when sanitation changed a candidate, a small note is visible in debug mode (e.g., “lists sanitized” count).
* Ensure binder/parse errors are not leaked as raw stack traces; they’re summarized.

---

## 10) Configuration & policy switches

### Functions

* **`RuntimeConfig`** (simple struct/dataclass)

  * `engine`, `topk`, `case_insensitive`, `strict_binding` (toggle binder flags), `execute_sql`, `limit_rows`.
* **`apply_config_defaults(config, artifacts)`**

  * For example, default `engine="sqlite"` if `--db` ends with `.db`.

### Tests

* Switching `strict_binding=False` allows more permissive runs but still parses.
* Setting `topk=1` vs `topk=5`—observe candidate exploration differs.

---

## 11) End-to-end runtime acceptance tests

### Scenarios

* **Columns only:** “`name and boundaries from regions`” → canonical & SQL OK, DB returns rows.
* **Function + column:** “`area of boundaries and name from regions`” → SQL emitted; if engine=sqlite, warning about PostGIS.
* **Basename use:** “`price and sale_date from sales`” → resolves via unique basenames to dotted canonicals and works.
* **Oxford list:** “`user_id, username, and is_active from users`” → OK.
* **Out-of-scope:** “`group by name`” → clean failure (unless/when grammar & binder grow to support it).

---

## 12) Future-proof notes (not required now)

* If/when you extend grammar to WHERE/ORDER BY, keep the **sanitizer** minimal and specific to list separators; avoid altering semantics.
* Consider caching:

  * Parsed grammar object (`Lark`) once per process.
  * A tiny LRU over `(nl_text → last good canonical/sql)` for REPL feel.
* For Postgres: add identifier quoting with double-quotes (same as SQLite) and pass-through PostGIS functions (no warning).

---

### Mapping to your existing code (reuse checklist)

* **Normalizer:** `normalize_text` (unchanged).
* **List sanitizer:** reuse your Phase-I/Phase-J sanitizer logic as a single runtime function.
* **Parser builder:** reuse `j_make_parser` interface.
* **Binder view:** adapt `i_make_relaxed_binder` to **strict** runtime settings and connector injection.
* **Template placeholder fill:** reuse the placeholder identification logic from Phase-I feasibility as the source of truth for `{column}/{columns}/{table}/{value}`.


