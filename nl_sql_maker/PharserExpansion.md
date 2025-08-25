# 1) Human-readable “Phrase Explorer” (audit + curation)

## Goals

* Generate natural-sounding NL variants your system can already handle.
* Show exactly which ones bind/parse/emit, and why failures happen.
* Let you cherry-pick “goldens” for regression tests.

## Inputs

* `h_graph_with_artifacts.yaml` (tables/columns).
* `h_binder.yaml` (functions, args, templates, connectors).
* `h_vocabulary.yaml` (aliases, deterministic/non-det).
* Grammar text.

## Generation strategy

* **Column lists:** for each table with N columns, sample 1–3 column names; produce variants:

  * “A and B”, “A, B, and C”, “A, B, C”, “A & B”, “A plus B”, “A with B”.
* **Function invocations:** for each select-friendly function:

  * Arg arity patterns: `of <col>`, `of <col> and <col>`, Oxford lists.
  * Paraphrases around `of` (to probe normalizer): “on”, “using”, “from”, “over”.
* **Basename resolution:** same as above but using plain basenames where unique.
* **Light grammar paraphrases:** “show”, “give me”, “list” → canonical “select”.
* **Noise toggles:** honor/omit Oxford; mix commas/and; insert harmless preambles
  (“please”, “now”, “for table users”).

## Scoring & display

* For each generated NL:

  * Normalized canonicals (pre/post sanitizer).
  * Bind result (ok/fail; binder message).
  * Parse result (ok/fail; parser message).
  * Emitted SQL (or why not).
* Output formats:

  * **CSV** (one row per NL; easy to sort).
  * **Markdown** report with grouped sections (table, function).
  * **Mini HTML** with filters (pass/fail, function, table).

## Curation workflow

* Filter to **Pass** and skim for naturalness.
* Promote interesting ones into a **golden set** (new test file).
* Save a **“near-miss” set** (consistent failures worth fixing later).

---

# 2) Safe, iterative vocabulary/bindings expansion (LLM-assisted)

## Seed specification

* Treat each entry in `keywords_and_functions.yaml` as a **contract**:

  * **canonical pattern** (e.g., `sum of {column}`).
  * **current aliases** (e.g., “sum of”, “total of”).
  * **explanations** (semantics, arg types).
  * **constraints** (select-only, not a clause; arg type rules).
  * **negative examples** (phrases that *should not* map).

## Candidate generation loop (per function/keyword)

* Prompt the LLM to propose **new surface forms** under constraints:

  * Avoid function names that collide with table/column names.
  * Prefer short, idiomatic phrases; avoid ambiguous prepositions unless tested.
  * Include multi-token verb phrases (“compute the sum of”, “aggregate by”).
* Ask for **structured output**: list of aliases + short justification.

## Automatic gating before any promotion

1. **Canonicalization simulation**
   Map each proposed alias to the canonical pattern (e.g., replace “sum of” with `sum of {column}`) and produce NL variants with real columns.
2. **Full runtime checks** (your `nl2sql_once`, strict and lenient):

   * must **normalize** (sanitizer ok, candidate produced),
   * must **bind** (types compat),
   * must **parse**,
   * must **emit SQL**.
3. **Collision checks**

   * Does alias conflict with existing function or connector strings?
   * Does it spuriously map to *other* functions on random inputs?
4. **Ambiguity / leakage checks**

   * Test alias over multiple tables/columns to ensure it doesn’t drift semantically.
5. **Stability threshold**

   * Require **N/NRUN ≥ threshold** pass rate (e.g., 90% across a matrix of tables/columns).

## Promotion mechanics

* Maintain a **quarantine file** `candidates_pending.yaml` with:

  * `proposed_alias`, `target_function/pattern`, `llm_reason`, `autotest_stats`.
* Batch mode: pop the oldest K candidates; run the gate; write results:

  * **Accepted** → appended to `keywords_and_functions.yaml`.
  * **Rejected** → appended to `failed_aliases.yaml` with reasons (for future prompt tuning).
* Every promotion bumps an artifacts **minor version**.

## Safety rails

* **No canonical changes** unless explicitly approved (avoid shifting core structure).
* Never change **connectors** via LLM (and/or grammar keywords).
* Keep **deterministic\_aliases** vs **non\_deterministic\_aliases** separated; the latter require higher acceptance thresholds.

---

# 3) External “Normalizer-X” layer (robustness fuzzing & rewrites)

## Purpose

Absorb variations that aren’t worth codifying as first-class aliases but that are common in user queries.

## Techniques (stacked, with scoring)

* **Regex rewrites** (pre-normalization):

  * Coordinations: “plus”, “&”, “as well as” → `and`.
  * Prepositions: “with/using/on/over” → `of` (only when followed by a columnish token).
  * Noise drop: “please”, “now”, “a list of”, “the”.
* **Spelling/typo toleration**:

  * Single-edit distance for column names (bounded to unique near-match).
  * Common keyboard swaps; diacritic stripping.
* **Morphology/lemmatization**:

  * “names” → “name”, “prices” → “price” (only when a singular exists in schema).
* **Unit/number normalizations** (future, for WHEREs).
* **Punctuation tolerance**:

  * Extra commas, stray periods around select/from.
* **Constituent relaxations**:

  * “show users with user\_id and username” → “select user\_id and username from users”.

## Fuzzer harness

* For each curated golden NL, generate perturbations:

  * Insert/remove Oxford comma; swap connectors; add light noise.
  * Case folding; random extra whitespace; benign stopwords.
  * One typo per content token.
* Feed all through `nl2sql_once` and track: **pre→post** mapping, errors, and recovered successes.

## Promotion path

* If a rewrite consistently helps across many samples (and never hurts), graduate it from “Normalizer-X” to **first-class** vocab alias or canonical pre-rewrite.

---

# 4) CI gates & metrics

## New pipeline stages

* **L: Phrase Explorer audit**
  Build the CSV/MD/HTML reports; fail the job if pass-rate drops below baseline.
* **M: Candidate Alias Replay**
  Run quarantine candidates; fail if acceptance < threshold or collisions detected.
* **N: Normalizer-X fuzzing**
  Run fuzz suite over goldens; alert on regressions (pass-to-fail).

## Key metrics

* **Coverage**: % of (table × function × arity) combos with ≥1 passing NL phrase.
* **Precision proxy**: accepted aliases with zero collisions across random schemas.
* **Robustness**: golden pass-rate under fuzzing (target ≥ 95%).
* **Drift detection**: compare emitted SQL deltas for unchanged goldens across runs.

---

# 5) Versioning & governance

* Version **artifacts** (H outputs) and **vocabulary** separately:

  * `keywords_and_functions.yaml`: semver minor on alias additions; major on canonical changes.
* Keep a **CHANGELOG.md** with:

  * Added/removed aliases; rationale; links to reports.
* Maintain a **Revert plan**: one click to roll vocab version back in the runtime CLI.

---

# 6) Human-in-the-loop curation UX (lightweight)

* A small HTML report with:

  * Filters: table, function, pass/fail, alias source (seed/LLM/fuzzer).
  * Checkbox to **promote** to goldens or to **reject** (writes to quarantine outcomes).
  * One-click “open in CLI” suggestion (pre-filled command).
* Export curated selections to `tests/test_runtime_goldens.py` (auto-generated).

---

# 7) Roll-out plan (incremental)

1. **Phrase Explorer** (report only)

   * Generate and review; promote a first batch of goldens.
2. **LLM expansion (offline)**

   * Seed with 3–5 functions; test the gating logic; calibrate thresholds.
3. **Normalizer-X v1**

   * Add a few high-value rewrites (and/plus/&; minor stopwords).
4. **CI integration**

   * Wire L, M, N stages; establish baselines; enable alerts, not fails.
5. **Promotion loops**

   * Batch-accept winners weekly; keep quarantine fresh.
6. **Widen scope**

   * Add more functions; try cross-table structures (later) with WHERE/joins.

---

# 8) Risks & mitigations

* **Alias collisions** (phrase matches the wrong thing)
  → Collision tests across randomized schemas; require zero collision before acceptance.
* **Over-normalization** (rewrites that change meaning)
  → Keep Normalizer-X rules conservative; require positive impact across multiple goldens; add “never rewrite after `from …`” guards, etc.
* **LLM hallucinations**
  → Hard gate: bind/parse/emit must pass across a matrix; manual spot checks in Explorer.
* **Reproducibility**
  → Pin seeds, persist candidate sets and decisions in commits.

---

# 9) End Goal

* A browsable gallery of **natural phrases** your system supports today.
* A **safe loop** to harvest hundreds of new aliases/patterns with high precision.
* A robustness layer that quietly shepherds messy input into your **solvable space**.
* Fresh **golden tests** derived from real (and realistic) language, not just synthetic strings.

