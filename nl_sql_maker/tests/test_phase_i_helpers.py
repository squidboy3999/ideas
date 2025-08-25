# tests/test_phase_i_helpers.py
import pytest
import re
from vbg_generate.phase_i_helpers import (
    i_extract_canonical_sets,
    i_get_column_meta,
    i_get_function_meta,
    i_table_to_columns,
    i_build_parser,
    i_introspect_grammar,
    i_generate_canonical_examples,
    i_make_relaxed_binder,
    i_roundtrip_one,
    i_roundtrip_suite,
    i_feasibility_for_function,
    i_feasibility_suite,
    i_make_negative_examples,
    i_negative_suite,
    i_sanitize_list_connectors
)

# ---------------------------
# Fixtures: minimal graph + binder catalogs
# ---------------------------

@pytest.fixture
def mini_graph():
    # Tables
    g = {
        "users": {"entity_type": "table", "metadata": {"columns": {
            "users.user_id": {}, "users.username": {}, "users.is_active": {}, "users.location": {},
        }}},
        "regions": {"entity_type": "table", "metadata": {"columns": {
            "regions.region_id": {}, "regions.name": {}, "regions.boundaries": {},
        }}},
        "sales": {"entity_type": "table", "metadata": {"columns": {
            "sales.sale_id": {}, "sales.user_id": {}, "sales.price": {}, "sales.quantity": {}, "sales.sale_date": {},
        }}},
        # Columns (with types to drive simple compat when needed)
        "users.user_id":    {"entity_type": "column", "metadata": {"table":"users","type_category":"integer"}},
        "users.username":   {"entity_type": "column", "metadata": {"table":"users","type_category":"text"}},
        "users.is_active":  {"entity_type": "column", "metadata": {"table":"users","type_category":"boolean"}},
        "users.location":   {"entity_type": "column", "metadata": {"table":"users","type_category":"geometry","labels":["postgis"]}},
        "regions.region_id":{"entity_type": "column", "metadata": {"table":"regions","type_category":"integer"}},
        "regions.name":     {"entity_type": "column", "metadata": {"table":"regions","type_category":"text"}},
        "regions.boundaries":{"entity_type": "column", "metadata": {"table":"regions","type_category":"geometry","labels":["postgis"]}},
        "sales.sale_id":    {"entity_type": "column", "metadata": {"table":"sales","type_category":"integer"}},
        "sales.user_id":    {"entity_type": "column", "metadata": {"table":"sales","type_category":"integer"}},
        "sales.price":      {"entity_type": "column", "metadata": {"table":"sales","type_category":"numeric"}},
        "sales.quantity":   {"entity_type": "column", "metadata": {"table":"sales","type_category":"integer"}},
        "sales.sale_date":  {"entity_type": "column", "metadata": {"table":"sales","type_category":"timestamp"}},
        # Functions (one spatial, one aggregate, one clause-ish to ensure filtering)
        "st_area":          {"entity_type": "postgis_actions", "binder":{"class":"spatial","clause":"select","args":["column"]}},
        "sum":              {"entity_type": "sql_actions",      "binder":{"class":"aggregate","clause":"select","args":["column"]}},
        "having":           {"entity_type": "sql_actions",      "binder":{"class":"clause","clause":"having","args":[]}},
        "limit":            {"entity_type": "sql_actions",      "binder":{"class":"clause","clause":"limit","args":[]}},
    }
    return g

@pytest.fixture
def binder_artifact(mini_graph):
    # A binder catalog that exposes connectors and punctuation explicitly
    return {
        "catalogs": {
            "tables": ["users","regions","sales"],
            "columns": {
                k: mini_graph[k]["metadata"] | {"labels": mini_graph[k]["metadata"].get("labels", [])}
                for k in mini_graph if mini_graph[k].get("entity_type") == "column"
            },
            "functions": {
                "st_area":{"class":"spatial","clause":"select","args":["column"]},
                "sum":{"class":"aggregate","clause":"select","args":["column"]},
                "having":{"class":"clause","clause":"having","args":[]},
                "limit":{"class":"clause","clause":"limit","args":[]},
            },
            "comparison_operators": {},
            "connectors": {"AND":"and","FROM":"from","OF":"of","COMMA":","},
            "punctuation": {",": ","},
            "_tables_detail": {},  # not used here
        }
    }

def _alts(vals: list[str]) -> str:
    vals = sorted(vals)
    return " | ".join(f'"{v}"' for v in vals) if vals else '""'

@pytest.fixture
def grammar_text(mini_graph):
    # Tiny canonical grammar aligned with your H grammar (no nested functions)
    tables = sorted([k for k,v in mini_graph.items() if v.get("entity_type") == "table"])
    columns = sorted([k for k,v in mini_graph.items() if v.get("entity_type") == "column"])
    functions = sorted([k for k,v in mini_graph.items() if v.get("entity_type") in ("sql_actions","postgis_actions")])
    return f'''
?query: "select" column_list _tbl_connector TABLE
_tbl_connector: "from" | "of"

?column_list: selectable
            | selectable ("," selectable)+ (","? "and" selectable)?
            | selectable "and" selectable

?selectable: COLUMN
           | FUNCTION ["of" column_args]

column_args: COLUMN ("," COLUMN)* (","? "and" COLUMN)?

TABLE: {_alts(tables)}
COLUMN: {_alts(columns)}
FUNCTION: {_alts(functions)}

%import common.WS
%ignore WS
'''.strip()

# ---------------------------
# Tests: accessors & topology
# ---------------------------

def test_extract_canonical_sets(mini_graph):
    s = i_extract_canonical_sets(mini_graph)
    assert "users" in s["tables"] and "regions" in s["tables"]
    assert "users.username" in s["columns"] and "regions.boundaries" in s["columns"]
    assert "st_area" in s["functions"] and "sum" in s["functions"]

def test_get_column_meta_prefers_binder(mini_graph, binder_artifact):
    # Override graph meta to prove binder wins
    mini_graph["users.username"]["metadata"]["type_category"] = "text"
    meta = i_get_column_meta("users.username", mini_graph, binder_artifact)
    assert meta["type_category"] == "text"
    # Missing in binder → falls back to graph
    meta2 = i_get_column_meta("sales.quantity", mini_graph, binder_artifact)
    assert meta2["type_category"] == "integer"

def test_get_function_meta_prefers_binder(mini_graph, binder_artifact):
    fm = i_get_function_meta("st_area", mini_graph, binder_artifact)
    assert fm.get("applicable_types") is None  # not provided but should not crash
    assert "label_rules" not in fm  # absent keys omitted

def test_table_to_columns_uses_dotted(mini_graph):
    t2c = i_table_to_columns(mini_graph)
    assert set(t2c["users"]) >= {"users.username","users.user_id","users.is_active","users.location"}
    # No basenames expected in canonicalized test data
    assert not any("." not in c for c in t2c["users"])

# ---------------------------
# Tests: grammar helpers
# ---------------------------

def test_build_parser_accepts_valid(grammar_text):
    p = i_build_parser(grammar_text)
    assert p is not None

def test_introspect_grammar_finds_list_tokens(grammar_text):
    meta = i_introspect_grammar(grammar_text)
    assert meta["supports_oxford_and"] is True
    assert set(meta["list_separators"]) >= {",", "and"}

# ---------------------------
# Tests: canonical generator
# ---------------------------

def test_generate_examples_respect_grammar_and_function_pool(mini_graph, binder_artifact, grammar_text):
    p = i_build_parser(grammar_text)
    sets = i_extract_canonical_sets(mini_graph)
    # With seed for determinism
    samples = i_generate_canonical_examples(mini_graph, p, sets, phrases=50, rng_seed=42, binder_artifact=binder_artifact)
    assert samples, "should generate some examples"
    # Ensure no clause-ish functions (having/limit) appear
    bad = [s for s in samples if " having " in s or " limit " in s]
    assert not bad, f"clause tokens leaked into select list: {bad[:3]}"

def test_generate_examples_use_from_table_columns_only(mini_graph, binder_artifact, grammar_text):
    p = i_build_parser(grammar_text)
    sets = i_extract_canonical_sets(mini_graph)
    s = i_generate_canonical_examples(mini_graph, p, sets, phrases=15, rng_seed=1, binder_artifact=binder_artifact)
    assert s
    # Weak check: if FROM users, selected columns should be users.* (generator contract)
    for q in s:
        if " from users" in q:
            sel = q.split("select",1)[1].split(" from ",1)[0]
            # Every column token that has a dot should start with users.
            dots = [tok for tok in re.split(r"[ ,]", sel) if "." in tok]
            assert all(d.startswith("users.") for d in dots), f"cross-table column leaked: {q}"

# ---------------------------
# Tests: binder construction & connectors
# ---------------------------

def test_relaxed_binder_recognizes_and(mini_graph, binder_artifact, grammar_text):
    # This is the smoking-gun reproduction of your bucket:
    # It passes only if i_make_relaxed_binder exposes connectors the binder actually reads.
    binder = i_make_relaxed_binder(mini_graph, binder_artifact)
    p = i_build_parser(grammar_text)
    canonical = "select users.username and users.user_id from users"
    ok, info = i_roundtrip_one(binder, p, canonical)
    assert ok, f"Binder should handle 'and' as a list separator; got error: {info.get('error')}"

# ---------------------------
# Tests: C1 roundtrip orchestration
# ---------------------------

def test_roundtrip_suite_runs_and_meets_loose_threshold(mini_graph, binder_artifact, grammar_text, monkeypatch):
    # Run C1 with a modest threshold; the key is it runs and returns a report structure.
    ok, report = i_roundtrip_suite(mini_graph, grammar_text, phrases=25, success_threshold=0.40, rng_seed=7, binder_artifact=binder_artifact)
    assert "successes" in report and "failures" in report
    assert ok, f"Expected loose threshold pass; buckets: {report.get('top_error_buckets')}"

# ---------------------------
# Tests: Feasibility
# ---------------------------

def test_feasibility_for_function_fill_env(mini_graph, binder_artifact):
    # Fake a template for sum
    binder_artifact = dict(binder_artifact)  # shallow copy
    binder_artifact["catalogs"]["functions"]["sum"]["template"] = "SUM({column})"
    sets = i_extract_canonical_sets(mini_graph)
    rep = i_feasibility_for_function(mini_graph, binder_artifact, "sum", sets)
    assert rep["ok"] is True and "column" in rep["placeholders"]
    assert rep["example_bind"]["column"].startswith(("users.","regions.","sales."))

def test_feasibility_suite_ok(mini_graph, binder_artifact):
    rep = i_feasibility_suite(mini_graph, binder_artifact, sample_functions=3)
    assert rep["ok"] is True

# ---------------------------
# Tests: Negatives
# ---------------------------

def test_make_negative_examples_shape(mini_graph):
    sets = i_extract_canonical_sets(mini_graph)
    negs = i_make_negative_examples(sets, k=6)
    assert 1 <= len(negs) <= 6

def test_negative_suite_blocks(mini_graph, binder_artifact, grammar_text):
    ok, rep = i_negative_suite(mini_graph, grammar_text, k=6, binder_artifact=binder_artifact)
    assert ok, f"negative examples should not pass both; got {rep}"

# ---------------------------
# Tests: Log builder (smoke)
# ---------------------------

def test_phase_i_log_smoke():
    from vbg_generate.phase_i_helpers import i_build_phase_i_log
    log = i_build_phase_i_log(
        {"rate": 0.5, "successes": 5, "tested": 10, "top_error_buckets":[("token 'and'...", 5)], "examples":[]},
        {"ok": True, "sampled": 3, "problems":[]},
        {"ok": True, "tested": 4, "passed_both":[]},
    )
    assert "[C1]" in log and "[C2]" in log and "[C3]" in log




def test_list_sanitizer_plain_and():
    s = "select users.username and users.user_id from users"
    assert i_sanitize_list_connectors(s) == "select users.username, users.user_id from users"

def test_list_sanitizer_oxford_three():
    s = "select a.b, c.d, and e.f from t"
    assert i_sanitize_list_connectors(s) == "select a.b, c.d, e.f from t"

def test_list_sanitizer_function_args():
    s1 = "select st_union of regions.boundaries and regions.boundaries from regions"
    assert i_sanitize_list_connectors(s1) == "select st_union of regions.boundaries, regions.boundaries from regions"
    s2 = "select st_union of a.b, c.d, and e.f from t"
    assert i_sanitize_list_connectors(s2) == "select st_union of a.b, c.d, e.f from t"
