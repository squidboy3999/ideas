# tests/test_cypher_helper.py
import re
import json
import hashlib
import types
import pytest

from vbg_tools.cypher_helper import (
    tokenize, extract_slots, norm_type_name, looks_like_id, norm_rule_id,
    is_primitive, sanitize_value, sanitize_props, to_alias_list, _prepare_connectors_from_keywords,
    synth_vocabulary, synth_binder, synth_grammar
)

# ---------------------------
# Pure helper tests
# ---------------------------

def test_tokenize_basic():
    assert tokenize("Show me, users_2024") == ["show", "me", ",", "users_2024"]
    assert tokenize("") == []
    assert tokenize("   ") == []

def test_extract_slots():
    s = "select {columns} from {table} where {col} > {value}"
    assert extract_slots(s) == ["columns", "table", "col", "value"]

def test_norm_type_name():
    assert norm_type_name("Var Char") == "var_char"
    assert norm_type_name("  NUMERIC ") == "numeric"

def test_looks_like_id():
    assert looks_like_id("id", [])
    assert looks_like_id("user_id", [])
    assert looks_like_id("user", ["ID"])
    assert not looks_like_id("price", [])
    assert not looks_like_id("region_code", ["code"])

def test_norm_rule_id_stable():
    a = norm_rule_id("SELECT  {table}   FROM {table} ")
    b = norm_rule_id("select {table} from {table}")
    assert a == b
    assert len(a) == 16
    assert re.fullmatch(r"[0-9a-f]{16}", a)

def test_is_primitive_and_sanitize_value():
    assert is_primitive(1)
    assert is_primitive(1.2)
    assert is_primitive("x")
    assert is_primitive(True)
    assert is_primitive(None)

    # dict -> JSON
    val = sanitize_value({"a": 1, "b": [2, {"c": 3}]})
    parsed = json.loads(val)
    assert parsed == {"a": 1, "b": [2, {"c": 3}]}

    # list of mixed values (non-primitive becomes JSON)
    val2 = sanitize_value([1, "x", {"y": 2}])
    assert val2[0] == 1 and val2[1] == "x" and json.loads(val2[2]) == {"y": 2}

def test_sanitize_props():
    d = {"a": {"k": "v"}, "b": [1, {"z": 9}], "c": 10}
    sp = sanitize_props(d)
    assert json.loads(sp["a"]) == {"k": "v"}
    assert sp["c"] == 10
    assert isinstance(sp["b"], list) and json.loads(sp["b"][1]) == {"z": 9}

def test_to_alias_list():
    assert to_alias_list({"aliases": ["show", "list"]}) == ["show", "list"]
    assert to_alias_list({"surface": "display"}) == ["display"]
    assert to_alias_list("only") == ["only"]
    assert to_alias_list(["a", "b"]) == ["a", "b"]
    assert to_alias_list({"aliases": None}) == []

def test_prepare_connectors_pref_and_fallback():
    # explicit connectors block
    kw = {"keywords": {"connectors": {"AND": "and", "FROM": "from"}}}
    out = _prepare_connectors_from_keywords(kw)
    assert out["AND"] == "and"
    assert out["FROM"] == "from"
    # fallback derives AND/OR/FROM/OF/COMMA
    kw2 = {"keywords": {
        "logical_operators": {"and": {"aliases": ["and", "plus"]}, "or": {"aliases": ["or"]}},
        "prepositions": {"from": {"aliases":["from"]}, "of": {"aliases":["of"]}}
    }}
    out2 = _prepare_connectors_from_keywords(kw2)
    for k in ("AND","OR","FROM","OF","COMMA"):
        assert k in out2
    assert out2["COMMA"] == ","

# ---------------------------
# Tiny dummy session for synth_* tests (no Neo4j server)
# ---------------------------

class DummyResult:
    def __init__(self, val):
        self._val = val
    def data(self):
        # expect list[dict]
        return self._val
    def value(self):
        # expect list[...] (e.g., [1] or ["x", ...])
        return self._val
    def single(self):
        if isinstance(self._val, list):
            return self._val[0] if self._val else None
        return self._val
    def __iter__(self):
        # not used in our code paths
        return iter([])

class DummyTx:
    def __init__(self, queue):
        self._queue = queue
    def run(self, *args, **kwargs):
        if not self._queue:
            raise AssertionError("DummySession queue exhausted for tx.run()")
        val = self._queue.pop(0)
        return DummyResult(val)

class DummySession:
    """
    Each execute_read(...) consumes exactly one queue item per tx.run() inside the lambda.
    We ignore the query itself; we just feed back the prepared response in order.
    """
    def __init__(self, queue):
        self._queue = list(queue)
    def execute_read(self, fn):
        tx = DummyTx(self._queue)
        return fn(tx)
    def execute_write(self, fn):
        # not used in synth_* tests
        tx = DummyTx(self._queue)
        return fn(tx)

# ---------------------------
# synth_vocabulary tests
# ---------------------------

def test_synth_vocabulary_from_minimal_rows():
    # Queue order matches internal helper call order:
    # 1) _block_by_role("select_verb").data()
    # 2) _block_by_role("comparator").data()
    # 3) _filler_aliases().value()
    # 4) _connector_rows().data()
    # 5) _select_template_row().data()
    # 6) _actions_by_role("sql_action").data()
    # 7) _actions_by_role("postgis_action").data()
    queue = [
        # 1
        [{"canonical": "select", "aliases": ["show", "list", "display"]}],
        # 2
        [{"canonical": "greater_than", "aliases": [">", "more than"]}],
        # 3
        ["the", "a", "all"],
        # 4
        [{"n":"AND","s":"and"}, {"n":"FROM","s":"from"}, {"n":"COMMA","s":","}],
        # 5
        [{"t":"select {columns} from {table}"}],
        # 6
        [{"name":"sum","template":"sum({value})","aliases":["total"],"reqs":[{"arg":"value","st":"numeric"}]}],
        # 7
        [],
    ]
    sess = DummySession(queue)
    vocab = synth_vocabulary(sess)
    assert "keywords" in vocab and "sql_actions" in vocab
    kw = vocab["keywords"]
    assert kw["select_verbs"]["select"]["aliases"] == ["display", "list", "show"]
    assert kw["comparison_operators"]["greater_than"]["aliases"] == [">", "more than"]
    assert kw["filler_words"]["_skip"]["aliases"] == ["a", "all", "the"]
    assert kw["connectors"]["AND"] == "and"
    assert kw["global_templates"]["select_template"] == "select {columns} from {table}"
    assert "sum" in vocab["sql_actions"]
    assert vocab["sql_actions"]["sum"]["applicable_types"] == {"value": ["numeric"]}

# ---------------------------
# synth_binder tests
# ---------------------------

def test_synth_binder_from_minimal_rows():
    # Order:
    # 1) _table_rows().data()
    # 2) _column_rows().data()
    # 3) _function_rows().data()
    # 4) _connector_rows().data()
    queue = [
        # 1
        [{"n":"users"},{"n":"sales"}],
        # 2
        [
            {"table":"users","name":"user_id","fqn":"users.user_id","types":["id"]},
            {"table":"sales","name":"price","fqn":"sales.price","types":["numeric"]},
        ],
        # 3
        [{"name":"sum","template":"sum({value})","reqs":[{"arg":"value","st":"numeric"}]}],
        # 4
        [{"n":"AND","s":"and"}],
    ]
    sess = DummySession(queue)
    binder = synth_binder(sess)
    cat = binder["catalogs"]
    assert "users" in cat["tables"] and "sales" in cat["tables"]
    # columns are keyed by FQN
    assert "users.user_id" in cat["columns"]
    assert cat["columns"]["users.user_id"]["slot_types"] == ["id"]
    assert cat["functions"]["sum"]["arity"] == 1
    assert cat["connectors"]["AND"] == "and"

# ---------------------------
# synth_grammar tests
# ---------------------------

def test_synth_grammar_from_minimal_rows():
    # Order:
    # 1) _has_select().value() -> list with single integer
    # 2) _connector_rows().data()
    # 3) _select_rule_rows().data()
    # 4) _expr_pred_rows().data()
    queue = [
        # 1
        [1],  # has select
        # 2
        [{"n":"FROM","s":"from"}, {"n":"AND","s":"and"}, {"n":"COMMA","s":","}],
        # 3
        [{"t":"select {columns} from {table}"}],
        # 4
        [
            {"nt":"Expression","text":"sum {value}","can":"sum"},
            {"nt":"Predicate","text":"value > {value}","can":"greater_than"},
        ],
    ]
    sess = DummySession(queue)
    grammar = synth_grammar(sess)

    # SELECT terminal and connector terminals are present
    assert 'SELECT: "select"i' in grammar
    assert 'FROM: "from"i' in grammar
    assert 'AND: "and"i' in grammar
    # grammar ignores whitespace
    assert "%ignore WS" in grammar
    # start rule and query
    assert "start: query" in grammar
    # slots are rewritten to nonterminals (COLUMNS/TABLE); template present (may include fallback alt)
    assert "query: SELECT COLUMNS FROM TABLE" in grammar.replace("\n", " ")
    # expression / predicate rules emitted
    assert "expression:" in grammar
    assert "predicate:" in grammar

def test_synth_grammar_fallback_and_injected_terminals():
    # No select canonical, no FROM/COMMA rows, no select template, no expr/pred rules
    # Order: has_select, connectors, select_rule_rows, expr_pred_rows
    queue = [
        [0],     # has_select -> false
        [{"n":"AND","s":"and"}],  # connectors w/o FROM/COMMA
        [],      # no select template
        [],      # no expr/pred rules
    ]
    sess = DummySession(queue)
    grammar = synth_grammar(sess)

    # We ALWAYS provide SELECT terminal so fallback parses
    assert 'SELECT: "select"i' in grammar
    # FROM/COMMA must be injected terminals
    assert 'FROM: "from"i' in grammar
    assert 'COMMA: ","' in grammar

    # Query must fall back to minimal rule
    assert "query: SELECT FROM" in grammar
    # No expression/predicate sections since none exist
    assert grammar.count("expression:") == 0
    assert grammar.count("predicate:") == 0

def test_synth_grammar_aggregates_rules_no_duplicates():
    # Multiple Expression/Predicate rows must aggregate into a single rule each
    # Order: has_select, connectors, select_rule_rows, expr_pred_rows
    queue = [
        [1],
        [{"n":"FROM","s":"from"}, {"n":"COMMA","s":","}],
        [{"t":"select {columns} from {table}"}],
        [
            {"nt":"Expression","text":"sum {value}","can":"sum"},
            {"nt":"Expression","text":"avg {value}","can":"avg"},
            {"nt":"Expression","text":"sum {value}","can":"sum"},  # duplicate body
            {"nt":"Predicate","text":"value > {value}","can":"gt"},
            {"nt":"Predicate","text":"value < {value}","can":"lt"},
            {"nt":"Predicate","text":"value > {value}","can":"gt"},  # duplicate body
        ],
    ]
    sess = DummySession(queue)
    grammar = synth_grammar(sess)

    # Each rule name should be emitted exactly once
    assert grammar.count("\nexpression:") == 1
    assert grammar.count("\npredicate:") == 1

    # And bodies should be alternations combining unique forms
    expr_line = next(x for x in grammar.splitlines() if x.startswith("expression:"))
    pred_line = next(x for x in grammar.splitlines() if x.startswith("predicate:"))

    # Rewritten unknown {value} -> VALUE; function tokens may be quoted by the generator.
    has_sum = ("sum VALUE" in expr_line) or ('"sum" VALUE' in expr_line)
    has_avg = ("avg VALUE" in expr_line) or ('"avg" VALUE' in expr_line)
    assert has_sum and has_avg
    assert " | " in expr_line

    # Operators may be quoted in the rewritten grammar; accept both forms
    assert (
        ('value ">" VALUE' in pred_line)
        or ('"value" ">" VALUE' in pred_line)
        or ('value > VALUE' in pred_line)
        or ('"value" > VALUE' in pred_line)
    )
    assert (
        ('value "<" VALUE' in pred_line)
        or ('"value" "<" VALUE' in pred_line)
        or ('value < VALUE' in pred_line)
        or ('"value" < VALUE' in pred_line)
    )
    assert " | " in pred_line
