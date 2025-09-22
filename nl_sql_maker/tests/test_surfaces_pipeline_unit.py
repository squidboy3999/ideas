import pytest
from pathlib import Path
from vbg_tools.surfaces_pipeline import generate_surfaces, PipelineConfig

def _grammar_for_actions(actions=("count","sum")):
    acts = " | ".join(f'"{a}"i' for a in actions)
    return '\n'.join([
        'SELECT: "select"i',
        'FROM: "from"i',
        'AND: "and"i',
        'OR: "or"i',
        'OF: "of"i',
        'COMMA: ","',
        'start: query',
        f'action: {acts}',
        'VALUE: "VALUE"',
        'projection: action [OF] VALUE',
        'query: SELECT FROM | SELECT projection FROM',
        '%import common.WS',
        '%ignore WS',
    ])

def _vocab_predicate_ready():
    return {
        "keywords": {
            "connectors": {"AND":"and","OR":"or","FROM":"from","OF":"of","COMMA":",","NOT":"not"},
            "select_verbs": {"select": {"aliases": ["show","display"]}},
            "comparison_operators": {
                "between": {"aliases": ["between"]},
                "greater_than": {"aliases": [">","greater than"]},
                "less_than": {"aliases": ["<","less than"]},
            }
        },
        "sql_actions": {
            "count": {"placement":"projection","bind_style":"of","aliases":["count"],"template":"COUNT({column})","applicable_types":{"column":["any"]}},
            "sum":   {"placement":"projection","bind_style":"of","aliases":["sum"],  "template":"SUM({column})",  "applicable_types":{"column":["numeric"]}},
        }
    }

def _binder_numeric_minimal():
    return {
        "catalogs": {
            "tables": {"users": {}},
            "columns": {
                "users.age": {"table":"users","name":"age","type":"integer"},
                "users.name":{"table":"users","name":"name","type":"text"},
            },
            "connectors": {"AND":"and","OR":"or","FROM":"from","OF":"of","COMMA":",","NOT":"not"},
            "functions": {}
        }
    }

def test_pipeline_produces_constrained_gold_when_numeric_present(tmp_path: Path):
    vocab = _vocab_predicate_ready()
    binder = _binder_numeric_minimal()
    grammar = _grammar_for_actions(("count","sum"))
    cfg = PipelineConfig(
        max_specs=20,
        per_spec_max=6,
        per_spec_unconstrained_max=1,
        global_unconstrained_budget=2,
        order="predicates_first",
    )
    bundle = generate_surfaces(vocab, binder, grammar, config=cfg)
    assert bundle.gold, "Expected some gold surfaces"

    def _has_predicate(nl: str) -> bool:
        s = nl.lower()
        return (" between " in s) or (" > " in s) or (" < " in s) or ("greater than" in s) or ("less than" in s)

    pred_gold = [g for g in bundle.gold if _has_predicate(g.get("natural_language",""))]
    assert pred_gold, "Expected at least one constrained surface in gold"
    assert len(pred_gold) >= len(bundle.gold) - 2

def test_pipeline_respects_applicability_gate(tmp_path: Path):
    vocab = {
        "keywords": {
            "connectors": {"AND":"and","OR":"or","FROM":"from","OF":"of","COMMA":",","NOT":"not"},
            "select_verbs": {"select": {"aliases": ["show"]}},
            "comparison_operators": { "between": {"aliases": ["between"]} }
        },
        "sql_actions": {
            "sum": {"placement":"projection","bind_style":"of","aliases":["sum"],"template":"SUM({column})"}
        }
    }
    binder = _binder_numeric_minimal()
    grammar = _grammar_for_actions(("sum",))
    cfg = PipelineConfig(max_specs=10, per_spec_max=3, per_spec_unconstrained_max=1, global_unconstrained_budget=1)

    bundle = generate_surfaces(vocab, binder, grammar, config=cfg)
    assert not bundle.gold and not bundle.multipath

    vocab["sql_actions"]["sum"]["applicable_types"] = {"column": ["numeric"]}
    bundle2 = generate_surfaces(vocab, binder, grammar, config=cfg)
    assert bundle2.gold
