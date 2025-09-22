from __future__ import annotations
from typing import Dict, Any, List
from .surfaces_spec_builder import SQLSpec, column_slot_types

def _select_aliases(vocab: Dict[str, Any]) -> List[str]:
    return list(((vocab.get("keywords") or {}).get("select_verbs") or {}).get("select", {}).get("aliases") or ["show"])

def _action_alias(vocab: Dict[str, Any], func: str) -> str:
    # Accept both top-level and legacy location for completeness (though aliases are usually in top-level)
    meta = (vocab.get("sql_actions") or {}).get(func) \
        or (((vocab.get("keywords") or {}).get("sql_actions") or {}).get(func) if isinstance((vocab.get("keywords") or {}).get("sql_actions"), dict) else {})
    aliases = (meta or {}).get("aliases") or [func]
    return str(aliases[0])

def _conn(vocab: Dict[str, Any], key: str, default: str) -> str:
    return ((vocab.get("keywords") or {}).get("connectors") or {}).get(key, default)

def _comp(vocab: Dict[str, Any], name: str, default: str) -> str:
    return (((vocab.get("keywords") or {}).get("comparison_operators") or {}).get(name) or {}).get("aliases", [default])[0]

def render_projection_phrases(spec: SQLSpec, vocab: Dict[str, Any]) -> List[str]:
    sels = _select_aliases(vocab)[:2]
    act  = _action_alias(vocab, spec.func)
    of   = _conn(vocab, "OF", "of")
    frm  = _conn(vocab, "FROM", "from")
    col  = spec.column.split(".", 1)[1]
    base = [f"{sels[0]} {act} {of} {col} {frm} {spec.table}"]
    if len(sels) > 1:
        base.append(f"{sels[1]} {act} {of} {col} {frm} {spec.table}")
    return base

def render_predicate_phrases(spec: SQLSpec, vocab: Dict[str, Any], binder: Dict[str, Any]) -> List[str]:
    sels = _select_aliases(vocab)[:2]
    act  = _action_alias(vocab, spec.func)
    of   = _conn(vocab, "OF", "of")
    frm  = _conn(vocab, "FROM", "from")
    and_kw = _conn(vocab, "AND", "and")

    col  = spec.column.split(".", 1)[1]
    base = [f"{sels[0]} {act} {of} {col} {frm} {spec.table}"]
    if len(sels) > 1:
        base.append(f"{sels[1]} {act} {of} {col} {frm} {spec.table}")

    slots = column_slot_types(binder, spec.column)
    out: List[str] = []

    between = _comp(vocab, "between", "between")
    gt = _comp(vocab, "greater_than", ">")
    lt = _comp(vocab, "less_than", "<")

    if "numeric" in slots:
        for pre in base:
            out.append(f"{pre} {col} {between} 18 {and_kw} 30")
            out.append(f"{pre} {col} {gt} 10")
            out.append(f"{pre} {col} {lt} 100")
    elif "date" in slots:
        for pre in base:
            out.append(f"{pre} {col} {between} 2020-01-01 {and_kw} 2020-12-31")
    return out

def assemble_surfaces(
    spec: SQLSpec,
    vocab: Dict[str, Any],
    binder: Dict[str, Any],
    *,
    per_spec_max: int = 6,
    per_spec_unconstrained_max: int = 2,
    order: str = "predicates_first",
    require_min_predicates: bool = True,
) -> List[str]:
    bases = render_projection_phrases(spec, vocab)
    preds = render_predicate_phrases(spec, vocab, binder)

    if order == "predicates_first":
        if require_min_predicates and not preds:
            return bases[:min(per_spec_unconstrained_max, per_spec_max)]
        ordered = preds + bases[:per_spec_unconstrained_max]
    else:
        ordered = bases[:per_spec_unconstrained_max] + preds
    return ordered[:per_spec_max]
