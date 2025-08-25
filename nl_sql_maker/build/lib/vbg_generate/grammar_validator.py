# src/n2s_validators/grammar_validator.py
from __future__ import annotations
import random
from collections import defaultdict
from typing import Dict, Any, List, Tuple, Set

from lark import Lark

# NEW: shared schema helpers (extracted)
from vbg_shared.schema_utils import (
    list_tables,
    list_columns,
    list_functions,
    table_to_columns,
    is_geometry_col,
    select_friendly_functions,
)

# ------------------------------------------------------------
# Minimal canonical pools adapter (graph → symbol sets)
# (Kept for simple external callers that expect this function.)
# ------------------------------------------------------------
def _canonicals(graph: Dict[str, Any]) -> Dict[str, Set[str]]:
    """
    Extract canonical symbol pools from the relationship graph.
    Expect graph entries shaped like: { name: {"entity_type": "...", ...}, ... }.
    """
    tables = {k for k, v in graph.items() if isinstance(v, dict) and v.get("entity_type") == "table"}
    cols   = {k for k, v in graph.items() if isinstance(v, dict) and v.get("entity_type") == "column"}
    funcs  = {k for k, v in graph.items() if isinstance(v, dict) and v.get("entity_type") in ("sql_actions", "postgis_actions")}
    return {"tables": tables, "columns": cols, "functions": funcs}

# ------------------------------------------------------------
# Grammar Analyzer
# ------------------------------------------------------------
class GrammarAnalyzer:
    """
    Computes minimal expansion depth per rule and provides rule lookup for a Lark parser.
    Keeps it runtime-friendly; no validator-only logic here.
    """
    def __init__(self, parser: Lark):
        self.parser = parser
        self.rule_lookup = defaultdict(list)
        for r in self.parser.rules:
            # robust across Lark versions
            origin = r.origin.name.value if hasattr(r.origin.name, "value") else r.origin.name
            self.rule_lookup[origin].append(r)

        self.min_depths: Dict[str, int] = {}
        self._calculate_min_depth()

    def _get_min_depth(self, term_name: str) -> int:
        # Uppercase or quoted literals are terminals; also treat a few grammar tokens as terminals
        if term_name.isupper() or term_name.startswith('"') or term_name in {"AND", "COMMA", "OF", "FROM", "SELECT"}:
            return 1
        return self.min_depths.get(term_name, 10 ** 9)

    def _calculate_min_depth(self) -> None:
        for _ in range(len(self.rule_lookup) + 3):
            for rule_name, expansions in self.rule_lookup.items():
                best = 10 ** 9
                for r in expansions:
                    if not r.expansion:
                        best = min(best, 1)  # empty production
                    else:
                        s = 1 + sum(self._get_min_depth(t.name) for t in r.expansion)
                        best = min(best, s)
                if best < self.min_depths.get(rule_name, 10 ** 9):
                    self.min_depths[rule_name] = best

# ------------------------------------------------------------
# Smart canonical generator (for stress tests)
# ------------------------------------------------------------
class SmartGenerator:
    """
    Canonical phrase generator guided by grammar depths with a simple recursion guard.
    Emits **canonical** strings only (no NL / aliasing here).
    """
    def __init__(self, parser: Lark, graph: Dict[str, Any], analyzer: GrammarAnalyzer):
        self.parser = parser
        self.rule_lookup = analyzer.rule_lookup
        self.analyzer = analyzer
        self.RECURSION_LIMIT = 4

        # Gather canonicals using shared helpers
        tables = list_tables(graph)
        cols   = list_columns(graph)
        fns    = list_functions(graph)

        # Prefer binder metadata to decide select-friendliness; fall back to deny-list heuristic
        fn_pool = select_friendly_functions(graph)
        if not fn_pool:
            fn_pool = fns  # rare fallback

        # Tolerant token names from grammar (COLUMN vs CANONICAL_COLUMN, etc.)
        self.vocab = {
            "CANONICAL_FUNCTION": fn_pool,
            "FUNCTION": fn_pool,
            "CANONICAL_COLUMN": cols,
            "COLUMN": cols,
            "CANONICAL_TABLE": tables,
            "TABLE": tables,
            "SELECT": ["select"],
            "OF": ["of"],
            "FROM": ["from"],
            "AND": ["and"],
            "OR": ["or"],
            "COMMA": [","],
        }

    def generate(self, graph: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
        """
        Produce a shallow canonical:
          select <1-3 items> from <table>
        Items are columns or simple 'fn of column' (no nesting).
        Prefer columns from the FROM table. Only use spatial fns if a geometry column exists.
        """
        tables = self.vocab.get("TABLE", []) or self.vocab.get("CANONICAL_TABLE", [])
        cols   = self.vocab.get("COLUMN", []) or self.vocab.get("CANONICAL_COLUMN", [])
        fns    = self.vocab.get("FUNCTION", []) or self.vocab.get("CANONICAL_FUNCTION", [])

        if not tables or not (cols or fns):
            return "", {"reason": "empty_vocab"}

        t = random.choice(tables)
        tcols_map = table_to_columns(graph)
        tcols = list(tcols_map.get(t, [])) or list(cols)

        # Precompute geometry columns (global & for this table)
        geom_cols_all = [c for c in cols if is_geometry_col(graph, c)]
        geom_cols_t   = [c for c in tcols if is_geometry_col(graph, c)]
        has_geom = bool(geom_cols_all)

        # pick 1–3 items
        k = random.choice([1, 1, 2, 2, 3])
        items: List[str] = []

        def pick_col(prefer_table: bool = True) -> str:
            pool = tcols if (prefer_table and tcols) else cols
            return random.choice(pool)

        def pick_geom_col(prefer_table: bool = True) -> Optional[str]:
            pool = geom_cols_t if (prefer_table and geom_cols_t) else geom_cols_all
            return random.choice(pool) if pool else None

        for _ in range(k):
            mode = random.random()
            # 70% columns, 30% functions
            if fns and mode < 0.30:
                fn = random.choice(fns)

                # simple check: if function name starts with 'st_', require a geometry column; else any column is OK.
                needs_geom = fn.startswith("st_")
                col = pick_geom_col() if (needs_geom and has_geom) else pick_col()

                # 80% of the time give it an argument
                if random.random() < 0.8 and col:
                    items.append(f"{fn} of {col}")
                else:
                    items.append(fn)
            else:
                items.append(pick_col())

        # join
        if len(items) == 1:
            sel = items[0]
        elif len(items) == 2:
            sel = f"{items[0]} and {items[1]}"
        else:
            head = ", ".join(items[:-1])
            sel = f"{head}, and {items[-1]}"

        canonical = f"select {sel} from {t}"
        return canonical, {"table": t, "items": items}

    # The _expand() method remains unchanged.
    def _expand(self, sym: str, depth: int, log: List[str], indent: str, counts: Dict[str, int]) -> str | None:
        log.append(f"{indent}>> {sym} depth={depth}")
        if depth <= 0:
            return None

        # Terminal?
        if sym not in self.rule_lookup:
            # choose from known vocab if present, else echo the literal (strip quotes)
            if sym in self.vocab and self.vocab[sym]:
                v = random.choice(self.vocab[sym])
            else:
                v = sym.strip('"')
            log.append(f"{indent}<< term '{v}'")
            return v

        # recursion guard per symbol
        counts = dict(counts)
        counts[sym] = counts.get(sym, 0) + 1
        if counts[sym] > self.RECURSION_LIMIT:
            # prefer non-self-recursive expansions
            cands = [r for r in self.rule_lookup[sym] if sym not in [t.name for t in r.expansion]]
            if not cands:
                return None
        else:
            cands = list(self.rule_lookup[sym])

        # depth-aware choice
        if depth < (self.analyzer.min_depths.get(sym, 1) + 5):
            costs = {r: 1 + sum(self.analyzer.min_depths.get(t.name, 0) for t in r.expansion) for r in cands}
            min_cost = min(costs.values())
            opts = [r for r, c in costs.items() if c == min_cost]
        else:
            opts = cands

        rule = random.choice(opts)
        parts: List[str] = []
        for t in rule.expansion:
            sub = self._expand(t.name, depth - 1, log, indent + "  ", counts)
            if sub is None:
                return None
            parts.append(sub)
        s = " ".join(x for x in parts if x != "")
        log.append(f"{indent}<< '{s[:60]}'")
        return s
