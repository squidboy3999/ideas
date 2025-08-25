# src/n2s_runtime/canonical_core.py
from __future__ import annotations
import re
from typing import Dict, Any, List, Tuple, Optional, Set, Union

# -------------------------
# Public types & exceptions
# -------------------------

class BindError(Exception):
    """Raised when canonical binding fails."""


Selectable = Union[
    Dict[str, str],  # {"type":"column","name":...}
    Dict[str, Any],  # {"type":"func","name":...,"args":[Selectable,...]}
]

# -------------------------
# Canonical tokenization/IO
# -------------------------

def canon_tokenize(s: str) -> list[str]:
    """
    Canonical tokenizer used by the binder.
    Keeps dotted identifiers intact (e.g., 'users.balance').
    Treats SQL-ish keywords and commas as separate tokens.
    """
    pat = re.compile(
        r"\|\||&&|<=|>=|!=|==|<>|[A-Za-z0-9_]+(?:\.[A-Za-z0-9_]+)+|[A-Za-z0-9_]+|[^\sA-Za-z0-9_]"
    )
    return pat.findall(s or "")



def _serialize_selectable(sel: Selectable) -> str:
    if sel["type"] == "column":
        return sel["name"]
    name = sel["name"]
    args = sel.get("args", [])
    if not args:
        return name
    return f"{name} of {_serialize_column_list(args)}"

def _serialize_column_list(items: List[Selectable]) -> str:
    if not items:
        return ""
    if len(items) == 1:
        return _serialize_selectable(items[0])
    if len(items) == 2:
        return f"{_serialize_selectable(items[0])} and {_serialize_selectable(items[1])}"
    head = ", ".join(_serialize_selectable(x) for x in items[:-1])
    return f"{head}, and {_serialize_selectable(items[-1])}"

def serialize_binding(binding: Dict[str, Any]) -> str:
    """
    Convert a canonical binding back to canonical text (“round-trip” form).
    """
    cols = _serialize_column_list(binding["selectables"])
    tbl  = binding["table"]
    return f"select {cols} from {tbl}"

# -------------------------
# View adapters (graph/binder)
# -------------------------

def _is_graph_like(obj: Dict[str, Any]) -> bool:
    # Heuristic: graph has many entries, each with 'entity_type'
    if not isinstance(obj, dict):
        return False
    # sample a few values
    i = 0
    for _, v in obj.items():
        if isinstance(v, dict) and "entity_type" in v:
            return True
        i += 1
        if i > 10:
            break
    return False

def _pick_catalogs(source: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize to a catalogs dict:
      - If source has 'catalogs', use it.
      - Else if it already looks like catalogs (functions/columns/tables keys), use it.
      - Else return {} (will fall back to graph accessors).
    """
    if not isinstance(source, dict):
        return {}
    if "catalogs" in source and isinstance(source["catalogs"], dict):
        return source["catalogs"]
    # catalogs-like (flat)
    keys = set(source.keys())
    if {"functions","columns","tables"} & keys:
        return source
    return {}

def _collect_canonicals_from_graph(graph: Dict[str, Any]) -> Tuple[Set[str], Set[str], Set[str]]:
    tables = {k for k, v in graph.items() if v.get("entity_type") == "table"}
    cols   = {k for k, v in graph.items() if v.get("entity_type") == "column"}
    funcs  = {k for k, v in graph.items() if v.get("entity_type") in ("sql_actions", "postgis_actions")}
    return tables, cols, funcs

def _collect_canonicals_from_catalogs(catalogs: Dict[str, Any]) -> Tuple[Set[str], Set[str], Set[str]]:
    tables = set()
    tbls = catalogs.get("tables")
    if isinstance(tbls, dict):
        tables = set(tbls.keys())
    elif isinstance(tbls, list):
        tables = set(tbls)
    cols   = set((catalogs.get("columns") or {}).keys())
    funcs  = set((catalogs.get("functions") or {}).keys())
    return tables, cols, funcs

def _canonicals(source: Dict[str, Any]) -> Dict[str, Set[str]]:
    """
    Unified canonical collector that accepts either a graph or a catalogs-shaped dict.
    """
    catalogs = _pick_catalogs(source)
    if catalogs:
        t, c, f = _collect_canonicals_from_catalogs(catalogs)
    elif _is_graph_like(source):
        t, c, f = _collect_canonicals_from_graph(source)
    else:
        t, c, f = set(), set(), set()
    return {"tables": t, "columns": c, "functions": f}

def _build_table_columns_from_graph(graph: Dict[str, Any]) -> Dict[str, List[str]]:
    out: Dict[str, List[str]] = {}
    for tname, tnode in graph.items():
        if tnode.get("entity_type") != "table":
            continue
        cols = ((tnode.get("metadata") or {}).get("columns") or {})
        out[tname] = list(cols.keys())
    return out

def _build_table_columns_from_catalogs(catalogs: Dict[str, Any]) -> Dict[str, List[str]]:
    """
    Prefer explicit table→columns if present; else, derive from column 'table' fields.
    """
    out: Dict[str, List[str]] = {}

    # If tables provided as dict with embedded column lists
    tbls = catalogs.get("tables") or {}
    if isinstance(tbls, dict):
        for tname, tnode in tbls.items():
            cols = list((tnode or {}).get("columns") or [])
            out[tname] = cols

    # Fallback: group by column.table (common binder.yaml shape)
    if not out:
        by_table: Dict[str, List[str]] = {}
        for cname, cinfo in (catalogs.get("columns") or {}).items():
            t = (cinfo or {}).get("table")
            if not t:
                continue
            by_table.setdefault(t, []).append(cname)
        out = by_table

    return out

def _get_col_meta_from_graph(graph: Dict[str, Any], name: str) -> Dict[str, Any]:
    node = graph.get(name, {})
    md = node.get("metadata", {}) if isinstance(node, dict) else {}
    return md if isinstance(md, dict) else {}

def _get_func_meta_from_graph(graph: Dict[str, Any], name: str) -> Dict[str, Any]:
    node = graph.get(name, {})
    md = node.get("metadata", {}) if isinstance(node, dict) else {}
    return md if isinstance(md, dict) else {}

def _get_col_meta_from_catalogs(catalogs: Dict[str, Any], name: str) -> Dict[str, Any]:
    c = (catalogs.get("columns") or {}).get(name, {}) or {}
    # normalize to graph-like shape for compatibility checks
    return {
        "type": c.get("type"),
        "type_category": c.get("type_category"),
        "labels": list(c.get("labels") or []),
    }

def _get_func_meta_from_catalogs(catalogs: Dict[str, Any], name: str) -> Dict[str, Any]:
    f = (catalogs.get("functions") or {}).get(name, {}) or {}
    # Pull known fields if present; fall back to empty dict.
    out: Dict[str, Any] = {}
    if "applicable_types" in f:
        out["applicable_types"] = f.get("applicable_types")
    if "label_rules" in f:
        out["label_rules"] = f.get("label_rules")
    return out

# -------------------------
# Compatibility predicate
# -------------------------

def is_compatible(column_md: Dict[str, Any], func_md: Dict[str, Any]) -> bool:
    """
    Single source of truth for column→function compatibility:
      - applicable_types: dict(var -> [allowed_types]) is ANY-satisfiable (`any` allowed).
      - label_rules: ["id", "not postgis"] must be satisfied.
    Missing metadata => permissive (True).
    """
    if not (isinstance(column_md, dict) and isinstance(func_md, dict)):
        return True

    # Type buckets
    app = func_md.get("applicable_types")
    if isinstance(app, dict) and app:
        col_type = str(column_md.get("type", "")).lower()
        type_ok = False
        for _var, allowed in app.items():
            if not isinstance(allowed, list):
                continue
            allowed_lc = {str(a).lower() for a in allowed}
            if "any" in allowed_lc or col_type in allowed_lc:
                type_ok = True
                break
    else:
        type_ok = True  # no constraints → permissive

    # Label rules
    labels = {str(x).lower() for x in column_md.get("labels", []) if isinstance(x, str)}
    rules  = [str(r) for r in func_md.get("label_rules", []) if isinstance(r, str)]
    labels_ok = True
    for r in rules:
        if r.startswith("not "):
            if r[4:].lower() in labels:
                labels_ok = False; break
        else:
            if r.lower() not in labels:
                labels_ok = False; break

    return bool(type_ok and labels_ok)

# -------------------------
# Canonical Binder (runtime-worthy)
# -------------------------

class CanonicalBinder:
    """
    Binds canonical token streams shaped like:

      SELECT column_list [FROM|OF table]

    where:
      column_list   := selectable ( (',' selectable)* (','? 'and' selectable)? | 'and' selectable )?
      selectable    := column | function_call
      function_call := function ['of' column_list]

    Parameters
    ----------
    strict_types : bool
        If True, type/label incompatibilities raise BindError.
        If False, incompatibilities are tolerated (and optionally coerced).
    coerce_types : bool
        If True (and not strict), replace incompatible arg columns with a
        compatible column from the current table (or globally as fallback).
    allow_ordering_funcs_in_args : bool
        If True, allow order_by_asc / order_by_desc as *argument* functions.
    """

    ARG_FUNC_DENY: Set[str] = {"order_by_asc", "order_by_desc"}

    def __init__(
        self,
        source: Dict[str, Any],
        *,
        strict_types: bool = True,
        coerce_types: bool = False,
        allow_ordering_funcs_in_args: bool = False,
    ):
        # Accept either a graph or binder catalogs
        self.catalogs: Dict[str, Any] = _pick_catalogs(source)
        self.graph: Dict[str, Any] = source if _is_graph_like(source) else {}

        c = _canonicals(source)
        self.tables: Set[str] = c["tables"]
        self.columns: Set[str] = c["columns"]
        self.functions: Set[str] = c["functions"]

        self.strict_types = bool(strict_types)
        self.coerce_types = bool(coerce_types)
        self.allow_ordering_funcs_in_args = bool(allow_ordering_funcs_in_args)

        # table -> [columns], and inverse
        if self.catalogs:
            self.table_columns: Dict[str, List[str]] = _build_table_columns_from_catalogs(self.catalogs)
        else:
            self.table_columns = _build_table_columns_from_graph(self.graph)

        self.column_owner: Dict[str, str] = {
            col: t for t, cols in self.table_columns.items() for col in cols
        }

    # ---------- metadata helpers ----------

    def _get_col_meta(self, name: str) -> Dict[str, Any]:
        if self.catalogs:
            return _get_col_meta_from_catalogs(self.catalogs, name)
        return _get_col_meta_from_graph(self.graph, name)

    def _get_func_meta(self, name: str) -> Dict[str, Any]:
        if self.catalogs:
            return _get_func_meta_from_catalogs(self.catalogs, name)
        return _get_func_meta_from_graph(self.graph, name)

    # ---------- compatibility helpers ----------

    def _first_compatible_col_for_func_in_table(self, fn: str, tname: str) -> Optional[str]:
        fn_md = self._get_func_meta(fn)
        for c in self.table_columns.get(tname, []):
            if is_compatible(self._get_col_meta(c), fn_md):
                return c
        cols = self.table_columns.get(tname, [])
        return cols[0] if cols else None

    def _first_compatible_col_global(self, fn: str) -> Optional[str]:
        fn_md = self._get_func_meta(fn)
        for col in self.columns:
            if is_compatible(self._get_col_meta(col), fn_md):
                return col
        # fallback: any column at all
        return next(iter(self.columns), None)

    def _any_table(self) -> Optional[str]:
        return next(iter(self.tables)) if self.tables else None

    # ---------- recursive descent over tokens ----------

    def bind(self, tokens: List[str], recorder: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Parse a canonical token stream into a minimal binding using the standard
        list/connector behavior. If `recorder` is provided, append human-readable
        trace lines to it.
        """
        pos = 0  # cursor over tokens

        def rec(msg: str) -> None:
            if recorder is not None:
                recorder.append(f"[pos={pos}] {msg}")

        def expect(token: str) -> None:
            nonlocal pos
            if pos >= len(tokens) or tokens[pos] != token:
                found = tokens[pos] if pos < len(tokens) else 'EOF'
                rec(f"EXPECT FAIL: wanted '{token}', found '{found}'")
                raise BindError(f"expected '{token}' at {pos}, found '{found}'")
            rec(f"EXPECT OK: '{token}'")
            pos += 1

        def peek() -> Optional[str]:
            return tokens[pos] if pos < len(tokens) else None

        def at_end() -> bool:
            return pos >= len(tokens)

        def parse_selectable() -> Selectable:
            """column_name | function_call | qualified column (table . column)"""
            nonlocal pos
            t = peek()
            rec(f"parse_selectable: lookahead='{t}'")
            if t is None:
                raise BindError("unexpected end while parsing selectable")

            # --- NEW: qualified column: table '.' column ---
            # If we see TABLE '.' COLUMN, accept it and return a column selectable
            # (we ignore the table part here; the table after FROM governs binding).
            if (
                t in self.tables
                and (pos + 1) < len(tokens) and tokens[pos + 1] == "."
                and (pos + 2) < len(tokens) and tokens[pos + 2] in self.columns
            ):
                tbl = tokens[pos]
                dot = tokens[pos + 1]  # '.'
                col = tokens[pos + 2]
                pos += 3
                # Optional sanity: check declared ownership (don’t fail; just record)
                belongs = col in (self.table_columns.get(tbl, []) or [])
                rec(f"SELECTABLE = qualified column '{tbl}.{col}' (belongs={belongs}) -> use column '{col}'")
                return {"type": "column", "name": col}

            # --- existing: bare column ---
            if t in self.columns:
                pos += 1
                rec(f"SELECTABLE = column '{t}'")
                return {"type": "column", "name": t}

            # --- existing: function (with optional 'of' args) ---
            if t in self.functions:
                pos += 1
                rec(f"SELECTABLE = function '{t}'")
                fn = {"type": "func", "name": t, "args": []}
                if peek() == "of":
                    expect("of")
                    rec("function has 'of' → parse argument list")
                    fn["args"] = parse_column_list()
                else:
                    rec("function without 'of' (zero-arg call)")
                return fn

            rec(f"SELECTABLE FAIL: '{t}' not column/function")
            raise BindError(f"token '{t}' is neither a column nor a function")

        def parse_column_list() -> List[Selectable]:
            """selectable ( , selectable )* ( ,? AND selectable )?"""
            nonlocal pos
            rec("parse_column_list: ENTER")
            items: List[Selectable] = [parse_selectable()]
            rec(f"parse_column_list: first item parsed; next='{peek()}'")

            # Comma chain
            while peek() == ",":
                expect(",")
                rec(f"comma-branch: parse another selectable; next='{peek()}'")
                items.append(parse_selectable())

            # Optional Oxford comma: try consuming a comma; if not followed by 'and', rewind.
            if peek() == ",":
                save = pos
                expect(",")
                if peek() == "and":
                    expect("and")
                    rec("Oxford ', and' tail: parse final selectable")
                    items.append(parse_selectable())
                else:
                    rec("not an Oxford ', and' → rewind")
                    pos = save

            # Or bare 'and' tail
            if peek() == "and":
                expect("and")
                rec("'and' tail: parse final selectable")
                items.append(parse_selectable())

            rec(f"parse_column_list: EXIT with {len(items)} item(s)")
            return items

        # ---- SELECT ----
        rec(f"BEGIN bind; tokens={tokens}")
        expect("select")

        selectables = parse_column_list()

        # ---- FROM | OF table ----
        nxt = peek()
        rec(f"post-selectables next token='{nxt}'")
        if nxt not in {"from", "of"}:
            raise BindError(f"expected 'from' or 'of' before table, found '{nxt}'")
        pos += 1
        rec(f"connector consumed: '{nxt}'")

        tbl = peek()
        if tbl not in self.tables:
            raise BindError(f"expected table after '{nxt}', found '{tbl}'")
        pos += 1
        rec(f"table = '{tbl}'")

        if not at_end():
            trailing = tokens[pos:]
            rec(f"TRAILING TOKENS: {trailing}")
            raise BindError(f"unexpected trailing tokens starting at {pos}: {trailing}")

        # Type compatibility checks (may raise; may coerce if configured)
        for sel in selectables:
            self._check_selectable_types(sel, current_table=tbl, recorder=recorder)

        binding = {
            "template_id": "select_cols_from_table",
            "table": tbl,
            "selectables": selectables,
        }
        rec("BIND SUCCESS")
        return binding

    # ---------- type/label checks (with optional coercion) ----------

    def _check_selectable_types(
        self,
        sel: Selectable,
        *,
        current_table: Optional[str],
        recorder: Optional[List[str]] = None
    ) -> None:
        def rec(msg: str) -> None:
            if recorder is not None:
                recorder.append(f"[typecheck] {msg}")

        if sel.get("type") == "column":
            rec(f"column '{sel['name']}' OK (no checks)")
            return

        if sel.get("type") == "func":
            fn = sel["name"]
            fn_md = self._get_func_meta(fn)
            args = sel.get("args", [])
            if not args:
                rec(f"function '{fn}' has no args; skip typecheck (ok if zero-arity)")
                return

            for idx, arg in enumerate(args):
                if arg.get("type") == "func":
                    # Disallow ordering helpers in arg position unless enabled
                    inner_name = arg.get("name")
                    if inner_name in self.ARG_FUNC_DENY and not self.allow_ordering_funcs_in_args:
                        rec(f"DISALLOWED arg function '{inner_name}' inside args to '{fn}'")
                        raise BindError(f"function '{inner_name}' not permitted in argument position")
                    rec(f"func '{fn}' ← nested func '{inner_name}' → recurse")
                    self._check_selectable_types(arg, current_table=current_table, recorder=recorder)
                    continue

                if arg.get("type") == "column":
                    col_name = arg["name"]
                    col_md = self._get_col_meta(col_name)
                    type_ok = is_compatible(col_md, fn_md)
                    rec(f"func '{fn}' ← arg column '{col_name}': compatible={type_ok}")

                    if type_ok:
                        continue

                    # Incompatible: decide based on strict/coercion knobs
                    if self.strict_types:
                        raise BindError(f"incompatible arg '{col_name}' for function '{fn}'")

                    if self.coerce_types:
                        # Try to replace with a compatible column from current table,
                        # else fall back to a global compatible column.
                        replacement = None
                        if current_table:
                            replacement = self._first_compatible_col_for_func_in_table(fn, current_table)
                        replacement = replacement or self._first_compatible_col_global(fn)
                        if replacement and replacement != col_name:
                            rec(f"COERCE: '{col_name}' → '{replacement}' for '{fn}'")
                            args[idx] = {"type": "column", "name": replacement}
                            continue
                        else:
                            rec(f"COERCE FAILED: no compatible column found for '{fn}' (kept '{col_name}')")
                            # tolerate and continue (non-strict)
                    else:
                        rec(f"TOLERATE: '{col_name}' incompatible for '{fn}' (non-strict, no coercion)")
                else:
                    rec(f"func '{fn}' ← unknown arg kind; ignored")

# -------------------------
# Sanity check / CLI
# -------------------------

def _make_tiny_graph() -> Dict[str, Any]:
    """
    Minimal graph for local sanity checks.
    """
    return {
        # table block w/ embedded columns (graph-style)
        "users": {
            "entity_type": "table",
            "metadata": {
                "columns": {
                    "name":   {"type": "TEXT", "labels": []},
                    "age":    {"type": "INT",  "labels": []},
                    "user_id":{"type": "INT",  "labels": ["id"]},
                }
            },
        },
        # columns as canonicals
        "name":    {"entity_type": "column", "metadata": {"type": "TEXT", "labels": []}},
        "age":     {"entity_type": "column", "metadata": {"type": "INT",  "labels": []}},
        "user_id": {"entity_type": "column", "metadata": {"type": "INT",  "labels": ["id"]}},
        # function avg (numeric)
        "avg": {
            "entity_type": "sql_actions",
            "metadata": {
                "applicable_types": {"column": ["int", "float", "any"]},
                "label_rules": []
            }
        },
    }

def main() -> None:
    # tiny sanity pass
    graph = _make_tiny_graph()
    binder_strict = CanonicalBinder(graph, strict_types=True, coerce_types=False)
    binder_flex   = CanonicalBinder(graph, strict_types=False, coerce_types=True)

    def run(binder: CanonicalBinder, s: str) -> None:
        toks = canon_tokenize(s)
        rec: List[str] = []
        try:
            bound = binder.bind(toks, recorder=rec)
            print("OK:", serialize_binding(bound))
        except Exception as e:
            print("ERR:", e)
        finally:
            if rec:
                print("  TRACE:")
                for line in rec:
                    print("   ", line)

    print("=== STRICT ===")
    run(binder_strict, "select name and avg of age from users")
    run(binder_strict, "select avg of name from users")  # should fail (type mismatch)

    print("\n=== FLEX+COERCE ===")
    run(binder_flex, "select avg of name from users")    # should coerce to 'age'

if __name__ == "__main__":
    main()
