# vbg_tools/artifact_helpers.py
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple
import yaml
import ast
import re

_DICTLIKE_RE = re.compile(r"^\s*\{.*\}\s*$")

# ---------- IO ----------

def load_yaml_file(p: str | Path) -> dict:
    path = Path(p)
    if not path.exists():
        raise FileNotFoundError(f"YAML not found: {path}")
    with path.open("r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh)
    return data or {}


def write_yaml_file(path: Path, obj: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        yaml.safe_dump(obj, fh, sort_keys=False, allow_unicode=True)


def write_text_file(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


# ---------- normalization / coercion ----------

CORE_CONNECTORS = {
    "AND": "and",
    "OR": "or",
    "NOT": "not",
    "FROM": "from",
    "OF": "of",
    "COMMA": ",",
}


def normalize_aliases(xs: Iterable[str]) -> list[str]:
    uniq = {str(x).strip() for x in (xs or []) if str(x).strip()}
    return sorted(uniq)


def coerce_types(v: Any) -> list[str]:
    if v is None:
        return []
    if isinstance(v, list):
        return [str(x) for x in v]
    return [str(v)]


def ensure_core_connectors(conn: dict | None) -> dict:
    conn = dict(conn or {})
    # Normalize keys to UPPER, keep values as-is
    upper = {str(k).upper(): str(v) for k, v in conn.items() if str(k).strip()}
    for k, v in CORE_CONNECTORS.items():
        if k not in upper:
            upper[k] = v
    return upper


# ---------- schema extraction helpers ----------

def iter_table_defs(schema: dict) -> list[tuple[str, object]]:
    """
    Returns (table_name, table_def) pairs.
    Accepts:
      tables: { name: {...} }  or  tables: [name, ...]
    """
    tables_node = schema.get("tables")
    pairs: list[tuple[str, object]] = []
    if isinstance(tables_node, dict):
        for tname, tdef in tables_node.items():
            pairs.append((str(tname), tdef))
    elif isinstance(tables_node, list):
        for tname in tables_node:
            pairs.append((str(tname), {}))
    return pairs


def coerce_column_row(table: str, col_spec: Any) -> dict | None:
    """
    Normalize a single column spec into a row dict:
      {"fqn": "<table>.<name>", "table": <table>, "name": <name>, "types": [..]}

    Accepts:
      - "age"
      - {"name":"age", "types":[...]} / {"type":"INTEGER"}
      - {"age": ["INTEGER","numeric"]}  (single-key style)
      - {"age": {"type":"INTEGER", "aliases":[...], "labels":[...]}} (nested meta)
    """
    def _coerce_types(v: Any) -> list[str]:
        if v is None:
            return []
        if isinstance(v, list):
            return [str(x).strip() for x in v if str(x).strip()]
        # handle dict-like strings by dropping them (we only want simple atoms here)
        if isinstance(v, str):
            s = v.strip()
            return [] if (s.startswith("{") and s.endswith("}")) else [s]
        if isinstance(v, dict):
            # prefer 'type' or 'slot_types' keys if present
            for k in ("types", "slot_types", "type", "slot_type"):
                if k in v:
                    return _coerce_types(v.get(k))
            return []
        return [str(v).strip()]

    # 1) Bare string
    if isinstance(col_spec, str):
        cname = col_spec.strip()
        if not cname:
            return None
        return {"fqn": f"{table}.{cname}", "table": table, "name": cname, "types": []}

    # 2) Mapping
    if isinstance(col_spec, dict):
        # (a) explicit name form
        cname = col_spec.get("name") or col_spec.get("column") or col_spec.get("id")
        types: list[str] | None = None

        for key in ("types", "slot_types", "type", "slot_type"):
            if key in col_spec:
                types = _coerce_types(col_spec.get(key))
                break

        # (b) single-key style: {"age": <meta>}
        if cname is None and len(col_spec) == 1:
            only_key, only_val = next(iter(col_spec.items()))
            cname = str(only_key).strip()
            types = _coerce_types(only_val)

        if cname:
            return {"fqn": f"{table}.{cname}", "table": table, "name": cname, "types": types or []}

    return None



def collect_table_rows(schema: dict) -> list[dict]:
    pairs = iter_table_defs(schema)
    return [{"n": t} for t, _ in pairs]


def _parse_dictlike_literal(s: str) -> Dict[str, Any] | None:
    """
    If s looks like a Python dict literal (single-quoted, as often seen in your inputs),
    try to parse it safely. Returns a plain dict with string keys, or None on failure.
    """
    try:
        if not isinstance(s, str) or not _DICTLIKE_RE.match(s):
            return None
        obj = ast.literal_eval(s)
        if not isinstance(obj, dict):
            return None
        # ensure string keys
        return {str(k): v for k, v in obj.items()}
    except Exception:
        return None


def _preferred_name_from_meta(meta: Dict[str, Any]) -> str:
    """
    Derive a readable column name from parsed meta:
      - If 'aliases' exists, prefer 'name' if present; otherwise one of:
        identifier, label, title, names; otherwise first alias.
      - If 'name' key exists, use it.
      - Fallback to 'col'.
    """
    # aliases may be any case; we want the literal string for output, not lowercased
    aliases = meta.get("aliases") or []
    if isinstance(aliases, (list, tuple)) and aliases:
        # Case-insensitive check for 'name' among aliases
        for a in aliases:
            if isinstance(a, str) and a.strip().lower() == "name":
                return "name"
        # Next preference list, case-insensitive
        prefs = ["identifier", "label", "title", "names"]
        lowered = [a.strip().lower() for a in aliases if isinstance(a, str)]
        for p in prefs:
            if p in lowered:
                # return the original-cased alias which matched
                for a in aliases:
                    if isinstance(a, str) and a.strip().lower() == p:
                        return a.strip()
        # Else just use the first alias
        for a in aliases:
            if isinstance(a, str) and a.strip():
                return a.strip()

    # Else try 'name' in meta
    if isinstance(meta.get("name"), str) and meta["name"].strip():
        return meta["name"].strip()

    return "col"


def collect_column_rows(schema: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Return a list of rows with shape:
        {"fqn": "<table>.<column>", "table": "<table>", "name": "<column>", "types": [<db-type strings>]}
    Robust to:
      * top-level `columns` mapping with dict-like keys/values (your "regions.{...}" case),
      * nested dict-like keys under `tables.<table>.columns`,
      * lists of dicts under `tables.<table>.columns`,
      * simple strings as column names.

    This function guarantees that returned FQNs never contain '{' or '}'.
    """
    rows: List[Dict[str, Any]] = []

    def _emit(table: str, name: str, types: List[str] | None):
        # sanitize name if somehow dict-like slipped through
        if "{" in name or "}" in name:
            name = "col"
        fqn = f"{table}.{name}"
        if "{" in fqn or "}" in fqn:
            # last-resort scrub
            fqn = f"{table}.col"
            name = "col"
        rows.append({"fqn": fqn, "table": table, "name": name, "types": [t for t in (types or []) if isinstance(t, str) and t.strip()]})

    # Helper to infer types from a meta dict or dict-like string
    def _types_from_meta(meta_like: Any) -> List[str]:
        meta = None
        if isinstance(meta_like, dict):
            meta = meta_like
        elif isinstance(meta_like, str):
            meta = _parse_dictlike_literal(meta_like)
        if not isinstance(meta, dict):
            return []
        t = meta.get("type")
        ts = meta.get("types")
        out: List[str] = []
        if isinstance(t, str) and t.strip():
            out.append(t.strip())
        if isinstance(ts, (list, tuple)):
            for x in ts:
                if isinstance(x, str) and x.strip():
                    out.append(x.strip())
        return out

    # 1) Top-level 'columns' block: may contain FQN keys and dict-like values
    top_cols = (schema.get("columns") or {}) if isinstance(schema.get("columns"), dict) else {}
    for key, val in top_cols.items():
        # Expect key like "table.col" or broken "table.{...}"
        if isinstance(key, str) and "." in key:
            table, raw_col = key.split(".", 1)
            if "{" in raw_col or "}" in raw_col:
                # try to parse dict-like key to find a better name
                meta = _parse_dictlike_literal(raw_col)
                name = _preferred_name_from_meta(meta or {})
            else:
                name = raw_col
            types = _types_from_meta(val)
            _emit(table, name, types)

    # 2) Nested under tables.<table>.columns:
    tables = schema.get("tables") or {}
    if isinstance(tables, dict):
        for table, tdef in tables.items():
            cols = (tdef or {}).get("columns")
            if cols is None:
                continue

            # Case A: list of dicts or strings
            if isinstance(cols, list):
                for c in cols:
                    # dict with explicit name
                    if isinstance(c, dict) and isinstance(c.get("name"), str):
                        name = c["name"].strip()
                        types = []
                        if isinstance(c.get("type"), str) and c["type"].strip():
                            types = [c["type"].strip()]
                        elif isinstance(c.get("types"), (list, tuple)):
                            types = [x.strip() for x in c["types"] if isinstance(x, str) and x.strip()]
                        _emit(table, name, types)
                    # plain string column name
                    elif isinstance(c, str) and c.strip():
                        _emit(table, c.strip(), [])
                    # dict-like single-key form: {"price": {"type": "..."}}
                    elif isinstance(c, dict) and len(c) == 1:
                        k, v = list(c.items())[0]
                        if isinstance(k, str):
                            _emit(table, k.strip(), _types_from_meta(v))
                        else:
                            # unexpected, skip
                            continue
                    # else ignore malformed
            # Case B: dict mapping name->meta; keys themselves may be dict-like strings
            elif isinstance(cols, dict):
                for k, v in cols.items():
                    if isinstance(k, str):
                        if "{" in k or "}" in k:
                            meta_k = _parse_dictlike_literal(k)
                            name = _preferred_name_from_meta(meta_k or {})
                        else:
                            name = k.strip()
                        _emit(table, name, _types_from_meta(v))
                    else:
                        # Very odd; try to coerce via value meta
                        types = _types_from_meta(v)
                        _emit(table, "col", types)

    return rows



def collect_functions_from_schema(schema: dict) -> list[dict]:
    """
    Optional functions block in schema:
      functions:
        order_by:
          template: "{column}"
          aliases: ["order by"]
          requirements: [{arg:"column", st:"any"}]
          placement: "clause"
          bind_style: "of"
    """
    rows: list[dict] = []
    fblock = schema.get("functions") or {}
    if not isinstance(fblock, dict):
        return rows
    for name, finfo in fblock.items():
        reqs = []
        for r in (finfo.get("requirements") or finfo.get("reqs") or []):
            arg = r.get("arg")
            st = r.get("st") or r.get("slot_type")
            if arg and st:
                reqs.append({"arg": arg, "st": st})
        rows.append({
            "name": str(name),
            "template": finfo.get("template", ""),
            "aliases": normalize_aliases(finfo.get("aliases", [])),
            "reqs": reqs,
            "placement": finfo.get("placement"),
            "bind_style": finfo.get("bind_style"),
        })
    return rows


# ---------- actions extraction from vocabulary ----------

def extract_keywords_root(kf: dict) -> dict:
    """Allow both nested and flat styles; return the 'keywords' map."""
    if "keywords" in kf and isinstance(kf["keywords"], dict):
        return kf["keywords"]
    return kf  # tolerate legacy flat files


def collect_actions_from_keywords(kw: dict, role: str = "sql_action") -> dict:
    """
    Returns a normalized map of actions under the given role name from the keywords object.
    We support: keywords.sql_actions and keywords.postgis_actions
    """
    key = "sql_actions" if role == "sql_action" else "postgis_actions"
    actions = kw.get(key) or {}
    out = {}
    for name, meta in actions.items():
        aliases = normalize_aliases(meta.get("aliases", []))
        template = str(meta.get("template", ""))
        placement = meta.get("placement") or "projection"
        bind_style = meta.get("bind_style") or ("of" if "{column}" in template or "{value}" in template else "to")
        applicable_types = meta.get("applicable_types") or {}
        # flatten to requirements (arg, st)
        reqs: list[dict] = []
        for arg, types in applicable_types.items():
            for st in (types or []):
                reqs.append({"arg": str(arg), "st": str(st)})
        out[str(name)] = {
            "template": template,
            "aliases": aliases,
            "placement": placement,
            "bind_style": bind_style,
            "applicable_types": {str(k): [str(x) for x in (v or [])] for k, v in applicable_types.items()},
            "reqs": reqs,
        }
    return out


def connectors_from_keywords(kw: dict) -> dict:
    # accept "connectors" or "CONNECTORS"
    raw = kw.get("connectors") or kw.get("CONNECTORS") or {}
    return ensure_core_connectors(raw)


def select_template_from_keywords(kw: dict) -> str:
    gt = kw.get("global_templates") or {}
    return gt.get("select_template") or "select {columns} from {table}"
