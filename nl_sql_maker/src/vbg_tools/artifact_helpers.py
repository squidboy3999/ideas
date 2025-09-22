# vbg_tools/artifact_helpers.py
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple
import yaml


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
    Accepts:
      - "age"
      - {"name":"age","types":["int"]}  / {"name":"age","type":"int"} / {"slot_type":"int"}…
      - {"age":["int","numeric"]} or {"age":"int"}
    Produces: {"fqn": "users.age", "table": "users", "types": [...]}
    """
    # string
    if isinstance(col_spec, str):
        cname = col_spec.strip()
        if not cname:
            return None
        return {"fqn": f"{table}.{cname}", "table": table, "types": []}

    # dict
    if isinstance(col_spec, dict):
        cname = col_spec.get("name") or col_spec.get("column") or col_spec.get("id")
        types = None
        # plural
        if "types" in col_spec:
            types = coerce_types(col_spec.get("types"))
        elif "slot_types" in col_spec:
            types = coerce_types(col_spec.get("slot_types"))
        # singular
        elif "type" in col_spec:
            types = coerce_types(col_spec.get("type"))
        elif "slot_type" in col_spec:
            types = coerce_types(col_spec.get("slot_type"))

        # single-key dict
        if cname is None and len(col_spec) == 1:
            cname, only_val = next(iter(col_spec.items()))
            if types is None:
                types = coerce_types(only_val)

        if cname:
            return {"fqn": f"{table}.{cname}", "table": table, "types": types or []}

    # unsupported
    return None


def collect_table_rows(schema: dict) -> list[dict]:
    pairs = iter_table_defs(schema)
    return [{"n": t} for t, _ in pairs]


def collect_column_rows(schema: dict) -> list[dict]:
    rows: list[dict] = []
    for tname, tdef in iter_table_defs(schema):
        # find columns list
        cols = None
        if isinstance(tdef, dict):
            cols = tdef.get("columns") or tdef.get("cols") or tdef.get("fields")
            if cols is None and isinstance(tdef.get("schema"), dict):
                cols = tdef["schema"].get("columns")
        elif isinstance(tdef, list):
            cols = tdef

        if not cols:
            continue

        if not isinstance(cols, list):
            if isinstance(cols, dict):
                cols = [{k: v} for k, v in cols.items()]
            else:
                continue

        for c in cols:
            row = coerce_column_row(tname, c)
            if row:
                rows.append(row)
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
