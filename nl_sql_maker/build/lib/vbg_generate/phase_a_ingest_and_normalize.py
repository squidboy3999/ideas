# phase_a_ingest_and_normalize.py
from __future__ import annotations
import re
import os
import hashlib
from typing import Dict, Any, Tuple, List, Iterable, Optional

try:
    import yaml
except Exception as e:
    raise RuntimeError("PyYAML is required for phase A. pip install pyyaml") from e


# -----------------------------
# Utilities
# -----------------------------

_WHITESPACE_RE = re.compile(r"\s+")
_NON_ALNUM_RE = re.compile(r"[^a-z0-9_]+")

def _lower_snake(s: str) -> str:
    if not isinstance(s, str):
        return s
    s = s.strip().lower()
    s = _WHITESPACE_RE.sub("_", s)
    s = _NON_ALNUM_RE.sub("_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s

def _safe_yaml_load(path: str) -> Dict[str, Any]:
    if not os.path.exists(path):
        raise FileNotFoundError(f"YAML not found: {path}")
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        raise ValueError(f"Top-level YAML must be a mapping (dict): {path}")
    return data

def _sha256_of_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()

# Map a wide range of SQL-ish types to sane buckets used downstream
def _bucket_type(t: str) -> str:
    if not t:
        return "any"
    s = str(t).strip().lower()
    if re.search(r"(int|serial|bigint|smallint|tinyint)", s):
        return "int"
    if re.search(r"(real|double|float|numeric|decimal)", s):
        return "float"
    if re.search(r"(bool)", s):
        return "bool"
    if re.search(r"(date|timestamp|datetime|time)", s):
        return "datetime" if "time" in s or "stamp" in s else "date"
    if re.search(r"(char|text|json|uuid|xml)", s):
        return "text"
    if re.search(r"(point|linestring|polygon|geometry|geography)", s):
        return "geometry"
    return "text"  # safe default

def _norm_labels(labels: Any) -> List[str]:
    out: List[str] = []
    if isinstance(labels, list):
        for x in labels:
            if isinstance(x, str) and x.strip():
                out.append(x.strip().lower())
    return sorted(list(dict.fromkeys(out)))

def _norm_alias_list(v: Any) -> List[str]:
    if not v:
        return []
    if isinstance(v, list):
        return sorted({str(x).strip().lower() for x in v if str(x).strip()})
    return []

def _append_if_unique(lst: List[str], *vals: str) -> None:
    seen = set(lst)
    for v in vals:
        if v and v not in seen:
            lst.append(v); seen.add(v)

# -----------------------------
# A1: Load sources (raw)
# -----------------------------

_RICH_KEYS = {
    "keywords", "sql_actions", "postgis_actions", "comparison_operators",
    "functions", "operators", "connectors",
    "prepositions", "logical_operators", "select_verbs", "filler_words",
}

def load_sources(schema_path: str, keywords_path: str) -> Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
    schema_raw = _safe_yaml_load(schema_path)
    keywords_raw = _safe_yaml_load(keywords_path)

    # Gate: shape check
    if "tables" not in schema_raw or not isinstance(schema_raw["tables"], dict):
        raise AssertionError("schema.yaml must have a top-level 'tables' mapping.")

    if not any(k in keywords_raw for k in _RICH_KEYS):
        raise AssertionError(
            "keywords_and_functions.yaml should define at least one of: "
            "functions/operators/connectors/keywords/sql_actions/postgis_actions/"
            "comparison_operators or the plain buckets (prepositions/logical_operators/select_verbs/filler_words)."
        )

    meta = {
        "schema_path": schema_path,
        "keywords_path": keywords_path,
        "schema_sha256": _sha256_of_file(schema_path),
        "keywords_sha256": _sha256_of_file(keywords_path),
    }
    return schema_raw, keywords_raw, meta


# -----------------------------
# A2: Canonicalize identifiers
# -----------------------------

def canonicalize_identifiers(schema_raw: Dict[str, Any],
                             keywords_raw: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
    Normalize all canonical IDs to lower_snakecase, bucket types, sanitize labels.
    Accept richer keyword/function/operator shapes:
      - functions: top-level 'functions' or 'sql_actions'/'postgis_actions'
      - operators: top-level 'operators' or nested 'keywords.comparison_operators'
      - keyword buckets: can be list[str] OR mapping of canonical -> {aliases: [...]}
      - connectors: optional; if absent, derive minimal set from prepositions/logical_operators
    Preserve rich fields like 'template', 'pattern', 'applicable_types', 'label_rules' for later phases.
    """
    # --- schema normalization (unchanged) ---
    s_out = {"tables": {}}
    for t_name, t_info in (schema_raw.get("tables") or {}).items():
        if not isinstance(t_info, dict):
            raise AssertionError(f"Table '{t_name}' must be a mapping.")
        t_can = _lower_snake(t_name)
        t_aliases = t_info.get("aliases") or []
        if not isinstance(t_aliases, list):
            raise AssertionError(f"Table '{t_name}': 'aliases' must be a list if present.")
        t_aliases_norm = sorted({str(a).strip().lower() for a in t_aliases if str(a).strip()})

        cols_in = (t_info.get("columns") or {})
        if not isinstance(cols_in, dict):
            raise AssertionError(f"Table '{t_name}': 'columns' must be a mapping of column -> spec.")

        cols_out = {}
        for c_name, c_spec in cols_in.items():
            if isinstance(c_spec, dict):
                c_type = _bucket_type(c_spec.get("type"))
                c_labels = _norm_labels(c_spec.get("labels"))
                c_aliases = c_spec.get("aliases") or []
                if not isinstance(c_aliases, list):
                    raise AssertionError(f"Column '{c_name}': 'aliases' must be a list if present.")
                c_aliases_norm = sorted({str(a).strip().lower() for a in c_aliases if str(a).strip()})
            else:
                # allow shorthand: column: "TYPE"
                c_type = _bucket_type(c_spec)
                c_labels = []
                c_aliases_norm = []

            c_can = _lower_snake(c_name)
            cols_out[c_can] = {
                "type": c_type,
                "labels": c_labels,
                "aliases": c_aliases_norm,
            }

        s_out["tables"][t_can] = {
            "aliases": t_aliases_norm,
            "columns": cols_out,
        }

    # Gate: Unique table/column names after normalization
    if len(s_out["tables"]) != len({k for k in s_out["tables"].keys()}):
        raise AssertionError("Duplicate table canonical names after normalization.")
    for t, ti in s_out["tables"].items():
        if not isinstance(ti.get("columns"), dict) or not ti["columns"]:
            raise AssertionError(f"Table '{t}' has no columns.")
        if len(ti["columns"]) != len(set(ti["columns"].keys())):
            raise AssertionError(f"Duplicate column canonical names found under table '{t}'.")

    # -----------------------------
    # Keywords/functions normalization (richer shape)
    # -----------------------------
    k_out: Dict[str, Any] = {}

    # ---- (A) Functions: merge from top-level 'functions' and sql/postgis actions ----
    fn_out: Dict[str, Any] = {}

    def _ingest_functions_block(block: Dict[str, Any]) -> None:
        for fname, fmeta in (block or {}).items():
            if not isinstance(fmeta, dict):
                raise AssertionError(f"Function '{fname}' must be a mapping.")
            f_can = _lower_snake(fname)
            aliases = _norm_alias_list(fmeta.get("aliases"))
            entry = {
                "aliases": aliases,
                # keep rich fields; later phases can use them
                "template": fmeta.get("template") if isinstance(fmeta.get("template"), str) else None,
                "pattern": fmeta.get("pattern") if isinstance(fmeta.get("pattern"), list) else None,
                "binder": fmeta.get("binder") if isinstance(fmeta.get("binder"), dict) else {},
                "label_rules": list(fmeta.get("label_rules") or []) if isinstance(fmeta.get("label_rules"), list) else [],
                "applicable_types": fmeta.get("applicable_types") if isinstance(fmeta.get("applicable_types"), dict) else {},
            }
            # strip Nones for compactness
            entry = {k: v for k, v in entry.items() if v not in (None, [], {})}
            fn_out[f_can] = entry

    # legacy/simple
    if isinstance(keywords_raw.get("functions"), dict):
        _ingest_functions_block(keywords_raw["functions"])
    # richer split
    if isinstance(keywords_raw.get("sql_actions"), dict):
        _ingest_functions_block(keywords_raw["sql_actions"])
    if isinstance(keywords_raw.get("postgis_actions"), dict):
        _ingest_functions_block(keywords_raw["postgis_actions"])
    # also, some configs put functions under keywords.functions
    if isinstance(keywords_raw.get("keywords"), dict) and isinstance(keywords_raw["keywords"].get("functions"), dict):
        _ingest_functions_block(keywords_raw["keywords"]["functions"])

    if fn_out:
        k_out["functions"] = fn_out

    # ---- (B) Operators: merge top-level 'operators' and keywords.comparison_operators ----
    ops_out: Dict[str, Any] = {}

    def _ingest_operators_block(block: Dict[str, Any]) -> None:
        for oname, ometa in (block or {}).items():
            if isinstance(ometa, dict):
                aliases = _norm_alias_list(ometa.get("aliases"))
                entry = {
                    "aliases": aliases,
                    # keep rich fields
                    "template": ometa.get("template") if isinstance(ometa.get("template"), str) else None,
                    "pattern": ometa.get("pattern") if isinstance(ometa.get("pattern"), list) else None,
                    "label_rules": list(ometa.get("label_rules") or []) if isinstance(ometa.get("label_rules"), list) else [],
                    "applicable_types": ometa.get("applicable_types") if isinstance(ometa.get("applicable_types"), dict) else {},
                }
                entry = {k: v for k, v in entry.items() if v not in (None, [], {})}
            else:
                # allow shorthand: operator: [aliases...]
                aliases = _norm_alias_list(ometa)
                entry = {"aliases": aliases}
            ops_out[_lower_snake(oname)] = entry

    if isinstance(keywords_raw.get("operators"), dict):
        _ingest_operators_block(keywords_raw["operators"])

    if isinstance(keywords_raw.get("keywords"), dict) and isinstance(keywords_raw["keywords"].get("comparison_operators"), dict):
        _ingest_operators_block(keywords_raw["keywords"]["comparison_operators"])

    if ops_out:
        k_out["operators"] = ops_out

    # ---- (C) Keyword buckets: lists OR mapping-with-aliases ----
    # We normalize to flat lists of strings.
    def _flatten_bucket(value: Any, *, include_keys: bool = True) -> List[str]:
        """
        Accepts:
          - list[str]
          - mapping: canon -> {aliases:[...], ...}
        Returns a flat list of alias surfaces (lowercased, unique).
        If include_keys, also include canonical keys as surfaces.
        """
        out: List[str] = []
        if isinstance(value, list):
            out = _norm_alias_list(value)
        elif isinstance(value, dict):
            acc: List[str] = []
            for k, v in value.items():
                if include_keys and isinstance(k, str) and k.strip():
                    _append_if_unique(acc, k.strip().lower())
                if isinstance(v, dict):
                    _append_if_unique(acc, *(_norm_alias_list(v.get("aliases"))))
                elif isinstance(v, list):
                    _append_if_unique(acc, *(_norm_alias_list(v)))
                elif isinstance(v, str):
                    _append_if_unique(acc, v.strip().lower())
            # uniq + stable
            seen, out = set(), []
            for a in acc:
                if a not in seen:
                    seen.add(a); out.append(a)
        else:
            out = []
        return out

    kw = keywords_raw.get("keywords") or {}

    # select_verbs
    sel_list = _flatten_bucket(
        kw.get("select_verbs", keywords_raw.get("select_verbs")), include_keys=True
    )
    if sel_list:
        k_out["select_verbs"] = sel_list

    # prepositions
    preps = _flatten_bucket(
        kw.get("prepositions", keywords_raw.get("prepositions")), include_keys=True
    )
    if preps:
        k_out["prepositions"] = preps

    # logical_operators (we keep just alias surfaces here; rich templates preserved only under operators if needed)
    logs = _flatten_bucket(
        kw.get("logical_operators", keywords_raw.get("logical_operators")), include_keys=True
    )
    if logs:
        k_out["logical_operators"] = logs

    # filler_words
    fillers = _flatten_bucket(
        kw.get("filler_words", keywords_raw.get("filler_words")), include_keys=True
    )
    if fillers:
        k_out["filler_words"] = fillers

    # ---- (D) Connectors: explicit or derived minimal set ----
    # If provided explicitly (NAME->surface), keep them.
    conns_raw = keywords_raw.get("connectors")
    if isinstance(conns_raw, dict) and conns_raw:
        k_out["connectors"] = {str(k).strip().upper(): str(v).strip().lower()
                               for k, v in conns_raw.items() if str(v).strip()}
    else:
        # derive minimally from buckets; ensure OF/FROM in particular
        conn_map: Dict[str, str] = {}
        # prioritize canonical surfaces if present
        def _pick_surface(pool: List[str], candidates: Iterable[str]) -> Optional[str]:
            cand_set = {c.lower() for c in candidates}
            for s in pool or []:
                if s.lower() in cand_set:
                    return s.lower()
            return None

        # OF / FROM from prepositions
        if preps:
            of_s = _pick_surface(preps, ["of"])
            from_s = _pick_surface(preps, ["from"])
            if of_s:  conn_map["OF"] = of_s
            if from_s: conn_map["FROM"] = from_s

        # AND / OR from logical_operators
        if logs:
            and_s = _pick_surface(logs, ["and"])
            or_s  = _pick_surface(logs, ["or"])
            if and_s: conn_map["AND"] = and_s
            if or_s:  conn_map["OR"]  = or_s

        # sensible defaults if still missing
        conn_map.setdefault("OF", "of")
        conn_map.setdefault("FROM", "from")
        conn_map.setdefault("AND", "and")
        conn_map.setdefault("OR", "or")
        k_out["connectors"] = conn_map

    # Gate: baseline sanity after normalization
    if not any(k in k_out for k in (
        "functions", "operators", "prepositions", "logical_operators",
        "select_verbs", "filler_words", "connectors"
    )):
        raise AssertionError("keywords_and_functions has no recognized sections after normalization.")

    return s_out, k_out


# -----------------------------
# Orchestrator for Phase A
# -----------------------------

def run_phase_a(schema_path: str, keywords_path: str) -> Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
    """
    External entrypoint for Phase A.
    Returns (schema_norm, keywords_norm, meta)
    """
    schema_raw, keywords_raw, meta = load_sources(schema_path, keywords_path)
    schema_norm, keywords_norm = canonicalize_identifiers(schema_raw, keywords_raw)
    return schema_norm, keywords_norm, meta
