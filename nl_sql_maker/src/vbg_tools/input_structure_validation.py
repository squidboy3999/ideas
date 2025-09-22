from __future__ import annotations

from typing import Any, Iterable


class InputValidationError(ValueError):
    """Raised when input YAMLs fail structural validation."""


# -------------------------
# utilities
# -------------------------

def _is_non_empty_str(x: Any) -> bool:
    return isinstance(x, str) and bool(x.strip())


def _all_non_empty_strs(xs: Iterable[Any]) -> bool:
    try:
        return all(_is_non_empty_str(s) for s in xs)
    except TypeError:
        return False


def _require(cond: bool, msg: str, errors: list[str]) -> None:
    if not cond:
        errors.append(msg)


# -------------------------
# keywords & functions.yaml
# -------------------------

_CORE_CONNECTORS = {"AND", "OR", "NOT", "FROM", "OF", "COMMA"}


def validate_keywords_and_functions(obj: dict) -> None:
    """
    Validates structure of keywords_and_functions.yaml.

    Accepts sql_actions either under keywords.sql_actions OR at the YAML top-level (sql_actions),
    validating whichever is present and non-empty.
    """
    errors: list[str] = []

    if not isinstance(obj, dict):
        raise InputValidationError("keywords_and_functions must be a mapping at the top level.")

    kw = obj.get("keywords")
    _require(isinstance(kw, dict), "Missing 'keywords' block (expected a mapping).", errors)
    if not isinstance(kw, dict):
        raise InputValidationError("\n".join(errors))

    # connectors
    raw_conn = (kw.get("connectors") or kw.get("CONNECTORS") or {})
    if not isinstance(raw_conn, dict):
        errors.append("keywords.connectors must be a mapping of NAME -> surface string.")
        raw_conn = {}

    upper_keys = {str(k).upper(): v for k, v in raw_conn.items()}
    missing = sorted(k for k in _CORE_CONNECTORS if k not in upper_keys)
    if missing:
        errors.append(f"keywords.connectors missing core items: {', '.join(missing)}.")
    else:
        for k in _CORE_CONNECTORS:
            v = upper_keys.get(k)
            if not _is_non_empty_str(v):
                errors.append(f"keywords.connectors[{k}] must be a non-empty string.")

    # global_templates.select_template
    gt = kw.get("global_templates") or {}
    sel_tmpl = gt.get("select_template")
    if not _is_non_empty_str(sel_tmpl):
        errors.append("keywords.global_templates.select_template is required and must be a non-empty string.")
    else:
        if "{columns}" not in sel_tmpl:
            errors.append("select_template must include the {columns} placeholder.")
        if "{table}" not in sel_tmpl:
            errors.append("select_template must include the {table} placeholder.")

    # sql_actions may be nested OR top-level; pick whichever is present and non-empty
    actions_map = kw.get("sql_actions")
    actions_ctx = "keywords.sql_actions"
    if not isinstance(actions_map, dict) or not actions_map:
        actions_map = obj.get("sql_actions")
        actions_ctx = "sql_actions (top-level)"
    if not isinstance(actions_map, dict) or not actions_map:
        errors.append(
            "keywords.sql_actions (or top-level sql_actions) must contain at least one action "
            "(non-empty mapping of action -> metadata)."
        )
    else:
        for name, meta in actions_map.items():
            ctx = f"{actions_ctx}[{name}]"
            if not isinstance(meta, dict):
                errors.append(f"{ctx} must be a mapping.")
                continue

            aliases = meta.get("aliases")
            if not isinstance(aliases, list) or not aliases or not _all_non_empty_strs(aliases):
                errors.append(f"{ctx}.aliases must be a non-empty list of non-empty strings.")

            template = meta.get("template")
            if not _is_non_empty_str(template):
                errors.append(f"{ctx}.template must be a non-empty string.")

            apt = meta.get("applicable_types", {})
            if apt is not None:
                if not isinstance(apt, dict):
                    errors.append(f"{ctx}.applicable_types must be a mapping of arg -> list of strings.")
                else:
                    for arg, types in apt.items():
                        if not isinstance(types, list) or not _all_non_empty_strs(types):
                            errors.append(f"{ctx}.applicable_types['{arg}'] must be a list of strings.")

            for opt_key in ("placement", "bind_style"):
                if opt_key in meta and not _is_non_empty_str(meta.get(opt_key)):
                    errors.append(f"{ctx}.{opt_key} must be a non-empty string if provided.")

    # Optional sections: if present, ensure aliases lists are strings.
    sv = kw.get("select_verbs", {})
    if isinstance(sv, dict):
        for can, meta in sv.items():
            if isinstance(meta, dict) and "aliases" in meta:
                aliases = meta.get("aliases")
                if not isinstance(aliases, list) or not _all_non_empty_strs(aliases):
                    errors.append(f"keywords.select_verbs[{can}].aliases must be a list of non-empty strings.")

    comps = kw.get("comparison_operators", {})
    if isinstance(comps, dict):
        for can, meta in comps.items():
            if isinstance(meta, dict) and "aliases" in meta:
                aliases = meta.get("aliases")
                if not isinstance(aliases, list) or not _all_non_empty_strs(aliases):
                    errors.append("keywords.comparison_operators aliases must be a list of non-empty strings.")
                    break

    fw = kw.get("filler_words", {})
    if isinstance(fw, dict) and "_skip" in fw:
        sk = fw["_skip"]
        if isinstance(sk, dict) and "aliases" in sk:
            aliases = sk.get("aliases")
            if not isinstance(aliases, list) or not _all_non_empty_strs(aliases):
                errors.append("keywords.filler_words._skip.aliases must be a list of non-empty strings.")

    if errors:
        raise InputValidationError("Input validation failed for keywords_and_functions:\n- " + "\n- ".join(errors))


# ------------
# schema.yaml
# ------------

def _normalize_columns_list(cols: Any) -> list:
    if isinstance(cols, list):
        return cols
    if isinstance(cols, dict):
        return [{k: v} for k, v in cols.items()]
    return []


def validate_schema(obj: dict) -> None:
    errors: list[str] = []

    if not isinstance(obj, dict):
        raise InputValidationError("schema must be a mapping at the top level.")

    tables = obj.get("tables")
    if tables is None:
        errors.append("Missing 'tables' section.")
        raise InputValidationError("Input validation failed for schema:\n- " + "\n- ".join(errors))

    # Build (table_name, table_def) pairs
    if isinstance(tables, dict):
        pairs = [(str(k), v) for k, v in tables.items()]
    elif isinstance(tables, list):
        pairs = [(str(k), {}) for k in tables]
    else:
        raise InputValidationError("Input validation failed for schema:\n- 'tables' must be a mapping or list.")

    if not pairs:
        errors.append("'tables' must not be empty.")

    total_valid_columns = 0

    for tname, tdef in pairs:
        # locate columns: support columns/cols/fields and nested table.schema.columns
        cols = None
        if isinstance(tdef, dict):
            cols = tdef.get("columns") or tdef.get("cols") or tdef.get("fields")
            if cols is None and isinstance(tdef.get("schema"), dict):
                cols = tdef["schema"].get("columns")
        elif isinstance(tdef, list):
            cols = tdef

        # Normalize mapping → list of single-key dicts
        if isinstance(cols, dict):
            cols = [{k: v} for k, v in cols.items()]
        if not isinstance(cols, list):
            cols = []

        if not cols:
            # allowed per table, but overall we require at least one column
            continue

        for spec in cols:
            # --- string column name ---
            if isinstance(spec, str):
                if not _is_non_empty_str(spec):
                    errors.append(f"Table '{tname}': column name must be a non-empty string.")
                    continue
                total_valid_columns += 1
                continue

            # --- dict column spec ---
            if isinstance(spec, dict):
                # Try direct named form first
                cname_val = spec.get("name") or spec.get("column") or spec.get("id")
                cname = cname_val if _is_non_empty_str(cname_val) else None

                # Gather candidate "types" value from common keys
                ctypes = None
                if "types" in spec:
                    ctypes = spec.get("types")
                elif "slot_types" in spec:
                    ctypes = spec.get("slot_types")
                elif "type" in spec:
                    ctypes = spec.get("type")
                elif "slot_type" in spec:
                    ctypes = spec.get("slot_type")

                # Single-key shorthand (including {'name': {...}} where 'name' is actually the column name)
                if cname is None and len(spec) == 1:
                    k, v = next(iter(spec.items()))
                    # This k is the actual column name
                    cname = k if _is_non_empty_str(k) else None
                    ctypes = v if ctypes is None else ctypes

                if not _is_non_empty_str(cname):
                    errors.append(f"Table '{tname}': column 'name' must be a non-empty string.")
                    continue

                # Validate ctypes (if present):
                #  - list → must be list[str]
                #  - str  → non-empty
                #  - dict → ACCEPT (DB metadata like {'db_type': 'int4', ...})
                #  - other scalars (int/None/etc.) → ACCEPT (we won't block DB outputs here)
                if ctypes is not None:
                    if isinstance(ctypes, list):
                        if not _all_non_empty_strs(ctypes):
                            errors.append(f"Table '{tname}': types/slot_types for '{cname}' must be strings.")
                            # don't count this column as valid yet; continue to next
                            continue
                    elif isinstance(ctypes, str):
                        if not _is_non_empty_str(ctypes):
                            errors.append(f"Table '{tname}': types/slot_types for '{cname}' must be non-empty strings.")
                            continue
                    elif isinstance(ctypes, dict):
                        # ACCEPT dict-shaped DB metadata without complaint
                        pass
                    else:
                        # Accept other scalars (e.g., ints) to be permissive with DB exports
                        pass

                total_valid_columns += 1
                continue

            # --- unsupported column spec type ---
            errors.append(f"Table '{tname}': unsupported column specification: {spec!r}")

    if total_valid_columns == 0:
        # Include the word 'columns' explicitly to satisfy test expectations
        errors.append("At least one column must be defined across all tables (no table defines any columns).")

    # Optional functions block validation
    fns = obj.get("functions")
    if fns is not None:
        if not isinstance(fns, dict):
            errors.append("'functions' must be a mapping of name -> metadata.")
        else:
            for name, meta in fns.items():
                ctx = f"functions[{name}]"
                if not isinstance(meta, dict):
                    errors.append(f"{ctx} must be a mapping.")
                    continue
                reqs = meta.get("requirements") or meta.get("reqs") or []
                if not isinstance(reqs, list):
                    errors.append(f"{ctx}.requirements must be a list of {{arg, st}}")
                else:
                    for r in reqs:
                        if not isinstance(r, dict):
                            errors.append(f"{ctx}.requirements entries must be mappings.")
                            continue
                        if not _is_non_empty_str(r.get("arg")) or not _is_non_empty_str(r.get("st")):
                            errors.append(f"{ctx}.requirements entries must include non-empty 'arg' and 'st' strings.")
                tmpl = meta.get("template")
                if tmpl is not None and not _is_non_empty_str(tmpl):
                    errors.append(f"{ctx}.template must be a non-empty string if provided.")
                aliases = meta.get("aliases")
                if aliases is not None:
                    if not isinstance(aliases, list) or not _all_non_empty_strs(aliases):
                        errors.append(f"{ctx}.aliases must be a list of non-empty strings if provided.")
                for opt in ("placement", "bind_style"):
                    if opt in meta and not _is_non_empty_str(meta.get(opt)):
                        errors.append(f"{ctx}.{opt} must be a non-empty string if provided.")

    if errors:
        raise InputValidationError("Input validation failed for schema:\n- " + "\n- ".join(errors))
