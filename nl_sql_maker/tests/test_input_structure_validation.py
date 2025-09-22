# tests/test_input_structure_validation.py
from __future__ import annotations

import copy
import pytest

# The module you will implement in Step 2 (Part 2)
from vbg_tools import input_structure_validation as v


# ---------------------------
# Helpers: minimal valid data
# ---------------------------

def minimal_valid_keywords() -> dict:
    return {
        "keywords": {
            "connectors": {
                "AND": "and",
                "OR": "or",
                "NOT": "not",
                "FROM": "from",
                "OF": "of",
                "COMMA": ",",
            },
            "select_verbs": {
                "select": {"aliases": ["show", "display", "list"]},
            },
            "comparison_operators": {
                "greater_than": {"aliases": ["greater than"]},
            },
            "filler_words": {"_skip": {"aliases": ["please", "the"]}},
            "global_templates": {
                # Require both placeholders; this is a strong contract for SQL assembly downstream
                "select_template": "select {columns} from {table}"
            },
            "sql_actions": {
                "count": {
                    "aliases": ["count of", "number of", "total number of"],
                    "template": "COUNT({column})",
                    "placement": "projection",
                    "bind_style": "of",
                    "applicable_types": {"column": ["any"]},
                },
                "avg": {
                    "aliases": ["average of", "avg of"],
                    "template": "AVG({column})",
                    "placement": "projection",
                    "bind_style": "of",
                    "applicable_types": {"column": ["numeric"]},
                },
            },
            "postgis_actions": {},
        }
    }


def minimal_valid_schema() -> dict:
    return {
        "tables": {
            "users": {
                "columns": [
                    {"name": "id", "types": ["int"]},
                    {"name": "age", "types": ["int", "numeric"]},
                    "email",  # string-only columns allowed (types optional)
                ]
            },
            "sales": {
                # also support single-key dict shorthand
                "columns": [
                    {"amount": ["numeric"]},
                    {"product_id": "int"},
                ]
            },
        },
        # Optional functions block: if present, must be well-formed
        "functions": {
            "order_by_desc": {
                "template": "{column}",
                "aliases": ["order by desc"],
                "requirements": [{"arg": "column", "st": "any"}],
                "placement": "clause",
                "bind_style": "of",
            },
            "order_by": {
                "template": "{column}",
                "aliases": ["order by"],
                "requirements": [{"arg": "column", "st": "any"}],
                "placement": "clause",
                "bind_style": "of",
            },
        },
    }


# ============================
# Keywords & functions: VALID
# ============================

def test_keywords_valid_minimal_passes():
    data = minimal_valid_keywords()
    # Should not raise
    v.validate_keywords_and_functions(data)


# ===================================
# Keywords & functions: INVALID cases
# ===================================

def test_keywords_missing_keywords_block():
    data = {"connectors": {"AND": "and"}}  # missing 'keywords'
    with pytest.raises(Exception) as e:
        v.validate_keywords_and_functions(data)
    msg = str(e.value)
    assert "keywords" in msg.lower()
    assert "missing" in msg.lower()

def test_keywords_missing_core_connectors():
    data = minimal_valid_keywords()
    # Remove a few core ones
    for k in ("AND", "FROM", "OF"):
        data["keywords"]["connectors"].pop(k, None)
    with pytest.raises(Exception) as e:
        v.validate_keywords_and_functions(data)
    msg = str(e.value)
    # Must enumerate the missing ones
    assert "connectors" in msg.lower()
    assert "missing" in msg.lower()
    assert "AND" in msg
    assert "FROM" in msg
    assert "OF" in msg

def test_keywords_sql_actions_required():
    data = minimal_valid_keywords()
    data["keywords"]["sql_actions"] = {}
    with pytest.raises(Exception) as e:
        v.validate_keywords_and_functions(data)
    msg = str(e.value)
    assert "sql_actions" in msg.lower()
    assert "at least one" in msg.lower()

def test_keywords_action_aliases_must_be_nonempty_strings():
    data = minimal_valid_keywords()
    data["keywords"]["sql_actions"]["count"]["aliases"] = ["", None, 123]
    with pytest.raises(Exception) as e:
        v.validate_keywords_and_functions(data)
    msg = str(e.value)
    assert "aliases" in msg.lower()
    assert "non-empty strings" in msg.lower()
    assert "count" in msg

def test_keywords_action_template_required():
    data = minimal_valid_keywords()
    data["keywords"]["sql_actions"]["avg"]["template"] = ""
    with pytest.raises(Exception) as e:
        v.validate_keywords_and_functions(data)
    msg = str(e.value)
    assert "template" in msg.lower()
    assert "avg" in msg

def test_keywords_action_applicable_types_shape():
    data = minimal_valid_keywords()
    # wrong shapes for applicable_types
    data["keywords"]["sql_actions"]["avg"]["applicable_types"] = {"column": "numeric"}  # should be list
    with pytest.raises(Exception) as e:
        v.validate_keywords_and_functions(data)
    msg = str(e.value)
    assert "applicable_types" in msg
    assert "list of strings" in msg

def test_keywords_select_template_required_and_has_placeholders():
    data = minimal_valid_keywords()
    data["keywords"]["global_templates"]["select_template"] = "select *"
    with pytest.raises(Exception) as e:
        v.validate_keywords_and_functions(data)
    msg = str(e.value)
    assert "select_template" in msg
    assert "{columns}" in msg or "columns placeholder" in msg.lower()
    assert "{table}" in msg or "table placeholder" in msg.lower()

def test_keywords_accepts_optional_sections_but_checks_types():
    data = minimal_valid_keywords()
    # optional: comparison_operators can be empty, but if present its shape must be correct
    data["keywords"]["comparison_operators"] = {"greater_than": {"aliases": ["gt", 5]}}
    with pytest.raises(Exception) as e:
        v.validate_keywords_and_functions(data)
    msg = str(e.value)
    assert "comparison_operators" in msg
    assert "aliases" in msg
    assert "strings" in msg


# ============================
# Schema: VALID
# ============================

def test_schema_valid_minimal_passes():
    data = minimal_valid_schema()
    v.validate_schema(data)  # should not raise


# ============================
# Schema: INVALID cases
# ============================

def test_schema_requires_tables():
    bad = {}
    with pytest.raises(Exception) as e:
        v.validate_schema(bad)
    msg = str(e.value)
    assert "tables" in msg.lower()
    assert "missing" in msg.lower()

def test_schema_tables_must_not_be_empty():
    bad = {"tables": {}}
    with pytest.raises(Exception) as e:
        v.validate_schema(bad)
    msg = str(e.value)
    assert "tables" in msg.lower()
    assert "empty" in msg.lower()

def test_schema_columns_required_somewhere():
    bad = {"tables": {"users": {}}}
    with pytest.raises(Exception) as e:
        v.validate_schema(bad)
    msg = str(e.value)
    assert "columns" in msg.lower()
    assert "at least one" in msg.lower()

def test_schema_column_shapes_supported_and_validated():
    data = minimal_valid_schema()
    # introduce several malformed column specs
    data = copy.deepcopy(data)
    data["tables"]["users"]["columns"].append({"name": ""})       # empty name
    data["tables"]["sales"]["columns"].append(42)                 # not str/dict
    with pytest.raises(Exception) as e:
        v.validate_schema(data)
    msg = str(e.value)
    # Should mention both issues (aggregate reporting)
    assert "users" in msg
    assert "empty" in msg.lower()
    assert "sales" in msg
    assert "unsupported" in msg.lower() or "invalid" in msg.lower()

def test_schema_slot_types_must_be_list_or_string_of_strings():
    data = minimal_valid_schema()
    data = copy.deepcopy(data)
    # Set an invalid types shape
    data["tables"]["sales"]["columns"][0] = {"amount": [1, None, "numeric"]}
    with pytest.raises(Exception) as e:
        v.validate_schema(data)
    msg = str(e.value)
    assert "types" in msg.lower() or "slot types" in msg.lower()
    assert "strings" in msg.lower()

def test_schema_optional_functions_block_if_present_must_be_well_formed():
    data = minimal_valid_schema()
    data = copy.deepcopy(data)
    data["functions"]["order_by"]["requirements"] = [{"arg": "column", "st": None}]
    with pytest.raises(Exception) as e:
        v.validate_schema(data)
    msg = str(e.value)
    assert "functions" in msg.lower()
    assert "requirements" in msg.lower()
    assert "order_by" in msg


# =======================================
# Cross-file expectations & friendly errs
# =======================================

def test_keywords_reports_all_issues_together():
    data = minimal_valid_keywords()
    data = copy.deepcopy(data)
    # Kill multiple things at once to ensure aggregation
    data["keywords"]["connectors"].pop("AND", None)
    data["keywords"]["connectors"].pop("OF", None)
    data["keywords"]["sql_actions"]["count"]["aliases"] = []
    data["keywords"]["global_templates"]["select_template"] = "select *"  # missing placeholders
    with pytest.raises(Exception) as e:
        v.validate_keywords_and_functions(data)
    msg = str(e.value)
    # The message should aggregate multiple issues
    assert "AND" in msg and "OF" in msg
    assert "sql_actions" in msg and "count" in msg and "aliases" in msg
    assert "select_template" in msg and "{columns}" in msg or "columns placeholder" in msg.lower()

def test_schema_reports_all_issues_together():
    data = {"tables": {"users": {"columns": [{"name": ""}, 42]}}, "functions": {"order_by": {"requirements": [{"arg": "column", "st": 1}]}}}
    with pytest.raises(Exception) as e:
        v.validate_schema(data)
    msg = str(e.value)
    # The message should aggregate multiple issues, not just the first one
    assert "users" in msg
    assert "empty" in msg.lower() or "name" in msg.lower()
    assert "unsupported" in msg.lower() or "invalid" in msg.lower()
    assert "functions" in msg.lower()
    assert "strings" in msg.lower()
