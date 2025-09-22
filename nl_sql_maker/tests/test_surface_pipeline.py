# tests/test_surface_pipeline.py
from __future__ import annotations
import os
import pytest

MIN_UNCONSTRAINED = int(os.environ.get("MIN_UNCONSTRAINED", "10"))
MIN_CONSTRAINED   = int(os.environ.get("MIN_CONSTRAINED",   "50"))
MIN_MULTIPATH     = int(os.environ.get("MIN_MULTIPATH",     "5"))

def _is_valid_gold_item(item) -> bool:
    return isinstance(item, dict) and "nl" in item and "sqls" in item and isinstance(item["sqls"], list) and item["sqls"]

@pytest.mark.usefixtures("surfaces")
def test_surfaces_files_present_and_nonempty(surfaces):
    gold = surfaces["gold"]
    assert isinstance(gold, list), "gold_surfaces.yml should be a list."
    assert len(gold) > 0, "gold_surfaces.yml is empty. Generate and validate surfaces first."
    # Multipath/invalid may be empty, but should be lists
    assert isinstance(surfaces["multi"], list), "multipath_surfaces.yml should be a list."
    assert isinstance(surfaces["invalid"], list), "invalid_surfaces.yml should be a list."

@pytest.mark.usefixtures("surfaces")
def test_gold_structure_and_distribution(surfaces):
    gold = [g for g in surfaces["gold"] if _is_valid_gold_item(g)]
    assert gold, "No valid items found in gold_surfaces.yml."
    # Constraint distribution (based on SQL WHERE presence)
    unconstrained = [g for g in gold if not any(" where " in s.lower() for s in g["sqls"])]
    constrained   = [g for g in gold if any(" where " in s.lower() for s in g["sqls"])]
    assert len(unconstrained) >= MIN_UNCONSTRAINED, (
        f"Expected at least {MIN_UNCONSTRAINED} unconstrained gold items; found {len(unconstrained)}."
    )
    assert len(constrained) >= MIN_CONSTRAINED, (
        f"Expected at least {MIN_CONSTRAINED} constrained gold items; found {len(constrained)}."
    )

@pytest.mark.usefixtures("surfaces")
def test_multipath_items_have_multiple_sqls(surfaces):
    multi = surfaces["multi"]
    # If no multipath planned, allow empty list; otherwise enforce minimum
    if MIN_MULTIPATH > 0:
        assert len(multi) >= MIN_MULTIPATH, (
            f"Expected at least {MIN_MULTIPATH} multipath items; found {len(multi)}."
        )
    for rec in multi:
        assert isinstance(rec, dict) and "nl" in rec and "sqls" in rec, "Multipath item missing fields."
        assert isinstance(rec["sqls"], list) and len(rec["sqls"]) >= 2, "Multipath item must have >= 2 SQLs."

@pytest.mark.usefixtures("surfaces")
def test_invalid_items_have_reason_if_present(surfaces):
    for rec in surfaces["invalid"]:
        assert isinstance(rec, dict) and "nl" in rec, "Invalid item missing 'nl'."
        # Accept either 'reason' or 'error' as the failure rationale
        assert ("reason" in rec) or ("error" in rec), "Invalid surface should include 'reason' or 'error'."
