# phase_j_integration_sanity.py
from __future__ import annotations
from typing import Dict, Any, Optional

from .phase_j_helpers import (
    j_random_integration_suite,
    j_audit_lossiness_suite,
    j_golden_suite,
    j_build_phase_j_log,
)

def _push_diag(graph: Dict[str, Any], bucket: str, payload: Any) -> None:
    graph["_diagnostics"] = graph.get("_diagnostics") or {}
    graph["_diagnostics"].setdefault(bucket, []).append(payload)

def _require_artifacts(graph: Dict[str, Any]) -> Dict[str, Any]:
    arts = graph.get("_artifacts")
    if not isinstance(arts, dict) or not arts:
        raise AssertionError("[J-GATE] Phase H artifacts missing (vocabulary/binder/grammar).")
    if not isinstance(arts.get("vocabulary"), dict):
        raise AssertionError("[J-GATE] Vocabulary missing in artifacts.")
    if not isinstance(arts.get("grammar_text"), str) or not arts["grammar_text"].strip():
        raise AssertionError("[J-GATE] Grammar text missing in artifacts.")
    return arts

def run_phase_j(
    graph_after_i: Dict[str, Any],
    *,
    random_phrases: int = 100,
    random_threshold: float = 0.90,
    lossiness_phrases: int = 100,
    golden_queries: Optional[list[str]] = None,
    golden_threshold: float = 1.0,
    max_candidates: int = 50,
    rng_seed: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Phase J: Full integration sanity (NL → normalize → bind → parse), randomized plus audit,
    with optional golden queries. Stores the full log and raises if thresholds aren’t met.
    """
    arts = _require_artifacts(graph_after_i)
    vocabulary = arts["vocabulary"]
    grammar_text = arts["grammar_text"]
    binder_artifact = arts.get("binder")  # optional

    # Runtime-ish binder mode (stricter than Phase I)
    binder_mode = {
        "strict_types": True,
        "coerce_types": False,
        "allow_ordering_funcs_in_args": False,
    }

    # I1: Randomized suite
    ok_rand, random_report = j_random_integration_suite(
        graph_after_i,
        vocabulary,
        grammar_text,
        num_phrases=random_phrases,
        success_threshold=random_threshold,
        max_candidates=max_candidates,
        rng_seed=rng_seed,
        binder_artifact=binder_artifact,
        binder_mode=binder_mode,
        sample_failures=3,
    )

    # I2: Lossiness audit (non-fatal)
    lossiness_report = j_audit_lossiness_suite(
        graph_after_i,
        vocabulary,
        grammar_text,
        num_phrases=lossiness_phrases,
        max_candidates=max_candidates,
        rng_seed=rng_seed,
        binder_artifact=binder_artifact,
        binder_mode=binder_mode,
    )

    # I3: Golden set (optional gate)
    golden_report = None
    ok_golden = True
    if golden_queries:
        ok_golden, golden_report = j_golden_suite(
            golden_queries,
            graph_after_i,
            vocabulary,
            grammar_text,
            success_threshold=golden_threshold,
            max_candidates=max_candidates,
            binder_artifact=binder_artifact,
            binder_mode=binder_mode,
        )

    # Store structured reports and human-readable log
    graph_after_i["_diagnostics"] = graph_after_i.get("_diagnostics") or {}
    graph_after_i["_diagnostics"]["phase_j.random_report"] = random_report
    graph_after_i["_diagnostics"]["phase_j.lossiness_report"] = lossiness_report
    if golden_report is not None:
        graph_after_i["_diagnostics"]["phase_j.golden_report"] = golden_report

    phase_log = j_build_phase_j_log(random_report, lossiness_report, golden_report)
    _push_diag(graph_after_i, "phase_j.integration_log", phase_log)

    # Gate
    if not ok_rand or not ok_golden:
        raise AssertionError("[J-GATE] Integration sanity failed. See graph['_diagnostics']['phase_j.integration_log'].")

    return graph_after_i
