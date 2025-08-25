# phase_i_cross_artifact_sanity.py
from __future__ import annotations
from typing import Dict, Any, Optional

# Phase-I helper layer (runtime-aligned)
from .phase_i_helpers import (
    i_roundtrip_suite,
    i_feasibility_suite,
    i_negative_suite,
    i_build_phase_i_log,
)

# -------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------

def _push_diag(graph: Dict[str, Any], bucket: str, payload: Any) -> None:
    graph["_diagnostics"] = graph.get("_diagnostics") or {}
    graph["_diagnostics"].setdefault(bucket, []).append(payload)

def _require_artifacts(graph: Dict[str, Any]) -> Dict[str, Any]:
    arts = graph.get("_artifacts")
    if not isinstance(arts, dict) or not arts:
        raise AssertionError("[I-GATE] Phase H artifacts missing (vocabulary/binder/grammar).")
    if not isinstance(arts.get("vocabulary"), dict):
        raise AssertionError("[I-GATE] Vocabulary missing in artifacts.")
    if not isinstance(arts.get("binder"), dict):
        raise AssertionError("[I-GATE] Binder missing in artifacts.")
    if not isinstance(arts.get("grammar_text"), str) or not arts["grammar_text"].strip():
        raise AssertionError("[I-GATE] Grammar text missing in artifacts.")
    return arts

# -------------------------------------------------------------------
# Orchestrator
# -------------------------------------------------------------------

def run_phase_i(
    graph_after_h: Dict[str, Any],
    *,
    roundtrip_phrases: int = 100,
    roundtrip_threshold: float = 0.95,
    negative_examples: int = 8,
    feasibility_sample_functions: int = 50,
    rng_seed: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Phase I: Cross-artifact sanity (canonical roundtrip, feasibility, negatives).
    Uses phase_i_helpers (runtime-aligned) and writes a full log + structured
    reports into graph['_diagnostics'] under the 'phase_i.*' buckets.

    Hard gates:
      - Roundtrip success rate must meet/exceed `roundtrip_threshold`.
      - No negative example may pass both binder and parser.
    Feasibility is advisory (non-fatal) and logged for triage.
    """
    arts = _require_artifacts(graph_after_h)
    grammar_text = arts["grammar_text"]
    binder_artifact = arts.get("binder")  # preferred by helpers; falls back to graph if absent

    # C1: Roundtrip
    c1_ok, c1_report = i_roundtrip_suite(
        graph_after_h,
        grammar_text,
        phrases=roundtrip_phrases,
        success_threshold=roundtrip_threshold,
        rng_seed=rng_seed,
        binder_artifact=binder_artifact,
    )

    # C2: Feasibility (advisory)
    c2_report = i_feasibility_suite(
        graph_after_h,
        binder_artifact=binder_artifact,
        sample_functions=feasibility_sample_functions,
    )

    # C3: Negatives
    c3_ok, c3_report = i_negative_suite(
        graph_after_h,
        grammar_text,
        k=negative_examples,
        binder_artifact=binder_artifact,
    )

    # Build readable log
    log_text = i_build_phase_i_log(c1_report, c2_report, c3_report)

    # Store diagnostics
    _push_diag(graph_after_h, "phase_i.roundtrip_report", c1_report)
    _push_diag(graph_after_h, "phase_i.feasibility_report", c2_report)
    _push_diag(graph_after_h, "phase_i.negatives_report", c3_report)
    _push_diag(graph_after_h, "phase_i.cross_artifacts_log", log_text)

    # Enforce gates
    if not c1_ok or not c3_ok:
        raise AssertionError(
            "[I-GATE] Cross-artifact sanity failed. "
            "See graph['_diagnostics']['phase_i.cross_artifacts_log'] for details."
        )

    return graph_after_h
