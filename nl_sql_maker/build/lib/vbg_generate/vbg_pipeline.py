# vbg_pipeline.py
from __future__ import annotations
import os
import sys
from typing import Dict, Any

# ---- Phase imports (A → K) ----
from .phase_a_ingest_and_normalize import run_phase_a
from .phase_b_base_graph import run_phase_b
from .phase_c_enriching_and_profiling import run_phase_c
from .phase_d_alias_system import run_phase_d
from .phase_e_function_signatures import run_phase_e
from .phase_f_global_policy_block import run_phase_f
from .phase_g_diagnostics import run_phase_g
from .phase_h_artifact_compilation import run_phase_h
from .phase_i_cross_artifact_sanity import run_phase_i
from .phase_j_integration_sanity import run_phase_j
from .phase_k_caching_and_provenance import run_phase_k

try:
    import yaml
except Exception as e:
    raise RuntimeError("PyYAML is required. pip install pyyaml") from e


# Change these paths to your project defaults or env vars
SCHEMA_PATH = os.environ.get("SCHEMA_YAML", "/app/schema.yaml")
KEYWORDS_PATH = os.environ.get("KEYWORDS_YAML", "/app/keywords_and_functions.yaml")
OUT_DIR = os.environ.get("OUT_DIR", "/app/out")
os.makedirs(OUT_DIR, exist_ok=True)


def write_yaml(obj: Dict[str, Any], path: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        yaml.safe_dump(obj, f, sort_keys=False)


def write_text(text: str, path: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        f.write(text if text is not None else "")

def _write_phase_j_diags(graph_obj: Dict[str, Any], out_dir: str) -> None:
    """Persist Phase J diagnostics and any structured reports, if present."""
    diags = (graph_obj.get("_diagnostics") or {})
    # Main human-readable log
    if diags.get("phase_j.integration_log"):
        with open(os.path.join(out_dir, "j_integration_log.txt"), "w", encoding="utf-8") as f:
            f.write("\n\n".join(diags["phase_j.integration_log"]))

    # Optional: structured reports, if your Phase J pushes them
    if diags.get("phase_j.random_report"):
        write_yaml(diags["phase_j.random_report"], os.path.join(out_dir, "j_random_report.yaml"))
    if diags.get("phase_j.lossiness_report"):
        write_yaml(diags["phase_j.lossiness_report"], os.path.join(out_dir, "j_lossiness_report.yaml"))
    if diags.get("phase_j.golden_report"):
        write_yaml(diags["phase_j.golden_report"], os.path.join(out_dir, "j_golden_report.yaml"))


def main() -> None:
    print("=== NLQ→SQL VBG Pipeline (A → K) ===")

    # -------- Phase A: Ingest & Normalize --------
    print("\n[Phase A] Ingest & Normalize")
    try:
        schema_norm, keywords_norm, meta = run_phase_a(SCHEMA_PATH, KEYWORDS_PATH)
        write_yaml(schema_norm, os.path.join(OUT_DIR, "a_schema_norm.yaml"))
        write_yaml(keywords_norm, os.path.join(OUT_DIR, "a_keywords_norm.yaml"))
        write_yaml(meta, os.path.join(OUT_DIR, "a_ingest_meta.yaml"))
        print("  ✓ Phase A completed.")
    except Exception as e:
        print("  ✗ Phase A failed:", e)
        sys.exit(1)

    # -------- Phase B: Base Graph --------
    print("\n[Phase B] Build Base Graph")
    try:
        graph_b = run_phase_b(schema_norm, keywords_norm)
        write_yaml(graph_b, os.path.join(OUT_DIR, "b_graph_base.yaml"))
        print("  ✓ Phase B completed.")
    except Exception as e:
        print("  ✗ Phase B failed:", e)
        sys.exit(1)

    # -------- Phase C: Enrichment & Profiling --------
    print("\n[Phase C] Enrichment & Profiling")
    try:
        graph_c = run_phase_c(graph_b)
        write_yaml(graph_c, os.path.join(OUT_DIR, "c_graph_enriched.yaml"))
        print("  ✓ Phase C completed.")
    except Exception as e:
        print("  ✗ Phase C failed:", e)
        sys.exit(1)

    # -------- Phase D: Alias System --------
    print("\n[Phase D] Alias System")
    try:
        vocabulary_d, graph_d = run_phase_d(graph_c)  # <-- unpack BOTH artifacts
        write_yaml(graph_d, os.path.join(OUT_DIR, "d_graph_aliases.yaml"))
        write_yaml(vocabulary_d, os.path.join(OUT_DIR, "d_vocabulary.yaml"))  # <-- save vocab too
        print("  ✓ Phase D completed.")
    except Exception as e:
        print("  ✗ Phase D failed:", e)
        sys.exit(1)

    # -------- Phase E: Function Signatures --------
    print("\n[Phase E] Function Signatures")
    try:
        graph_e = run_phase_e(graph_d)
        write_yaml(graph_e, os.path.join(OUT_DIR, "e_graph_functions.yaml"))
        print("  ✓ Phase E completed.")
    except Exception as e:
        print("  ✗ Phase E failed:", e)
        sys.exit(1)

    # -------- Phase F: Global Policy Block --------
    print("\n[Phase F] Global Policies")
    try:
        graph_f = run_phase_f(graph_e, vocabulary_d)  # <-- pass vocabulary from D
        write_yaml(graph_f, os.path.join(OUT_DIR, "f_graph_policies.yaml"))
        print("  ✓ Phase F completed.")
    except Exception as e:
        print("  ✗ Phase F failed:", e)
        sys.exit(1)

    # -------- Phase G: Diagnostics --------
    print("\n[Phase G] Diagnostics")
    try:
        graph_g = run_phase_g(graph_f, vocabulary_d)
        write_yaml(graph_g, os.path.join(OUT_DIR, "g_graph_diagnostics.yaml"))
        # Optionally also write a flattened diagnostics log if present
        diags = (graph_g.get("_diagnostics") or {})
        if diags:
            write_yaml(diags, os.path.join(OUT_DIR, "g_diagnostics.yaml"))
        print("  ✓ Phase G completed.")
    except Exception as e:
        print("  ✗ Phase G failed:", e)
        sys.exit(1)

    # -------- Phase H: Artifact Compilation (Vocabulary, Binder, Grammar) --------
    print("\n[Phase H] Artifact Compilation")
    try:
        graph_h = run_phase_h(graph_g, vocabulary_d)
        write_yaml(graph_h, os.path.join(OUT_DIR, "h_graph_with_artifacts.yaml"))

        arts = graph_h.get("_artifacts") or {}
        vocab = arts.get("vocabulary")
        binder = arts.get("binder")
        grammar_text = arts.get("grammar_text")

        if isinstance(vocab, dict):
            write_yaml(vocab, os.path.join(OUT_DIR, "h_vocabulary.yaml"))
        if isinstance(binder, dict):
            write_yaml(binder, os.path.join(OUT_DIR, "h_binder.yaml"))
        if isinstance(grammar_text, str):
            write_text(grammar_text, os.path.join(OUT_DIR, "h_grammar.lark"))

        print("  ✓ Phase H completed.")
    except Exception as e:
        print("  ✗ Phase H failed:", e)
        sys.exit(1)

    # -------- Phase I: Cross-Artifact Sanity --------
    print("\n[Phase I] Cross-Artifact Sanity")
    try:
        graph_i = run_phase_i(
            graph_h,
            roundtrip_phrases=100,
            roundtrip_threshold=0.95,
            negative_examples=8,
            feasibility_sample_functions=50,
            # Tip: pin for reproducible failures
            rng_seed=1234,
        )
        write_yaml(graph_i, os.path.join(OUT_DIR, "i_graph_cross_sane.yaml"))

        # Persist Phase I logs if present
        diags = (graph_i.get("_diagnostics") or {})
        if diags.get("phase_i.cross_artifacts_log"):
            with open(os.path.join(OUT_DIR, "i_cross_artifacts_log.txt"), "w", encoding="utf-8") as f:
                f.write("\n\n".join(diags["phase_i.cross_artifacts_log"]))

        print("  ✓ Phase I completed.")
    except Exception as e:
        print("  ✗ Phase I failed:", e)

        # Even on failure, Phase I wrote diagnostics into graph_h. Persist them:
        try:
            # 1) Graph snapshot at failure
            write_yaml(graph_h, os.path.join(OUT_DIR, "i_graph_cross_failed.yaml"))

            # 2) Log text if present
            diags = (graph_h.get("_diagnostics") or {})
            if diags.get("phase_i.cross_artifacts_log"):
                with open(os.path.join(OUT_DIR, "i_cross_artifacts_log.txt"), "w", encoding="utf-8") as f:
                    f.write("\n\n".join(diags["phase_i.cross_artifacts_log"]))

            # 3) Optional: dump grammar used at failure for quick repro
            arts = graph_h.get("_artifacts") or {}
            grammar_text = arts.get("grammar_text")
            if isinstance(grammar_text, str) and grammar_text.strip():
                write_text(grammar_text, os.path.join(OUT_DIR, "i_grammar_at_failure.lark"))
        except Exception as dump_err:
            print("  (Failed to persist Phase I failure artifacts):", dump_err)

        sys.exit(1)

    # -------- Phase J: Integration Sanity --------
    print("\n[Phase J] Integration Sanity (with normalizer)")
    graph_j = graph_i  # default to the input (Phase J mutates its input on failure)
    try:
        graph_j = run_phase_j(
            graph_i,
            random_phrases=100,
            random_threshold=0.90,
            lossiness_phrases=100,
            golden_queries=None,          # or provide a list of golden NL queries
            golden_threshold=1.0,
            max_candidates=50,
            rng_seed=None,
        )
        # Success path: persist graph + diagnostics
        write_yaml(graph_j, os.path.join(OUT_DIR, "j_graph_integration_ok.yaml"))
        _write_phase_j_diags(graph_j, OUT_DIR)
        print("  ✓ Phase J completed.")
    except Exception as e:
        # Failure path: still persist the mutated graph (Phase J pushed diags before raising)
        print("  ✗ Phase J failed:", e)
        # Use whichever graph we have (Phase J mutates its input even on failure)
        fail_graph = graph_j if isinstance(graph_j, dict) else graph_i
        write_yaml(fail_graph, os.path.join(OUT_DIR, "j_graph_integration_failed.yaml"))
        _write_phase_j_diags(fail_graph, OUT_DIR)
        sys.exit(1)

    # -------- Phase K: Caching & Provenance --------
    print("\n[Phase K] Caching & Provenance")
    try:
        graph_k = run_phase_k(
            graph_j,
            source_schema=schema_norm,
            source_keywords=keywords_norm,
            cache_namespace="vbg",
        )
        write_yaml(graph_k, os.path.join(OUT_DIR, "k_graph_final.yaml"))

        prov = graph_k.get("_provenance") or {}
        if prov:
            write_yaml(prov, os.path.join(OUT_DIR, "k_provenance.yaml"))

        cache_keys = graph_k.get("_cache_keys") or {}
        if cache_keys:
            write_yaml(cache_keys, os.path.join(OUT_DIR, "k_cache_keys.yaml"))

        print("  ✓ Phase K completed.")
    except Exception as e:
        print("  ✗ Phase K failed:", e)
        sys.exit(1)

    print("\n=== Done. Artifacts written to:", OUT_DIR, "===\n")


if __name__ == "__main__":
    main()
