# phase_k_caching_and_provenance.py
from __future__ import annotations
from typing import Dict, Any, Optional, Tuple
import json, hashlib, time

# -------------------------------------------------------------------
# Hashing / provenance helpers
# -------------------------------------------------------------------

def _stable_json(obj: Any) -> str:
    # Compact, sorted JSON for stable hashing
    return json.dumps(obj, sort_keys=True, separators=(",", ":"))

def _sha256_hex(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()

def _hash_dict(d: Dict[str, Any]) -> str:
    return _sha256_hex(_stable_json(d))

def _push(graph: Dict[str, Any], key: str, payload: Any) -> None:
    graph[key] = graph.get(key) or {}
    # When key refers to a dict-like store (e.g. _provenance), merge
    if isinstance(graph[key], dict) and isinstance(payload, dict):
        graph[key].update(payload)
    else:
        graph[key] = payload

# -------------------------------------------------------------------
# Provenance builders
# -------------------------------------------------------------------

def _node_provenance(graph: Dict[str, Any]) -> Dict[str, Any]:
    """
    Create a lightweight provenance index:
      canonical -> {entity_type, table?, columns?}
    Useful for diffing graph changes at a glance.
    """
    out: Dict[str, Any] = {}
    for name, node in graph.items():
        if not isinstance(node, dict): 
            continue
        etype = node.get("entity_type")
        if not etype:
            continue
        md = node.get("metadata") or {}
        entry: Dict[str, Any] = {"entity_type": etype}
        if etype == "table":
            entry["columns"] = sorted(list((md.get("columns") or {}).keys()))
        elif etype == "column":
            entry["table"] = md.get("table")
            entry["type"] = md.get("type")
            entry["labels"] = sorted(list(md.get("labels") or []))
        elif etype in ("sql_actions", "postgis_actions"):
            b = node.get("binder") or {}
            entry["class"] = b.get("class")
            entry["args"] = b.get("args")
        out[name] = entry
    return out

def _artifact_summaries(artifacts: Dict[str, Any]) -> Dict[str, str]:
    """
    Hash each artifact for cache keys & quick change detection.
    """
    out: Dict[str, str] = {}
    for k in ("vocabulary", "binder", "grammar_text"):
        v = artifacts.get(k)
        if v is None:
            out[k] = "missing"
        elif isinstance(v, str):
            out[k] = _sha256_hex(v)
        else:
            out[k] = _hash_dict(v)
    return out

# -------------------------------------------------------------------
# Orchestrator
# -------------------------------------------------------------------

def run_phase_k(
    graph_after_j: Dict[str, Any],
    *,
    source_schema: Optional[Dict[str, Any]] = None,
    source_keywords: Optional[Dict[str, Any]] = None,
    cache_namespace: str = "nlqsql",
) -> Dict[str, Any]:
    """
    Phase K: attach caching & provenance metadata.
    - Stable hashes for inputs, graph, and artifacts
    - Node-level provenance snapshot
    - Cache keys with a namespace for external stores
    """
    # Input hashes (if provided; otherwise mark unknown)
    inputs = {
        "schema_hash": _hash_dict(source_schema) if isinstance(source_schema, dict) else "unknown",
        "keywords_hash": _hash_dict(source_keywords) if isinstance(source_keywords, dict) else "unknown",
    }

    # Graph & artifacts hashes
    graph_hash = _hash_dict({k: v for k, v in graph_after_j.items() if not k.startswith("_")})
    artifacts = graph_after_j.get("_artifacts") or {}
    artifact_hashes = _artifact_summaries(artifacts)

    # A single combined token for cache-busting
    combined_token = _sha256_hex(_stable_json({
        "ns": cache_namespace,
        "inputs": inputs,
        "graph": graph_hash,
        "artifacts": artifact_hashes,
    }))

    provenance = {
        "run_epoch": int(time.time()),
        "inputs": inputs,
        "graph_hash": graph_hash,
        "artifact_hashes": artifact_hashes,
        "combined_cache_token": combined_token,
        "node_index": _node_provenance(graph_after_j),
    }

    _push(graph_after_j, "_provenance", provenance)

    # Convenience cache keys for callers
    graph_after_j["_cache_keys"] = {
        "namespace": cache_namespace,
        "graph": graph_hash,
        "vocabulary": artifact_hashes.get("vocabulary", "missing"),
        "binder": artifact_hashes.get("binder", "missing"),
        "grammar": artifact_hashes.get("grammar_text", "missing"),
        "combined": combined_token,
    }
    return graph_after_j
