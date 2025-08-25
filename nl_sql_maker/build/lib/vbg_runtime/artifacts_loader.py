# runtime/artifacts_loader.py
from __future__ import annotations

import os
from typing import Any, Dict, Tuple

import yaml


class ArtifactLoadError(ValueError):
    """Raised when artifacts are missing or malformed with a clear, actionable message."""


def _require_file(path: str, label: str) -> None:
    if not os.path.exists(path):
        raise ArtifactLoadError(f"Missing {label}: expected file at '{path}'")
    if not os.path.isfile(path):
        raise ArtifactLoadError(f"{label} is not a file: '{path}'")


def _load_yaml(path: str, label: str) -> Dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
    except Exception as e:
        raise ArtifactLoadError(f"Failed to parse {label} YAML at '{path}': {e}") from e
    if data is None:
        # Allow empty dict fallback to improve error messages downstream
        return {}
    if not isinstance(data, dict):
        raise ArtifactLoadError(f"{label} at '{path}' must be a YAML mapping (dict).")
    return data


def _load_text(path: str, label: str) -> str:
    try:
        with open(path, "r", encoding="utf-8") as f:
            text = f.read()
    except Exception as e:
        raise ArtifactLoadError(f"Failed to read {label} at '{path}': {e}") from e
    return text


def validate_artifacts(
    graph: Dict[str, Any],
    vocabulary: Dict[str, Any],
    binder_artifact: Dict[str, Any],
    grammar_text: str,
) -> None:
    """
    Validate the minimal runtime contract for artifacts.
    - graph: dict and includes _artifacts (informational, runtime uses topology)
    - vocabulary: has deterministic_aliases / non_deterministic_aliases
    - binder.catalogs: includes columns, functions, connectors
    - grammar_text: non-empty string
    """
    if not isinstance(graph, dict):
        raise ArtifactLoadError("Graph must be a dict.")
    if "_artifacts" not in graph:
        raise ArtifactLoadError("Graph is missing '_artifacts' (did Phase H run?)")

    if not isinstance(vocabulary, dict):
        raise ArtifactLoadError("Vocabulary must be a dict.")
    det = vocabulary.get("deterministic_aliases")
    nd = vocabulary.get("non_deterministic_aliases")
    if not isinstance(det, dict):
        raise ArtifactLoadError("Vocabulary missing 'deterministic_aliases' mapping.")
    if nd is None:
        # allow absent non_deterministic_aliases as empty mapping
        vocabulary["non_deterministic_aliases"] = {}
        nd = vocabulary["non_deterministic_aliases"]
    if not isinstance(nd, dict):
        raise ArtifactLoadError("Vocabulary 'non_deterministic_aliases' must be a mapping (dict).")

    if not isinstance(binder_artifact, dict):
        raise ArtifactLoadError("Binder artifact must be a dict.")
    catalogs = binder_artifact.get("catalogs")
    if not isinstance(catalogs, dict):
        raise ArtifactLoadError("Binder artifact missing 'catalogs' dict.")
    for key in ("columns", "functions", "connectors"):
        if key not in catalogs:
            raise ArtifactLoadError(f"Binder catalogs missing '{key}'.")
    if not isinstance(catalogs.get("columns"), dict):
        raise ArtifactLoadError("Binder catalogs 'columns' must be a dict.")

def load_artifacts(artifacts_dir: str) -> Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any], str]:
    """
    Load Phase-H outputs from a directory and return:
      (graph, vocabulary, binder_artifact, grammar_text)

    Expected files:
      - h_graph_with_artifacts.yaml
      - h_vocabulary.yaml
      - h_binder.yaml
      - h_grammar.lark
    """
    if not artifacts_dir or not os.path.isdir(artifacts_dir):
        raise ArtifactLoadError(f"Artifacts directory not found: '{artifacts_dir}'")

    graph_path   = os.path.join(artifacts_dir, "h_graph_with_artifacts.yaml")
    vocab_path   = os.path.join(artifacts_dir, "h_vocabulary.yaml")
    binder_path  = os.path.join(artifacts_dir, "h_binder.yaml")
    grammar_path = os.path.join(artifacts_dir, "h_grammar.lark")

    # Ensure files exist
    _require_file(graph_path,   "graph")
    _require_file(vocab_path,   "vocabulary")
    _require_file(binder_path,  "binder")
    _require_file(grammar_path, "grammar")

    # Load
    graph          = _load_yaml(graph_path, "graph")
    vocabulary     = _load_yaml(vocab_path, "vocabulary")
    binderArtifact = _load_yaml(binder_path, "binder")
    grammar_text   = _load_text(grammar_path, "grammar")

    # Make sure ND map exists (empty dict is fine)
    if "non_deterministic_aliases" not in vocabulary or vocabulary["non_deterministic_aliases"] is None:
        vocabulary["non_deterministic_aliases"] = {}

    # Validate shapes early with helpful errors
    validate_artifacts(graph, vocabulary, binderArtifact, grammar_text)

    return graph, vocabulary, binderArtifact, grammar_text


__all__ = ["load_artifacts", "validate_artifacts", "ArtifactLoadError"]
