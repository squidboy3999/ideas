# tests/test_artifact_loading_runtime.py
import os
import io
import yaml
import pytest
from typing import Any, Dict

from vbg_runtime.artifacts_loader import load_artifacts, validate_artifacts, ArtifactLoadError


OUT_DIR = os.environ.get("VGB_OUT_DIR", "out")


@pytest.mark.skipif(
    not (os.path.exists(os.path.join(OUT_DIR, "h_graph_with_artifacts.yaml"))
         and os.path.exists(os.path.join(OUT_DIR, "h_vocabulary.yaml"))
         and os.path.exists(os.path.join(OUT_DIR, "h_binder.yaml"))
         and os.path.exists(os.path.join(OUT_DIR, "h_grammar.lark"))),
    reason="Phase H artifacts not found in OUT_DIR (set VGB_OUT_DIR).",
)
def test_happy_path_loads_and_validates():
    graph, vocab, binder, grammar = load_artifacts(OUT_DIR)

    assert isinstance(graph, dict) and "_artifacts" in graph
    assert isinstance(vocab, dict)
    assert isinstance(vocab.get("deterministic_aliases"), dict)
    assert isinstance(vocab.get("non_deterministic_aliases"), dict)

    catalogs = binder.get("catalogs") or {}
    assert isinstance(catalogs.get("columns"), dict)
    assert isinstance(catalogs.get("functions"), dict)
    connectors = catalogs.get("connectors") or {}
    assert isinstance(connectors, dict)

    # Spot check connectors (names may vary by your Phase-F policy; use expected keys if known)
    # Commonly: AND / COMMA / OF / FROM
    for k in ("AND", "COMMA", "OF", "FROM"):
        assert k in connectors or k.lower() in connectors, f"connector '{k}' missing"


def _write(path: str, text: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)


def test_missing_files_raise_clear_errors(tmp_path):
    # Only write one file; others missing
    _write(tmp_path / "h_graph_with_artifacts.yaml", yaml.safe_dump({"_artifacts": {}}))

    with pytest.raises(ArtifactLoadError) as ei:
        load_artifacts(str(tmp_path))
    msg = str(ei.value)
    assert "Missing vocabulary" in msg or "Missing binder" in msg or "Missing grammar" in msg


def test_malformed_yaml_raises(tmp_path):
    # Create all files but make one malformed
    _write(tmp_path / "h_graph_with_artifacts.yaml", yaml.safe_dump({"_artifacts": {}}))
    _write(tmp_path / "h_vocabulary.yaml", ":\n  - not valid yaml\n")
    _write(tmp_path / "h_binder.yaml", yaml.safe_dump({"catalogs": {"columns": {}, "functions": {}, "connectors": {}}}))
    _write(tmp_path / "h_grammar.lark", "start: \"ok\"")

    with pytest.raises(ArtifactLoadError) as ei:
        load_artifacts(str(tmp_path))
    assert "Failed to parse vocabulary" in str(ei.value)


def test_validate_shapes_clear_errors(tmp_path):
    # Valid files but wrong shapes to hit specific validators
    _write(tmp_path / "h_graph_with_artifacts.yaml", yaml.safe_dump({}))  # no _artifacts
    _write(tmp_path / "h_vocabulary.yaml", yaml.safe_dump({"deterministic_aliases": {}, "non_deterministic_aliases": {}}))
    _write(tmp_path / "h_binder.yaml", yaml.safe_dump({"catalogs": {"columns": {}, "functions": {}, "connectors": {}}}))
    _write(tmp_path / "h_grammar.lark", "query: \"select\"")

    graph, vocab, binder, grammar = (
        yaml.safe_load(open(tmp_path / "h_graph_with_artifacts.yaml", "r", encoding="utf-8")),
        yaml.safe_load(open(tmp_path / "h_vocabulary.yaml", "r", encoding="utf-8")),
        yaml.safe_load(open(tmp_path / "h_binder.yaml", "r", encoding="utf-8")),
        open(tmp_path / "h_grammar.lark", "r", encoding="utf-8").read(),
    )

    with pytest.raises(ArtifactLoadError) as ei:
        validate_artifacts(graph, vocab, binder, grammar)
    assert "_artifacts" in str(ei.value)
