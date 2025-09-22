# tests/conftest.py
from __future__ import annotations
import os, sys, subprocess, json
from pathlib import Path
import tempfile
import pytest
import yaml

# Make project root importable once here
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# -------------------------------
# Helpers: IO + minimal fallbacks
# -------------------------------

_MIN_KW = {
    "keywords": {
        "connectors": {"AND":"and","OR":"or","NOT":"not","FROM":"from","OF":"of","COMMA":","},
        "select_verbs": {"select": {"aliases": ["show","display","list"]}},
        "comparison_operators": {"greater_than": {"aliases": ["greater than"]}},
        "filler_words": {"_skip": {"aliases": ["please","the"]}},
        "global_templates": {"select_template": "select {columns} from {table}"},
        "sql_actions": {
            "count": {
                "aliases":["count of","number of"],
                "template":"COUNT({column})",
                "placement":"projection","bind_style":"of",
                "applicable_types":{"column":["any"]}
            }
        },
        "postgis_actions": {}
    }
}

_MIN_SCHEMA = {
    "tables": {
        "users": {"columns": [{"name":"id","types":["int"]}, {"name":"age","types":["int"]}, "email"]},
        "sales": {"columns": [{"amount":["numeric"]}, {"product_id":"int"}]}
    },
    "functions": {
        "order_by":      {"template":"{column}","aliases":["order by"],      "requirements":[{"arg":"column","st":"any"}],"placement":"clause","bind_style":"of"},
        "order_by_desc": {"template":"{column}","aliases":["order by desc"], "requirements":[{"arg":"column","st":"any"}],"placement":"clause","bind_style":"of"},
    }
}

def _write_yaml(path: Path, obj: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        yaml.safe_dump(obj, f, sort_keys=False, allow_unicode=True)

def _load_yaml(path: Path):
    if not path.exists():
        raise AssertionError(f"Expected file missing: {path}")
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)

# ------------------------------------
# Session setup: build artifacts once
# ------------------------------------

@pytest.fixture(scope="session")
def art_dir(tmp_path_factory) -> Path:
    """
    Build artifacts into a fresh temp dir for this test session.
    Inputs can be overridden via env:
      KEYWORDS_PATH, SCHEMA_PATH → paths to YAMLs
    If not present, use repo-root defaults; if still absent, write minimal fallbacks.
    Optionally set SURFACES_CMD to generate gold/multipath/invalid surfaces after artifacts.
    """
    out = tmp_path_factory.mktemp("artifacts")

    # Locate inputs
    kw_path = os.environ.get("KEYWORDS_PATH")
    sc_path = os.environ.get("SCHEMA_PATH")

    if not kw_path:
        default_kw = ROOT / "keywords_and_functions.yaml"
        kw_path = str(default_kw) if default_kw.exists() else None
    if not sc_path:
        default_sc = ROOT / "schema.yaml"
        sc_path = str(default_sc) if default_sc.exists() else None

    # If either missing, write minimal fallbacks into the temp dir
    if not kw_path:
        _write_yaml(out / "keywords_and_functions.yaml", _MIN_KW)
        kw_path = str(out / "keywords_and_functions.yaml")
    if not sc_path:
        _write_yaml(out / "schema.yaml", _MIN_SCHEMA)
        sc_path = str(out / "schema.yaml")

    # Generate artifacts (call offline generator programmatically)
    from vbg_tools.generate_artifacts import main as gen_main
    rc = gen_main(["--keywords", kw_path, "--schema", sc_path, "--out", str(out), "--quiet"])
    if rc != 0:
        raise AssertionError(f"Artifact generation failed with exit code {rc}")

    # Optionally generate surfaces via a user-supplied command
    surf_cmd = os.environ.get("SURFACES_CMD", "").strip()
    if surf_cmd:
        # The command can reference {out} for the artifact dir
        cmd = surf_cmd.format(out=str(out))
        completed = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        if completed.returncode != 0:
            raise AssertionError(f"Surface generation failed.\nSTDOUT:\n{completed.stdout}\nSTDERR:\n{completed.stderr}")

    # Make it discoverable for any legacy code
    os.environ["ART_DIR"] = str(out)
    return out

# -----------------------------
# Artifact path conveniences
# -----------------------------

@pytest.fixture(scope="session")
def vocab_path(art_dir: Path) -> Path:
    return art_dir / "graph_vocabulary.yaml"

@pytest.fixture(scope="session")
def binder_path(art_dir: Path) -> Path:
    return art_dir / "graph_binder.yaml"

@pytest.fixture(scope="session")
def grammar_path(art_dir: Path) -> Path:
    return art_dir / "graph_grammar.lark"

@pytest.fixture(scope="session")
def gold_path(art_dir: Path) -> Path:
    return art_dir / "gold_surfaces.yml"

@pytest.fixture(scope="session")
def multipath_path(art_dir: Path) -> Path:
    return art_dir / "multipath_surfaces.yml"

@pytest.fixture(scope="session")
def invalid_path(art_dir: Path) -> Path:
    return art_dir / "invalid_surfaces.yml"

# -----------------------------
# Artifact loaders
# -----------------------------

@pytest.fixture(scope="session")
def vocab(vocab_path: Path):
    return _load_yaml(vocab_path)

@pytest.fixture(scope="session")
def binder(binder_path: Path):
    return _load_yaml(binder_path)

@pytest.fixture(scope="session")
def grammar_text(grammar_path: Path) -> str:
    if not grammar_path.exists():
        raise AssertionError(f"Expected file missing: {grammar_path}")
    return grammar_path.read_text(encoding="utf-8")

# --------------------------------------------
# Optional runtime invocation for quick checks
# --------------------------------------------

@pytest.fixture(scope="session")
def runtime_cmd():
    cmd = os.environ.get("RUNTIME_PARSE_CMD")
    if not cmd:
        pytest.skip("Set RUNTIME_PARSE_CMD to enable runtime parse validation (skipping).")
    return cmd

def run_runtime_json(cmd_template: str, nl: str) -> dict:
    cmd = cmd_template.format(nl=nl)
    completed = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    out = (completed.stdout or "").strip()
    if not out:
        raise AssertionError(f"Runtime produced no output.\nSTDERR:\n{completed.stderr}")
    try:
        return json.loads(out)
    except json.JSONDecodeError:
        raise AssertionError(f"Runtime did not return JSON.\nSTDOUT:\n{out}\nSTDERR:\n{completed.stderr}")

# --------------------------------
# Optional surfaces loading/skip
# --------------------------------

@pytest.fixture(scope="session")
def surfaces(gold_path: Path, multipath_path: Path, invalid_path: Path):
    """
    If the three surfaces files exist (or were generated via SURFACES_CMD), load them.
    Otherwise skip the surfaces tests cleanly.
    """
    have_any = gold_path.exists() or multipath_path.exists() or invalid_path.exists()
    if not have_any:
        pytest.skip("Surfaces not present. Set SURFACES_CMD to generate them or provide the files.")
    gold = _load_yaml(gold_path) if gold_path.exists() else []
    multi = _load_yaml(multipath_path) if multipath_path.exists() else []
    invalid = _load_yaml(invalid_path) if invalid_path.exists() else []
    return {"gold": gold or [], "multi": multi or [], "invalid": invalid or []}
