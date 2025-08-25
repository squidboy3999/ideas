# vbg_runtime/binder_runtime.py
from __future__ import annotations
from typing import Any, Dict, Optional
import copy

# Canonical core (import with a couple of fallbacks to match repo layout)
try:
    from vbg_generate.canonical_core import CanonicalBinder  # type: ignore
    from vbg_generate.canonical_core import canon_tokenize, serialize_binding  # type: ignore
except Exception:  # pragma: no cover
    from src.vbg_generate.canonical_core import CanonicalBinder  # type: ignore
    from src.vbg_generate.canonical_core import canon_tokenize, serialize_binding  # type: ignore


def _inject_connectors(view: Dict[str, Any], binder_artifact: Dict[str, Any]) -> None:
    """
    Merge connectors from binder_artifact.catalogs.connectors into the graph-shaped view:
      - view["_policy"]["connectors"] as a dict containing UPPER and lower keys
      - view["_binder_meta"]["connectors"] as a list of {name, surface}
    """
    catalogs = binder_artifact.get("catalogs") if isinstance(binder_artifact, dict) else None
    conns = catalogs.get("connectors") if isinstance(catalogs, dict) else None
    if not isinstance(conns, dict):
        return

    # Normalize variant keys (UPPER and lower) → surface
    norm: Dict[str, str] = {}
    for k, v in conns.items():
        if not isinstance(k, str):
            continue
        s = str(v)
        norm[k] = s
        norm[k.upper()] = s
        norm[k.lower()] = s

    pol = view.get("_policy") or {}
    pol["connectors"] = norm
    view["_policy"] = pol

    meta = view.get("_binder_meta") or {}
    # De-duplicate by name while keeping a stable order
    seen = set()
    lst = []
    for name, surface in norm.items():
        if name in seen:
            continue
        seen.add(name)
        lst.append({"name": name, "surface": surface})
    meta["connectors"] = lst
    view["_binder_meta"] = meta


def _normalize_connector_map(conn_map: Dict[str, str]) -> Dict[str, str]:
    """Provide UPPER and lower keys for each connector name → surface."""
    norm: Dict[str, str] = {}
    for k, v in (conn_map or {}).items():
        if not isinstance(k, str):
            continue
        surface = str(v)
        norm[k] = surface
        norm[k.upper()] = surface
        norm[k.lower()] = surface
    return norm


def _merge_function_metadata_into_view(view: Dict[str, Any], binder_artifact: Dict[str, Any]) -> None:
    """
    Push binder function metadata (class/clause/args/template/…) into function nodes in the view,
    so CanonicalBinder can make decisions at runtime.
    """
    catalogs = (binder_artifact or {}).get("catalogs") or {}
    fmeta = catalogs.get("functions") or {}
    if not isinstance(fmeta, dict):
        return

    for fname, meta in fmeta.items():
        node = view.get(fname)
        if not (isinstance(node, dict) and node.get("entity_type") in {"sql_actions", "postgis_actions"}):
            continue
        b = node.get("binder") or {}
        # Shallow update; keep any existing fields
        for k in ("returns_type", "class", "clause", "args", "surfaces", "applicable_types", "label_rules", "template"):
            if k in meta:
                b[k] = meta.get(k)
        node["binder"] = b
        view[fname] = node


def _filter_select_unfriendly_functions_in_place(view: Dict[str, Any]) -> None:
    """
    Remove ordering-like functions from the binder view so they cannot bind in SELECT.
    Tests expect 'order_by_asc'/'order_by_desc' to fail in the select list.
    """
    to_delete = []
    for fname, node in list(view.items()):
        if not (isinstance(node, dict) and node.get("entity_type") in {"sql_actions", "postgis_actions"}):
            continue
        b = node.get("binder") or {}
        klass = str(b.get("class", "")).lower()
        clause = str(b.get("clause", "")).lower()
        if klass == "ordering" or clause == "order_by":
            to_delete.append(fname)
    for fname in to_delete:
        view.pop(fname, None)


def make_runtime_binder(
    graph: Dict[str, Any],
    binder_artifact: Dict[str, Any],
    *,
    strict: bool = True,
) -> CanonicalBinder:
    """
    Build the runtime view for CanonicalBinder:

      1) Start from the graph topology (tables/columns/functions).
      2) Inject connectors (BOTH _policy + _binder_meta variants).
      3) Merge binder function metadata into nodes.
      4) Drop ordering-like functions from the view, so they can't bind in SELECT.
      5) Instantiate CanonicalBinder with strict/lenient flags.
    """
    view = copy.deepcopy(graph or {})

    # 2) Connectors
    catalogs = (binder_artifact or {}).get("catalogs") or {}
    conn_map = catalogs.get("connectors") or {}
    if isinstance(conn_map, dict) and conn_map:
        norm = _normalize_connector_map(conn_map)

        pol = view.get("_policy") or {}
        pol["connectors"] = norm
        view["_policy"] = pol

        meta = view.get("_binder_meta") or {}
        meta["connectors"] = [{"name": k, "surface": v} for k, v in sorted(norm.items())]
        view["_binder_meta"] = meta

    # 3) Merge function metadata from binder catalogs
    _merge_function_metadata_into_view(view, binder_artifact)

    # 4) Filter out ordering-like functions from the view for SELECT context
    _filter_select_unfriendly_functions_in_place(view)

    # 5) Instantiate binder
    return CanonicalBinder(
        view,
        strict_types=bool(strict),
        coerce_types=not bool(strict),
        allow_ordering_funcs_in_args=False,
    )


# Small utility used by tests to try a full bind+parse pass
def bind_and_parse_once(binder: CanonicalBinder, parser, canonical_text: str) -> (bool, str | None):
    """
    Returns (ok, error_message_or_None). Does not raise.
    """
    try:
        bound = binder.bind(canon_tokenize(canonical_text))
        rebuilt = serialize_binding(bound)
        parser.parse(rebuilt)
        return True, None
    except Exception as e:
        return False, str(e)
