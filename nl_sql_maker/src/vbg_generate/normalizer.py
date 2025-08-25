# src/n2s_runtime/normalizer.py
from __future__ import annotations
import re
from dataclasses import dataclass, field
from typing import Callable, Dict, Iterable, List, Sequence, Tuple, Optional, Set

# -----------------------------
# Types
# -----------------------------
Pair = Tuple[str, bool]
Phrase = List[Pair]
NDMap = Dict[str, List[str]]

# -----------------------------
# Flight recorder (optional)
# -----------------------------
@dataclass
class FlightRecorder:
    events: List[Tuple[str, Dict[str, object]]] = field(default_factory=list)
    def log(self, evt: str, **data: object) -> None: self.events.append((evt, data))
    def warn(self, evt: str, **data: object) -> None: self.events.append((f"WARNING:{evt}", data))
    def fail(self, evt: str, **data: object) -> None: self.events.append((f"FAIL:{evt}", data))
    def dump(self, print_fn: Callable[[str], None] = print) -> None:
        for e, d in self.events:
            print_fn(f"{e}: {d}")

# -----------------------------
# Tokenization
# -----------------------------
# Match dotted identifiers FIRST, then other tokens.
TOKENIZER_RE = re.compile(
    r"\|\||&&|<=|>=|!=|==|<>|[A-Za-z0-9_]+(?:\.[A-Za-z0-9_]+)+|[A-Za-z0-9_']+|[^\sA-Za-z0-9_]"
)

def tokenize(s: str) -> List[str]:
    """Lightweight NL tokenizer suitable for alias mapping (keeps dotted ids intact)."""
    return TOKENIZER_RE.findall(s or "")


def squash_spaces(s: str) -> str:
    return " ".join((s or "").split())

# -----------------------------
# Vocabulary shaping (alias → canonicals) + identities
# -----------------------------
_SENTINEL_BLACKLIST = {"", "skip", "_skip"}

def _coerce_listy(v: object) -> List[str]:
    if isinstance(v, list): return [("" if o is None else str(o)) for o in v]
    return ["" if v is None else str(v)]

def _collect_canonicals(det: Dict[str, object], nd: Dict[str, object]) -> List[str]:
    vals: List[str] = []
    for v in det.values():
        if isinstance(v, str) and v not in _SENTINEL_BLACKLIST:
            vals.append(v)
    for v in nd.values():
        if isinstance(v, list):
            for o in v:
                if isinstance(o, str) and o not in _SENTINEL_BLACKLIST:
                    vals.append(o)
        elif isinstance(v, str) and v not in _SENTINEL_BLACKLIST:
            vals.append(v)
    return vals

# -----------------------------
# Vocabulary shaping (alias → canonicals) + identities
# -----------------------------
_SENTINEL_BLACKLIST = {"", "skip", "_skip"}

def _coerce_listy(v: object) -> List[str]:
    if isinstance(v, list): return [("" if o is None else str(o)) for o in v]
    return ["" if v is None else str(v)]

def _collect_canonicals(det: Dict[str, object], nd: Dict[str, object]) -> List[str]:
    vals: List[str] = []
    for v in det.values():
        if isinstance(v, str) and v not in _SENTINEL_BLACKLIST:
            vals.append(v)
    for v in nd.values():
        if isinstance(v, list):
            vals.extend([o for o in v if isinstance(o, str) and o not in _SENTINEL_BLACKLIST])
        elif isinstance(v, str) and v not in _SENTINEL_BLACKLIST:
            vals.append(v)
    return vals

def _graph_canonicals_for_identities(graph: Optional[Dict[str, object]]) -> List[str]:
    if not isinstance(graph, dict): return []
    out: List[str] = []
    for k, v in graph.items():
        if isinstance(v, dict) and v.get("entity_type") in {"table", "column", "sql_actions", "postgis_actions"}:
            out.append(str(k))
    return out

def _augment_with_graph_identities_and_dot_bridge(nd: Dict[str, List[str]],
                                                  graph: Optional[Dict[str, object]]) -> None:
    """
    Make the normalizer robust to dotted tokens that appear in NL even if the vocabulary
    doesn't list them:
      - Add identity entries for every canonical present in the GRAPH.
      - Add 'dot-bridge' alias keys like 'users . balance' -> ['users.balance'].
    """
    if not isinstance(graph, dict): return

    # Identities for graph canonicals
    for c in _graph_canonicals_for_identities(graph):
        nd.setdefault(c, [])
        if c not in nd[c]:
            nd[c].append(c)

    # Dot-bridge for column canonicals: users.balance  <=  users . balance
    for k, v in graph.items():
        if not (isinstance(v, dict) and v.get("entity_type") == "column"):
            continue
        if "." not in k:
            continue
        lhs, rhs = k.split(".", 1)
        spaced_key = f"{lhs} . {rhs}"
        nd.setdefault(spaced_key, [])
        if k not in nd[spaced_key]:
            nd[spaced_key].append(k)

def build_nd_map(vocabulary: Dict[str, Dict[str, object]],
                 graph: Optional[Dict[str, object]] = None) -> NDMap:
    """
    Build non-deterministic alias→canonicals map with canonical identities.
    Also inject identities/dot-bridge from the GRAPH so the normalizer can
    handle dotted canonicals in NL even if the vocabulary avoids them.
    """
    det = vocabulary.get("deterministic_aliases", {}) or {}
    nd  = vocabulary.get("non_deterministic_aliases", {}) or {}

    out: NDMap = {}

    # non-deterministic first
    for k, v in nd.items():
        out[str(k)] = _coerce_listy(v)

    # deterministic (append single target)
    for k, v in det.items():
        canon = "" if (v is None or v in {"skip", "_skip"}) else str(v)
        out.setdefault(str(k), [])
        if canon not in out[str(k)]:
            out[str(k)].append(canon)

    # identities for all canonicals seen in the VOCAB
    for c in _collect_canonicals(det, nd):
        out.setdefault(c, [])
        if c not in out[c]:
            out[c].append(c)

    # NEW: add GRAPH identities and dot-bridge
    _augment_with_graph_identities_and_dot_bridge(out, graph)

    return out


def build_reverse_alias_map(vocabulary: Dict[str, Dict[str, object]]) -> Dict[str, List[str]]:
    """
    canonical → list of aliases (including identity).
    Useful for tests/denormalization only.
    """
    det = vocabulary.get("deterministic_aliases", {}) or {}
    nd  = vocabulary.get("non_deterministic_aliases", {}) or {}

    rev: Dict[str, List[str]] = {}

    # deterministic
    for alias, canonical in det.items():
        c = "" if (canonical is None or canonical in {"skip", "_skip"}) else str(canonical)
        if c == "":
            continue
        rev.setdefault(c, [])
        if alias not in rev[c]:
            rev[c].append(alias)
        if c not in rev[c]:
            rev[c].append(c)

    # non-deterministic
    for alias, clist in nd.items():
        for c in _coerce_listy(clist):
            if c == "":
                continue
            rev.setdefault(c, [])
            if alias not in rev[c]:
                rev[c].append(alias)
            if c not in rev[c]:
                rev[c].append(c)

    # de-dup in place (stable)
    for c, arr in rev.items():
        seen, out = set(), []
        for a in arr:
            if a not in seen:
                seen.add(a); out.append(a)
        rev[c] = out
    return rev

# -----------------------------
# Punctuation passthrough
# -----------------------------
def punctuation_passthrough(tokens: Sequence[str], passthrough: Iterable[str]) -> Phrase:
    pt = set(passthrough)
    return [(t, t in pt) for t in tokens]

# -----------------------------
# BFS segmentation over aliases (leftmost-first)
# -----------------------------
def _max_key_len_words(nd: NDMap) -> int:
    m = 1
    for k in nd.keys():
        L = max(1, len(k.split(" ")))
        if L > m: m = L
    return m

def _serialize_phrase(ph: Phrase) -> Tuple[Tuple[str, bool], ...]:
    return tuple(ph)

def bfs_resolve_leftmost_spans(
    initial: Phrase,
    ndict: NDMap,
    *,
    joiner: str = " ",
    cap_nodes: int = 200,
    cap_results: int = 200,
    warn_every: int = 50,
    fr: Optional[FlightRecorder] = None,
) -> List[str]:
    """
    Core alias→canonical resolver:
      - finds the leftmost unmapped token run
      - tries longest-to-shortest multiword alias spans
      - replaces span with a single canonical token (marked mapped)
      - BFS over possibilities, capped for safety
    Returns: list of *flat canonical strings* (no structural guessing).
    """
    from collections import deque
    q = deque([initial])
    seen = {_serialize_phrase(initial)}
    finals: List[str] = []
    max_len = _max_key_len_words(ndict)
    node_expanded = 0

    while q:
        phrase = q.popleft()

        # find leftmost unmapped
        try:
            i = next(idx for idx, (_, m) in enumerate(phrase) if not m)
        except StopIteration:
            s = joiner.join(t for t, _ in phrase)
            finals.append(s)
            if fr and len(finals) % warn_every == 0:
                fr.warn("final_count", count=len(finals))
            if len(finals) > cap_results:
                if fr: fr.fail("final_cap_exceeded", cap=cap_results, count=len(finals))
                break
            continue

        # contiguous unmapped run [i:r)
        r = i
        while r < len(phrase) and not phrase[r][1]:
            r += 1
        run_len = r - i
        tried = False

        # try spans within run
        for span_len in range(min(max_len, run_len), 0, -1):
            span_text = joiner.join(phrase[k][0] for k in range(i, i + span_len))
            options = ndict.get(span_text)
            if not options:
                continue
            tried = True
            for opt in options:
                new_phrase = phrase[:i] + [(opt, True)] + phrase[i + span_len:]
                key = _serialize_phrase(new_phrase)
                if key in seen:
                    continue
                seen.add(key)
                q.append(new_phrase)
                node_expanded += 1
                if fr and node_expanded % warn_every == 0:
                    fr.warn("node_count", count=node_expanded)
                if node_expanded > cap_nodes:
                    if fr: fr.fail("node_cap_exceeded", cap=cap_nodes, count=node_expanded)
                    return finals

        if not tried:
            if fr:
                fr.log("prune", leftmost=phrase[i][0], run_len=run_len)

    return finals

# -----------------------------
# Public API: normalization (purely lexical)
# -----------------------------
# -----------------------------
# Public API: normalization (purely lexical)
# -----------------------------
_DEFAULT_PASSTHROUGH = (",", "of", "from", "and", "or", "select")

def normalize_text(
    vocabulary: Dict[str, Dict[str, object]],
    text: str,
    *,
    tokenizer: Callable[[str], List[str]] = tokenize,
    joiner: str = " ",
    case_insensitive: bool = False,
    punctuation_as_mapped: Iterable[str] = _DEFAULT_PASSTHROUGH,
    cap_nodes: int = 200,
    cap_results: int = 200,
    warn_every: int = 50,
    fr: Optional[FlightRecorder] = None,
    # legacy args kept for backward compatibility (ignored):
    binder: object = None,
    prefer_from: bool = True,
    # NEW:
    graph: Optional[Dict[str, object]] = None,
) -> List[str]:
    """
    NL → list of *flat canonical strings* in original token order.
    No structural or grammatical guessing is performed here.
    Now robust to:
      - dotted identifiers (tokenized as single tokens),
      - connectors/keywords passed through,
      - identities injected from GRAPH.
    """
    nd = build_nd_map(vocabulary, graph=graph)
    s = text.casefold() if case_insensitive else text
    toks = tokenizer(s)
    if fr: fr.log("tokens", tokens=toks)

    seed = punctuation_passthrough(toks, punctuation_as_mapped)
    if fr: fr.log("seed", phrase=seed)

    finals_raw = bfs_resolve_leftmost_spans(
        initial=seed,
        ndict=nd,
        joiner=joiner,
        cap_nodes=cap_nodes,
        cap_results=cap_results,
        warn_every=warn_every,
        fr=fr,
    )

    # De-dup + trivial hygiene
    BAD_BIGRAMS = {("of", "of"), ("from", "from"), ("and", "and"), (",", ",")}
    seen, flat_canonicals = set(), []
    for f in finals_raw:
        clean = squash_spaces(f)
        if not clean or clean in seen:
            continue
        toks2 = clean.split()
        if any((a, b) in BAD_BIGRAMS for a, b in zip(toks2, toks2[1:])):
            if fr: fr.log("drop_bad_bigram", text=clean)
            continue
        seen.add(clean)
        flat_canonicals.append(clean)

    if fr: fr.log("finals_flat_only", count=len(flat_canonicals))
    return flat_canonicals


# -----------------------------
# Denormalization helpers (for tests)
# -----------------------------
_CONNECTORS: Set[str] = {"of", "from", "and"}
def _bucket_aliases_by_trailing_connector(aliases: List[str]) -> Tuple[List[str], Dict[str, List[str]]]:
    endswith: Dict[str, List[str]] = {c: [] for c in _CONNECTORS}
    plain: List[str] = []
    for a in aliases:
        s = a.strip()
        low = s.lower()
        matched = False
        for c in _CONNECTORS:
            if low.endswith(" " + c):
                endswith[c].append(s); matched = True; break
        if not matched:
            plain.append(s)
    return plain, endswith

def denormalize_phrase(
    canonical_phrase: str,
    reverse_alias_map: Dict[str, List[str]],
    *,
    connectors: Set[str] = _CONNECTORS,
) -> str:
    """
    Replace canonical tokens with aliases while avoiding connector duplication (e.g., 'of of').
    ',' and connector tokens are treated as locked.
    """
    LOCK = {",", "COMMA"} | {c for c in connectors}
    toks = (canonical_phrase or "").split()
    out: List[str] = []
    i = 0
    while i < len(toks):
        t = toks[i]
        if t in LOCK or t == ",":
            out.append("," if t in {",", "COMMA"} else t)
            i += 1
            continue

        choices = reverse_alias_map.get(t, None)
        if not choices:
            out.append(t); i += 1; continue

        nxt = toks[i + 1].lower() if i + 1 < len(toks) else None
        plain, ends = _bucket_aliases_by_trailing_connector(choices)

        if nxt in connectors:
            if plain:
                out.append(plain[0]); i += 1
            elif nxt in ends and ends[nxt]:
                out.append(ends[nxt][0]); i += 2  # consume the next connector
            else:
                pool = plain or [a for arr in ends.values() for a in arr]
                out.append(pool[0]); i += 1
        else:
            out.append(plain[0] if plain else [a for arr in ends.values() for a in arr][0])
            i += 1

    return " ".join(out)

# -----------------------------
# Debug probe (optional)
# -----------------------------
def inspect_leftmost(
    vocabulary: Dict[str, Dict[str, object]],
    text: str,
    punctuation_as_mapped: Iterable[str] = _DEFAULT_PASSTHROUGH,
    joiner: str = " ",
    graph: Optional[Dict[str, object]] = None,
) -> None:
    nd = build_nd_map(vocabulary, graph=graph)
    toks = tokenize(text)
    seed = punctuation_passthrough(toks, punctuation_as_mapped)
    i = None
    for idx, (_, m) in enumerate(seed):
        if not m:
            i = idx; break
    max_len = max(1, max(len(k.split(" ")) for k in nd.keys())) if nd else 1
    print(f"\n[inspect] '{text}'")
    print(f"  tokens: {toks}")
    print(f"  seed  : {seed}")
    if i is None:
        print("  fully mapped at seed"); return
    spans = []
    run_len = len(toks) - i
    for span_len in range(1, min(max_len, run_len) + 1):
        span = joiner.join(toks[i:i+span_len])
        if span in nd:
            spans.append((span, len(nd[span])))
    print(f"  leftmost unmapped idx={i} token='{toks[i]}'")
    print(f"  nd keys here: {spans if spans else 'NONE  <-- coverage gap at leftmost'}")


