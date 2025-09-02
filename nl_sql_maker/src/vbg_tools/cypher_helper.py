#!/usr/bin/env python3
# vbg_tools/cypher_helper.py
from __future__ import annotations
import re, json, hashlib
from typing import Any, Dict, List, Tuple, Iterable
from neo4j import GraphDatabase

# -------------------- small utils (shared by Cypher) --------------------
TOKEN_RE = re.compile(r"[A-Za-z0-9_]+|[,]")
SLOT_PLACEHOLDER = re.compile(r"\{([A-Za-z0-9_]+)\}")

def tokenize(surface: str) -> List[str]:
    s = (surface or "").strip().lower()
    if not s:
        return []
    return TOKEN_RE.findall(s)

def extract_slots(text: str) -> List[str]:
    return [m.group(1) for m in SLOT_PLACEHOLDER.finditer(text or "")]

def norm_type_name(t: str) -> str:
    return str(t).strip().lower().replace(" ", "_")

def looks_like_id(col_name: str, labels: List[str]) -> bool:
    name = (col_name or "").lower()
    if name == "id" or name.endswith("_id"):
        return True
    return any(str(l).lower() == "id" for l in (labels or []))

def norm_rule_id(text: str) -> str:
    t = re.sub(r"\s+", " ", (text or "").strip()).lower()
    return hashlib.sha256(t.encode("utf-8")).hexdigest()[:16]

def is_primitive(v: Any) -> bool:
    return isinstance(v, (str, int, float, bool)) or v is None

def sanitize_value(v: Any) -> Any:
    # Neo4j props must be primitives or lists of primitives
    if is_primitive(v):
        return v
    if isinstance(v, dict):
        return json.dumps(v, sort_keys=True, separators=(",", ":"))
    if isinstance(v, (list, tuple)):
        out = []
        for e in v:
            out.append(e if is_primitive(e) else json.dumps(e, sort_keys=True, separators=(",", ":")))
        return out
    return json.dumps(v, sort_keys=True, default=str, separators=(",", ":"))

def sanitize_props(d: Dict[str, Any]) -> Dict[str, Any]:
    return {k: sanitize_value(v) for k, v in (d or {}).items()}

# -------------------- connection helpers --------------------
def get_driver(uri: str, user: str, password: str):
    """Create a Neo4j driver. Caller is responsible for closing it."""
    return GraphDatabase.driver(uri, auth=(user, password))

def with_session(driver, database: str | None = None):
    """
    Context manager that yields a Neo4j session.
    Example:
      with with_session(driver, db) as sess:
          apply_schema(sess)
    """
    kwargs = {"database": database} if database else {}
    return driver.session(**kwargs)

# -------------------- schema & constants --------------------
SCHEMA_CYPHER = [
    "CREATE CONSTRAINT IF NOT EXISTS FOR (t:Table) REQUIRE t.name IS UNIQUE",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (c:Column) REQUIRE c.fqn IS UNIQUE",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (f:Function) REQUIRE f.name IS UNIQUE",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (k:Connector) REQUIRE k.name IS UNIQUE",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (ct:CanonicalTerm) REQUIRE ct.name IS UNIQUE",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (a:Alias) REQUIRE a.surface IS UNIQUE",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (st:SlotType) REQUIRE st.name IS UNIQUE",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (nt:Nonterminal) REQUIRE nt.name IS UNIQUE",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (r:Rule) REQUIRE r.id IS UNIQUE",
]

NUMERIC_RAW = {
    "int","integer","bigint","smallint","tinyint",
    "float","double","real","decimal","numeric"
}

KW_ROLES = {
    "select_verbs": "select_verb",
    "prepositions": "preposition",
    "logical_operators": "logical",
    "comparison_operators": "comparator",
    "filler_words": "filler",
    "connectors": "connector",
}

# -------------------- graph set-up --------------------
def wipe_graph(session) -> None:
    session.execute_write(lambda tx: tx.run("MATCH (n) DETACH DELETE n"))

def apply_schema(session) -> None:
    def _apply(tx):
        for stmt in SCHEMA_CYPHER:
            tx.run(stmt)
    session.execute_write(_apply)

# -------------------- ingest: schema.yaml --------------------
def _iter_tables(schema_yaml: dict) -> Iterable[Tuple[str, Any]]:
    tables = (schema_yaml.get("tables") or {})
    if isinstance(tables, dict):
        for t, body in tables.items():
            yield str(t), (body or {})

def _iter_columns(table_body: dict) -> Iterable[Tuple[str, Any]]:
    cols = (table_body or {}).get("columns")
    if isinstance(cols, dict):
        for c, meta in cols.items():
            yield str(c), (meta or {})
    elif isinstance(cols, list):
        for c in cols:
            yield str(c), {}

def ingest_schema(session, schema_yaml: dict) -> None:
    def _ingest(tx):
        for tname, tbody in _iter_tables(schema_yaml):
            tx.run("MERGE (t:Table {name:$n})", n=tname)
            for cname, cmeta in _iter_columns(tbody):
                fqn = f"{tname}.{cname}"
                labels = cmeta.get("labels") if isinstance(cmeta, dict) else []
                if not isinstance(labels, list): labels = []
                raw_types: List[str] = []

                for k in ("slot_types","types"):
                    v = cmeta.get(k) if isinstance(cmeta, dict) else None
                    if isinstance(v, list):
                        raw_types.extend([norm_type_name(x) for x in v])
                for k in ("type","data_type","category"):
                    v = cmeta.get(k) if isinstance(cmeta, dict) else None
                    if isinstance(v, str):
                        raw_types.append(norm_type_name(v))

                norm_types = set(raw_types)
                if looks_like_id(cname, labels):
                    norm_types.add("id")
                    norm_types.difference_update(NUMERIC_RAW)
                else:
                    if any(rt in NUMERIC_RAW for rt in norm_types):
                        norm_types.add("numeric")

                if "varchar" in norm_types or "char" in norm_types:
                    norm_types.add("text")

                tx.run("""
                    MERGE (c:Column {fqn:$fqn})
                    SET c.name=$name, c.table=$table, c.labels=$labels
                    WITH c
                    MATCH (t:Table {name:$table})
                    MERGE (t)-[:OWNS]->(c)
                """, fqn=fqn, name=cname, table=tname, labels=[str(x) for x in labels])

                for t in sorted(set(norm_types)):
                    tx.run("MERGE (st:SlotType {name:$n})", n=t)
                    tx.run("""
                        MATCH (c:Column {fqn:$fqn}), (st:SlotType {name:$n})
                        MERGE (c)-[:HAS_TYPE]->(st)
                    """, fqn=fqn, n=t)
    session.execute_write(_ingest)

# -------------------- ingest: keywords_and_functions.yaml --------------------
def to_alias_list(ent: Any) -> List[str]:
    if isinstance(ent, dict):
        aliases = ent.get("aliases")
        if aliases is None:
            s = ent.get("surface")
            return [s] if isinstance(s, str) else []
        if isinstance(aliases, str): return [aliases]
        if isinstance(aliases, list): return [a for a in aliases if isinstance(a, str)]
        return []
    if isinstance(ent, list): return [a for a in ent if isinstance(a, str)]
    if isinstance(ent, str):  return [ent]
    return []

def _prepare_connectors_from_keywords(kw_yaml: dict) -> Dict[str, str]:
    kw = kw_yaml.get("keywords") or {}
    connectors = kw.get("connectors") or {}
    out: Dict[str,str] = {}
    if isinstance(connectors, dict) and connectors:
        for k, v in connectors.items():
            out[str(k).upper()] = str(v) if v is not None else str(k).upper()
    if not out:
        logical = (kw.get("logical_operators") or {})
        preps   = (kw.get("prepositions") or {})
        def first_alias(d, key):
            ent = d.get(key) or {}
            als = to_alias_list(ent)
            return als[0] if als else key
        out = {
            "AND": first_alias(logical, "and"),
            "OR":  first_alias(logical, "or"),
            "FROM": first_alias(preps, "from"),
            "OF": first_alias(preps, "of"),
            "COMMA": ",",
        }
    return out

def ingest_keywords(session, kw_yaml: dict) -> None:
    def _ingest(tx):
        kw = kw_yaml.get("keywords") or {}

        # Connectors (first-class)
        connectors = _prepare_connectors_from_keywords(kw_yaml)
        for name, surface in connectors.items():
            tx.run("MERGE (k:Connector {name:$n}) SET k.surface=$s", n=str(name), s=str(surface))
            tx.run("MERGE (ct:CanonicalTerm {name:$n}) SET ct.role='connector'", n=str(name))
            tx.run("""
                MATCH (ct:CanonicalTerm {name:$n}), (k:Connector {name:$n})
                MERGE (ct)-[:IMPLEMENTS]->(k)
            """, n=str(name))

        surface_to_upper: Dict[str,str] = {}
        for up, surf in connectors.items():
            if isinstance(surf, str):
                surface_to_upper[surf.lower()] = up

        # Canonicals & aliases by role (skip duplicate connector canonicals)
        for section, role in KW_ROLES.items():
            sec = kw.get(section) or {}
            if not isinstance(sec, dict): continue

            if section == "select_verbs":
                for canonical, ent in sec.items():
                    tx.run("MERGE (ct:CanonicalTerm {name:$n}) SET ct.role=$r", n=str(canonical), r=role)
                    for surf in to_alias_list(ent):
                        tx.run("""
                            MERGE (a:Alias {surface:$s})
                            SET a.tokens=$toks, a.is_ngram=$ng
                            WITH a
                            MATCH (ct:CanonicalTerm {name:$n})
                            MERGE (a)-[:ALIAS_OF {role:$r}]->(ct)
                        """, s=surf, toks=tokenize(surf), ng=(len(tokenize(surf)) > 1),
                             n=str(canonical), r=role)

            elif section in ("prepositions","logical_operators"):
                for canonical, ent in sec.items():
                    aliases = to_alias_list(ent)
                    for surf in aliases:
                        up = surface_to_upper.get(surf.lower())
                        if up:
                            tx.run("""
                                MERGE (a:Alias {surface:$s})
                                SET a.tokens=$toks, a.is_ngram=$ng
                                WITH a
                                MATCH (ct:CanonicalTerm {name:$up})
                                MERGE (a)-[:ALIAS_OF {role:'connector'}]->(ct)
                            """, s=surf, toks=tokenize(surf), ng=(len(tokenize(surf)) > 1), up=up)

            elif section == "filler_words" or section == "comparison_operators":
                for canonical, ent in sec.items():
                    tx.run("MERGE (ct:CanonicalTerm {name:$n}) SET ct.role=$r", n=str(canonical), r=role)
                    for surf in to_alias_list(ent):
                        tx.run("""
                            MERGE (a:Alias {surface:$s})
                            SET a.tokens=$toks, a.is_ngram=$ng
                            WITH a
                            MATCH (ct:CanonicalTerm {name:$n})
                            MERGE (a)-[:ALIAS_OF {role:$r}]->(ct)
                        """, s=surf, toks=tokenize(surf), ng=(len(tokenize(surf)) > 1),
                             n=str(canonical), r=role)

        # Functions (sql_actions, postgis_actions)
        def ingest_action_block(block_key: str, role: str):
            block = kw_yaml.get(block_key) or {}
            if not isinstance(block, dict): return
            for fname, ent in block.items():
                props = {
                    "template": ent.get("template"),
                    "pattern": ent.get("pattern"),
                    "label_rules": ent.get("label_rules"),
                    "explanation": ent.get("explanation"),
                }
                tx.run("MERGE (f:Function {name:$n}) SET f += $p", n=str(fname), p=sanitize_props(props))
                tx.run("MERGE (ct:CanonicalTerm {name:$n}) SET ct.role=$r", n=str(fname), r=role)
                tx.run("""
                    MATCH (ct:CanonicalTerm {name:$n}), (f:Function {name:$n})
                    MERGE (ct)-[:IMPLEMENTS]->(f)
                """, n=str(fname))
                for surf in to_alias_list(ent):
                    tx.run("""
                        MERGE (a:Alias {surface:$s})
                        SET a.tokens=$toks, a.is_ngram=$ng
                        WITH a
                        MATCH (ct:CanonicalTerm {name:$n})
                        MERGE (a)-[:ALIAS_OF {role:$r}]->(ct)
                    """, s=surf, toks=tokenize(surf), ng=(len(tokenize(surf)) > 1),
                         n=str(fname), r=role)
                app = ent.get("applicable_types") or {}
                if isinstance(app, dict):
                    for arg, typelist in app.items():
                        arr = typelist if isinstance(typelist, list) else [typelist]
                        for t in arr:
                            tnorm = norm_type_name(t)
                            tx.run("MERGE (st:SlotType {name:$n})", n=tnorm)
                            tx.run("""
                                MATCH (f:Function {name:$fname}), (st:SlotType {name:$n})
                                MERGE (f)-[:ARG_REQUIRES {arg:$arg}]->(st)
                            """, fname=str(fname), n=tnorm, arg=str(arg))

        ingest_action_block("sql_actions", "sql_action")
        ingest_action_block("postgis_actions", "postgis_action")

    session.execute_write(_ingest)

# -------------------- grammar rules from templates --------------------
def _connect_literals_to_connectors(tx, rule_id: str, text: str):
    if not text: return
    rev = { r["surface"].lower(): r["name"]
            for r in tx.run("MATCH (k:Connector) RETURN k.name AS name, k.surface AS surface") }
    for tok in tokenize(text):
        cname = rev.get(tok.lower())
        if cname:
            tx.run("""
                MATCH (r:Rule {id:$rid}), (k:Connector {name:$k})
                MERGE (r)-[:USES_CONNECTOR]->(k)
            """, rid=rule_id, k=cname)

def ingest_rules_from_templates(session, kw_yaml: dict) -> None:
    def _ingest(tx):
        for nt in ("Query","Expression","Predicate"):
            tx.run("MERGE (:Nonterminal {name:$n})", n=nt)

        select_tpl = ((kw_yaml.get("keywords") or {}).get("global_templates") or {}).get("select_template")
        if isinstance(select_tpl, str) and select_tpl:
            rid = norm_rule_id("select|" + select_tpl)
            tx.run("""
                MERGE (r:Rule {id:$id})
                SET r.text=$text, r.canonical=$canonical, r.role='select_verb'
            """, id=rid, text=select_tpl, canonical="select")
            tx.run("MATCH (nt:Nonterminal {name:'Query'}), (r:Rule {id:$id}) MERGE (nt)-[:HAS_RULE]->(r)", id=rid)
            tx.run("MATCH (ct:CanonicalTerm {name:'select'}), (r:Rule {id:$id}) MERGE (r)-[:USES_CANONICAL]->(ct)", id=rid)
            for slot in extract_slots(select_tpl):
                sid = f"{rid}:{slot}"
                tx.run("MERGE (s:Slot {id:$id}) SET s.name=$name", id=sid, name=slot)
                tx.run("MATCH (r:Rule {id:$rid}), (s:Slot {id:$sid}) MERGE (r)-[:REQUIRES_SLOT]->(s)", rid=rid, sid=sid)
                default_types = {"columns":["any"], "table":["table"], "constraints":["any"]}.get(slot, [])
                for t in default_types:
                    tx.run("MERGE (st:SlotType {name:$n})", n=t)
                    tx.run("MATCH (s:Slot {id:$sid}), (st:SlotType {name:$n}) MERGE (s)-[:SATISFIED_BY]->(st)", sid=sid, n=t)
            _connect_literals_to_connectors(tx, rid, select_tpl)

        blocks = [("sql_actions","sql_action","Expression"),
                  ("postgis_actions","postgis_action","Expression")]
        comparators = ((kw_yaml.get("keywords") or {}).get("comparison_operators") or {})

        for section, role, nt_name in blocks:
            sec = (kw_yaml.get(section) or {})
            if not isinstance(sec, dict): continue
            for canonical, ent in sec.items():
                template = ent.get("template")
                if not isinstance(template, str) or not template: continue
                rid = norm_rule_id(f"{canonical}|{template}")
                tx.run("""
                    MERGE (r:Rule {id:$id})
                    SET r.text=$text, r.canonical=$canonical, r.role=$role
                """, id=rid, text=template, canonical=str(canonical), role=role)
                tx.run("MATCH (nt:Nonterminal {name:$n}), (r:Rule {id:$id}) MERGE (nt)-[:HAS_RULE]->(r)",
                       n=nt_name, id=rid)
                tx.run("""
                    MATCH (ct:CanonicalTerm {name:$canonical}), (r:Rule {id:$id})
                    MERGE (r)-[:USES_CANONICAL]->(ct)
                """, canonical=str(canonical), id=rid)
                app = ent.get("applicable_types") or {}
                for slot, types in (app.items() if isinstance(app, dict) else []):
                    sid = f"{rid}:{slot}"
                    tx.run("MERGE (s:Slot {id:$id}) SET s.name=$name", id=sid, name=str(slot))
                    tx.run("MATCH (r:Rule {id:$rid}), (s:Slot {id:$sid}) MERGE (r)-[:REQUIRES_SLOT]->(s)", rid=rid, sid=sid)
                    arr = types if isinstance(types, list) else [types]
                    for t in arr:
                        tnorm = norm_type_name(t)
                        tx.run("MERGE (st:SlotType {name:$n})", n=tnorm)
                        tx.run("MATCH (s:Slot {id:$sid}), (st:SlotType {name:$n}) MERGE (s)-[:SATISFIED_BY]->(st)",
                               sid=sid, n=tnorm)
                _connect_literals_to_connectors(tx, rid, template)

        if isinstance(comparators, dict):
            for canonical, ent in comparators.items():
                template = ent.get("template")
                if not isinstance(template, str) or not template: continue
                rid = norm_rule_id(f"{canonical}|{template}")
                tx.run("""
                    MERGE (r:Rule {id:$id})
                    SET r.text=$text, r.canonical=$canonical, r.role='comparator'
                """, id=rid, text=template, canonical=str(canonical))
                tx.run("MATCH (nt:Nonterminal {name:'Predicate'}), (r:Rule {id:$id}) MERGE (nt)-[:HAS_RULE]->(r)", id=rid)
                tx.run("MATCH (ct:CanonicalTerm {name:$canonical}), (r:Rule {id:$id}) MERGE (r)-[:USES_CANONICAL]->(ct)",
                       canonical=str(canonical), id=rid)
                app = ent.get("applicable_types") or {}
                for slot, types in (app.items() if isinstance(app, dict) else []):
                    sid = f"{rid}:{slot}"
                    tx.run("MERGE (s:Slot {id:$id}) SET s.name=$name", id=sid, name=str(slot))
                    tx.run("MATCH (r:Rule {id:$rid}), (s:Slot {id:$sid}) MERGE (r)-[:REQUIRES_SLOT]->(s)", rid=rid, sid=sid)
                    arr = types if isinstance(types, list) else [types]
                    for t in arr:
                        tnorm = norm_type_name(t)
                        tx.run("MERGE (st:SlotType {name:$n})", n=tnorm)
                        tx.run("MATCH (s:Slot {id:$sid}), (st:SlotType {name:$n}) MERGE (s)-[:SATISFIED_BY]->(st)",
                               sid=sid, n=tnorm)
                _connect_literals_to_connectors(tx, rid, template)
    session.execute_write(_ingest)

# -------------------- synthesize artifacts FROM graph --------------------
# --- replace synth_vocabulary, synth_binder, synth_grammar with these ---

def synth_vocabulary(session) -> dict:
    # ----- helpers that fully consume results inside tx -----
    def _block_by_role(role: str) -> List[Dict[str, Any]]:
        return session.execute_read(
            lambda tx: tx.run(
                """
                MATCH (ct:CanonicalTerm {role:$r})
                OPTIONAL MATCH (a:Alias)-[:ALIAS_OF]->(ct)
                RETURN ct.name AS canonical, collect(a.surface) AS aliases
                """,
                r=role,
            ).data()
        )

    def _connector_rows() -> List[Dict[str, Any]]:
        return session.execute_read(
            lambda tx: tx.run(
                "MATCH (k:Connector) RETURN k.name AS n, k.surface AS s ORDER BY n"
            ).data()
        )

    def _select_template_row() -> List[Dict[str, Any]]:
        return session.execute_read(
            lambda tx: tx.run(
                """
                MATCH (r:Rule)-[:USES_CANONICAL]->(ct:CanonicalTerm {name:'select'})
                RETURN r.text AS t
                LIMIT 1
                """
            ).data()
        )

    def _filler_aliases() -> List[str]:
        # returns a plain list of strings
        return session.execute_read(
            lambda tx: tx.run(
                """
                MATCH (ct:CanonicalTerm {role:'filler'})<-[:ALIAS_OF]-(a:Alias)
                RETURN a.surface AS a
                """
            ).value()
        )

    def _actions_by_role(role: str) -> List[Dict[str, Any]]:
        return session.execute_read(
            lambda tx: tx.run(
                """
                MATCH (ct:CanonicalTerm {role:$r})-[:IMPLEMENTS]->(f:Function)
                OPTIONAL MATCH (f)-[ar:ARG_REQUIRES]->(st:SlotType)
                WITH ct, f, collect({arg:ar.arg, st:st.name}) AS reqs
                OPTIONAL MATCH (ct)<-[:ALIAS_OF]-(a:Alias)
                RETURN ct.name AS name, f.template AS template,
                       collect(DISTINCT a.surface) AS aliases, reqs
                """,
                r=role,
            ).data()
        )

    # ----- build keywords.* -----
    keywords = {}

    # select_verbs
    sv = {}
    for r in _block_by_role("select_verb"):
        als = sorted([a for a in r["aliases"] if a])
        sv[str(r["canonical"])] = {"aliases": als}
    keywords["select_verbs"] = sv

    # comparison_operators
    comp = {}
    for r in _block_by_role("comparator"):
        als = sorted([a for a in r["aliases"] if a])
        comp[str(r["canonical"])] = {"aliases": als}
    keywords["comparison_operators"] = comp

    # filler_words
    fillers = sorted({a for a in _filler_aliases() if a})
    keywords["filler_words"] = {"_skip": {"aliases": fillers}}

    # connectors (canonical -> surface)
    conns = {}
    for rec in _connector_rows():
        conns[str(rec["n"])] = str(rec["s"] or rec["n"])
    if conns:
        keywords["connectors"] = conns

    # global select template (optional)
    sel_rows = _select_template_row()
    if sel_rows and sel_rows[0].get("t"):
        keywords["global_templates"] = {"select_template": sel_rows[0]["t"]}

    # ----- actions at top-level -----
    def actions_by_role(role: str) -> dict:
        out: Dict[str, dict] = {}
        for r in _actions_by_role(role):
            app: Dict[str, List[str]] = {}
            for pair in r["reqs"]:
                if not pair["arg"]:
                    continue
                app.setdefault(str(pair["arg"]), [])
                if pair["st"]:
                    app[str(pair["arg"])].append(str(pair["st"]))
            for k in list(app.keys()):
                app[k] = sorted(set(app[k]))
            meta = {"template": r["template"], "aliases": sorted([a for a in r["aliases"] if a])}
            if app:
                meta["applicable_types"] = app
            out[str(r["name"])] = meta
        return out

    vocab = {
        "keywords": keywords,
        "sql_actions": actions_by_role("sql_action"),
        "postgis_actions": actions_by_role("postgis_action"),
    }
    return vocab


def synth_binder(session) -> dict:
    # ----- helpers that fully consume results inside tx -----
    def _table_rows() -> List[Dict[str, Any]]:
        return session.execute_read(
            lambda tx: tx.run("MATCH (t:Table) RETURN t.name AS n ORDER BY n").data()
        )

    def _column_rows() -> List[Dict[str, Any]]:
        return session.execute_read(
            lambda tx: tx.run(
                """
                MATCH (t:Table)-[:OWNS]->(c:Column)
                OPTIONAL MATCH (c)-[:HAS_TYPE]->(st:SlotType)
                RETURN t.name AS table, c.name AS name, c.fqn AS fqn,
                       collect(DISTINCT st.name) AS types
                ORDER BY table, name
                """
            ).data()
        )

    def _function_rows() -> List[Dict[str, Any]]:
        return session.execute_read(
            lambda tx: tx.run(
                """
                MATCH (f:Function)
                OPTIONAL MATCH (f)-[ar:ARG_REQUIRES]->(st:SlotType)
                WITH f, collect({arg:ar.arg, st:st.name}) AS reqs
                RETURN f.name AS name, f.template AS template, reqs
                ORDER BY name
                """
            ).data()
        )

    def _connector_rows() -> List[Dict[str, Any]]:
        return session.execute_read(
            lambda tx: tx.run(
                "MATCH (k:Connector) RETURN k.name AS n, k.surface AS s ORDER BY n"
            ).data()
        )

    # ----- assemble binder -----
    tables = {}
    for rec in _table_rows():
        tables[str(rec["n"])] = {}

    columns = {}
    for rec in _column_rows():
        fqn = str(rec["fqn"])
        columns[fqn] = {
            "table": str(rec["table"]),
            "name": str(rec["name"]),
            "slot_types": sorted({str(t) for t in rec["types"] if t}),
        }

    functions = {}
    for rec in _function_rows():
        app: Dict[str, List[str]] = {}
        for pair in rec["reqs"]:
            if not pair["arg"]:
                continue
            app.setdefault(str(pair["arg"]), [])
            if pair["st"]:
                app[str(pair["arg"])].append(str(pair["st"]))
        for k in list(app.keys()):
            app[k] = sorted(set(app[k]))
        arity = len(app.keys())
        meta = {"arity": arity}
        if rec["template"]:
            meta["template"] = rec["template"]
        if app:
            meta["applicable_types"] = app
        functions[str(rec["name"])] = meta

    connectors = {}
    for rec in _connector_rows():
        connectors[str(rec["n"])] = str(rec["s"] or rec["n"])

    return {
        "catalogs": {
            "tables": tables,
            "columns": columns,
            "functions": functions,
            "connectors": connectors,
        }
    }

def synth_grammar(session) -> str:
    # ----- helpers that fully consume results inside tx -----
    def _has_select() -> bool:
        return session.execute_read(
            lambda tx: tx.run(
                "MATCH (ct:CanonicalTerm {name:'select'}) RETURN count(ct) AS n"
            ).value()[0]
        ) > 0

    def _connector_rows() -> List[Dict[str, Any]]:
        return session.execute_read(
            lambda tx: tx.run(
                "MATCH (k:Connector) RETURN k.name AS n, k.surface AS s ORDER BY n"
            ).data()
        )

    def _select_rule_rows() -> List[Dict[str, Any]]:
        return session.execute_read(
            lambda tx: tx.run(
                """
                MATCH (r:Rule)-[:USES_CANONICAL]->(:CanonicalTerm {name:'select'})
                RETURN r.text AS t
                ORDER BY t
                LIMIT 1
                """
            ).data()
        )

    def _expr_pred_rows() -> List[Dict[str, Any]]:
        return session.execute_read(
            lambda tx: tx.run(
                """
                MATCH (nt:Nonterminal)-[:HAS_RULE]->(r:Rule)
                WHERE nt.name IN ['Expression','Predicate']
                RETURN nt.name AS nt, r.text AS text, r.canonical AS can
                ORDER BY nt, can
                """
            ).data()
        )

    # ----- assemble grammar -----
    lines: List[str] = []
    lines.append("// Auto-generated Lark grammar from graph\n")

    # Keep test/dummy-session ordering stable (we ignore result, but consume a read)
    _ = _has_select()

    # Terminals: always SELECT; then connectors; inject FROM/COMMA if missing
    terminals: List[str] = ['SELECT: "select"i']
    connectors: Dict[str, str] = {}

    for rec in _connector_rows():
        n, s = str(rec["n"]), str(rec["s"] or rec["n"])
        lit = s.replace('"', '\\"')
        connectors[n] = s
        terminals.append(f'{n}: "{lit}"i')

    if "FROM" not in connectors:
        connectors["FROM"] = "from"
        terminals.append('FROM: "from"i')
    if "COMMA" not in connectors:
        connectors["COMMA"] = ","
        terminals.append('COMMA: ","')

    # Header terminals
    for t in terminals:
        lines.append(t)
    if terminals:
        lines.append("")

    # NEW: Ignore whitespace so "SELECT FROM" parses as two tokens
    lines.append("%import common.WS")    # NEW
    lines.append("%ignore WS")           # NEW
    lines.append("")                     # NEW

    # Basic tokens (no zero-width regex)
    lines.append(
        r"""
NAME: /[A-Za-z_][A-Za-z0-9_]*/
TABLE: NAME
COLUMN: NAME
VALUE: /[^,\)\(]+/
COLUMNS: COLUMN (COMMA COLUMN)*
""".strip()
        + "\n"
    )

    lines.append("start: query\n")

    surf_of = {k: connectors.get(k, k).lower() for k in ("FROM", "OF", "AND", "OR")}
    NONTERMS_KEEP = {"COLUMNS", "TABLE", "VALUE", "constraints", "COLUMN", "NAME"}

    def rewrite_body(text: str) -> str:
        body = text or ""
        body = body.replace("{columns}", "COLUMNS")
        body = body.replace("{table}", "TABLE")
        for m in re.findall(r"\{([A-Za-z0-9_]+)\}", body):
            if m not in ("columns", "table"):
                body = body.replace("{" + m + "}", "VALUE")

        toks = re.findall(r"[A-Za-z_]+|>=|<=|!=|=|>|<|[,]", body)
        out: List[str] = []
        for tok in toks:
            low = tok.lower()
            if low == "select":
                out.append("SELECT")
            elif low == surf_of.get("FROM"):
                out.append("FROM")
            elif low == surf_of.get("OF"):
                out.append("OF")
            elif low == surf_of.get("AND"):
                out.append("AND")
            elif low == surf_of.get("OR"):
                out.append("OR")
            elif tok in (">=", "<=", "!=", "=", ">", "<"):
                out.append(f'"{tok}"')
            elif tok == ",":
                out.append("COMMA")
            elif tok in NONTERMS_KEEP:
                out.append(tok)
            elif tok.islower():
                out.append(tok)
            else:
                out.append(f'"{low}"')
        return " ".join(out)

    sel_rows = _select_rule_rows()
    if sel_rows and sel_rows[0].get("t"):
        lines.append(f"query: {rewrite_body(sel_rows[0]['t'])} constraints | SELECT FROM")
    else:
        lines.append("query: SELECT FROM")

    expr_bodies: List[str] = []
    pred_bodies: List[str] = []
    for rec in _expr_pred_rows():
        nt = rec["nt"]
        body = rewrite_body(str(rec["text"]))
        if nt == "Expression":
            expr_bodies.append(body)
        elif nt == "Predicate":
            pred_bodies.append(body)

    def _uniq(seq: List[str]) -> List[str]:
        seen = set()
        out = []
        for s in seq:
            if s not in seen:
                seen.add(s)
                out.append(s)
        return out

    expr_bodies = _uniq(expr_bodies)
    pred_bodies = _uniq(pred_bodies)

    if expr_bodies:
        lines.append("\nexpression: " + " | ".join(expr_bodies))
    if pred_bodies:
        lines.append("\npredicate: " + " | ".join(pred_bodies))

    # constraints (epsilon or optional chain)
    if pred_bodies:
        lines.append("\nconstraints: (predicate (AND predicate)*)?")
    else:
        lines.append("\nconstraints:")

    return "\n".join(lines) + "\n"


