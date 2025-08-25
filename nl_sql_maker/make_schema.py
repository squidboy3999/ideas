import sqlite3
import yaml
import os
import inflect # NEW: Import the inflect library

DB_PATH = "/app/test.db"
OUTPUT_SCHEMA_PATH = "/app/schema.yaml"
# NEW: Path for our persistent, learning alias dictionary
ALIAS_DICT_PATH = "/app/alias_dictionary.yaml" 

# Mapping from SpatiaLite geometry type codes to strings
GEOMETRY_TYPE_MAP = {
    1: 'POINT', 2: 'LINESTRING', 3: 'POLYGON', 4: 'MULTIPOINT',
    5: 'MULTILINESTRING', 6: 'MULTIPOLYGON', 7: 'GEOMETRYCOLLECTION'
}

def load_or_initialize_yaml(path):
    """Loads a YAML file if it exists, otherwise returns an empty dictionary."""
    if os.path.exists(path):
        with open(path, 'r') as f:
            return yaml.safe_load(f)
    return {}

# --- Intra-table alias collision prevention ---

def _propose_column_aliases(name: str, p_engine, alias_dict: dict) -> set:
    """
    Build a conservative alias set for a column name.
    - snake_case → 'snake case'
    - last-word singular/plural variants
    - merge in learned aliases from alias_dictionary
    """
    aliases = set()
    aliases.add(name)
    aliases.add(name.replace('_', ' '))

    last_word = name.split('_')[-1]
    singular_last = p_engine.singular_noun(last_word) or last_word
    if singular_last != last_word:
        aliases.add(name.replace(last_word, singular_last).replace('_', ' '))

    plural_last = p_engine.plural(last_word)
    if plural_last != last_word:
        aliases.add(name.replace(last_word, plural_last).replace('_', ' '))

    # learned aliases (if any)
    for a in alias_dict.get(name, []):
        s = str(a).strip()
        if s:
            aliases.add(s)

    return aliases


def _resolve_intra_table_alias_collisions(table_name: str, col_alias_map: dict) -> tuple[dict, list]:
    """
    Given {col -> set(aliases)} within a table, remove any alias that appears
    on 2+ columns in the SAME table. Return (clean_map, warnings).
    """
    # alias -> [columns that want it]
    inv = {}
    for col, aliases in col_alias_map.items():
        for a in aliases:
            inv.setdefault(a.lower().strip(), []).append(col)

    dropped_events = []
    for alias, cols in inv.items():
        if len(cols) <= 1:
            continue  # no conflict
        # drop alias from ALL colliding columns
        for c in cols:
            if alias in {x.lower() for x in col_alias_map[c]}:
                # remove the exact-cased variant(s)
                to_remove = {x for x in col_alias_map[c] if x.lower() == alias}
                col_alias_map[c] -= to_remove
        dropped_events.append({
            "table": table_name,
            "alias": alias,
            "columns": sorted(cols),
            "action": "dropped_from_all"
        })
    return col_alias_map, dropped_events


def _emit_collision_warnings(events: list) -> None:
    for ev in events:
        print(
            f"WARNING: Intra-table alias collision in '{ev['table']}': "
            f"alias '{ev['alias']}' appeared on columns {ev['columns']}. "
            f"Action: {ev['action']}."
        )


def get_schema_from_db(db_path, alias_dict):
    """
    Queries an SQLite database and returns an enriched schema dictionary.
    Prevents intra-table column alias collisions by dropping the ambiguous alias
    from all conflicting columns within the same table (and logs a warning).
    """
    p = inflect.engine()

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    conn.enable_load_extension(True)
    conn.execute("SELECT load_extension('mod_spatialite')")

    schema = {'tables': {}}
    geometry_info = {}

    # SpatiaLite geometry metadata (if present)
    try:
        cursor.execute("SELECT f_table_name, f_geometry_column, geometry_type, srid FROM geometry_columns;")
        for row in cursor.fetchall():
            table_name, column_name, subtype_code, srid = row
            base_type = 'GEOGRAPHY' if srid == 4326 else 'GEOMETRY'
            subtype_str = GEOMETRY_TYPE_MAP.get(subtype_code, 'UNKNOWN')
            geometry_info[(table_name, column_name)] = {'base_type': base_type, 'subtype': subtype_str}
    except sqlite3.Error:
        # Non-spatial DBs won't have this table—totally fine.
        pass

    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables_to_exclude = {
        'sqlite_sequence', 'spatial_ref_sys', 'geometry_columns',
        'vector_layers', 'virts_geometry_columns', 'spatialite_history',
        'spatial_ref_sys_aux', 'views_geometry_columns', 'geometry_columns_statistics',
        'views_geometry_columns_statistics', 'virts_geometry_columns_statistics',
        'geometry_columns_field_infos', 'views_geometry_columns_field_infos',
        'virts_geometry_columns_field_infos', 'geometry_columns_time',
        'geometry_columns_auth', 'views_geometry_columns_auth',
        'virts_geometry_columns_auth', 'data_licenses', 'sql_statements_log',
        'SpatialIndex', 'ElementaryGeometries', 'KNN'
    }
    tables = [row[0] for row in cursor.fetchall() if row[0] not in tables_to_exclude]

    for table_name in tables:
        # --- Table aliases (dedup via set) ---
        t_aliases = set()
        singular = p.singular_noun(table_name) or table_name
        plural = p.plural(singular)
        t_aliases.update({singular, plural})
        t_aliases.update(alias_dict.get(table_name, []))

        # We'll populate columns after collision resolution
        schema['tables'][table_name] = {'aliases': sorted(t_aliases), 'columns': {}}
        alias_dict[table_name] = sorted(t_aliases)

        # --- Collect raw column info first (so we can resolve collisions) ---
        cursor.execute(f"PRAGMA table_info({table_name});")
        columns = cursor.fetchall()

        # Stage 1: propose aliases per column
        col_alias_proposals = {}
        col_labels = {}
        col_types = {}

        for column in columns:
            cid, name, ctype, notnull, dflt_value, pk = column

            # Type mapping / spatial detection
            mapped_type = (ctype or "").upper()
            labels = set()
            geom_data = geometry_info.get((table_name, name))
            if geom_data:
                mapped_type = f"{geom_data['base_type'].lower()}_{geom_data['subtype'].lower()}"
                labels.add('postgis')

            # Rule-based labels
            name_lower = name.lower()
            if 'id' in name_lower: labels.add('id')
            if 'latitude' in name_lower: labels.add('latitude')
            if 'longitude' in name_lower: labels.add('longitude')

            # Propose aliases (before collision resolution)
            proposed = _propose_column_aliases(name, p, alias_dict)

            col_alias_proposals[name] = proposed
            col_labels[name] = labels
            col_types[name] = mapped_type

        # Stage 2: drop ambiguous aliases within the table
        col_alias_proposals, dropped_events = _resolve_intra_table_alias_collisions(
            table_name, {k: set(v) for k, v in col_alias_proposals.items()}
        )
        if dropped_events:
            _emit_collision_warnings(dropped_events)

        # Stage 3: finalize column entries and update alias_dict
        for name in col_types.keys():
            aliases_final = sorted(list(col_alias_proposals[name]))
            schema['tables'][table_name]['columns'][name] = {
                'aliases': aliases_final,
                'type': col_types[name],
                'labels': sorted(list(col_labels[name])),
            }
            # persist only the finalized aliases (no dropped ones)
            alias_dict[name] = aliases_final

    conn.close()
    return schema, alias_dict


def main():
    # --- UPDATED: Load the persistent alias dictionary ---
    alias_dictionary = load_or_initialize_yaml(ALIAS_DICT_PATH)
    print(f"Loaded {len(alias_dictionary)} canonical names from alias dictionary.")

    print(f"Querying database '{DB_PATH}' for schema...")
    schema_data, updated_alias_dict = get_schema_from_db(DB_PATH, alias_dictionary)

    if schema_data:
        os.makedirs(os.path.dirname(OUTPUT_SCHEMA_PATH), exist_ok=True)
        with open(OUTPUT_SCHEMA_PATH, 'w') as f:
            yaml.dump(schema_data, f, sort_keys=False)
        print(f"Schema successfully written to '{OUTPUT_SCHEMA_PATH}'.")

        # --- UPDATED: Save the enriched alias dictionary back to disk ---
        with open(ALIAS_DICT_PATH, 'w') as f:
            yaml.dump(updated_alias_dict, f, sort_keys=False)
        print(f"Alias dictionary updated and saved to '{ALIAS_DICT_PATH}'. Now contains {len(updated_alias_dict)} names.")

    else:
        print("Schema generation failed.")

if __name__ == '__main__':
    # You might want to run generate_db.py first to ensure test.db exists
    # import generate_db
    # generate_db.create_db()
    main()