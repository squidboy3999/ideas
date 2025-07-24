import yaml
import collections
from .action_templates import sql_action_templates, postgis_action_templates

def process_schema(yaml_file_path):
    """
    Loads schema from a YAML file and generates a structured list of actions,
    applicable columns, and keywords based on data types and metadata.
    """
    try:
        with open(yaml_file_path, 'r') as file:
            schema = yaml.safe_load(file)
    except FileNotFoundError:
        print(f"Error: YAML file not found at {yaml_file_path}")
        return None
    except yaml.YAMLError as e:
        print(f"Error parsing YAML file: {e}")
        return None

    all_actions = []
    all_keywords = set()
    all_columns_info = collections.defaultdict(list) # Stores columns by their qualified name (table.column)

    # First, collect all column information
    for table in schema.get('tables', []):
        table_name = table['name']
        for column in table.get('columns', []):
            col_name = column['name']
            qualified_col_name = f"{table_name}.{col_name}"
            all_columns_info[qualified_col_name] = {
                'table': table_name,
                'column': col_name,
                'data_type': column['data_type'].upper(), # Normalize type to uppercase
                'metadata': [m.lower() for m in column.get('metadata', [])] # Normalize metadata to lowercase
            }

    # Combine templates
    all_action_templates = sql_action_templates + postgis_action_templates

    # Populate actions and keywords
    for template in all_action_templates:
        action_entry = {
            'name': template['name'],
            'sql_func': template['sql_func'],
            'keywords': template['keywords'],
            'applicable_columns_by_type': collections.defaultdict(list) # Placeholder for categorized columns
        }
        all_keywords.update(template['keywords'])

        for qualified_col_name, col_info in all_columns_info.items():
            for placeholder_type, condition_func in template['applies_to'].items():
                if condition_func(col_info):
                    action_entry['applicable_columns_by_type'][placeholder_type].append(qualified_col_name)

        # Only add actions that have at least one applicable column
        if any(action_entry['applicable_columns_by_type'].values()):
            all_actions.append(action_entry)

    # Return the structured data
    return {
        'actions': all_actions,
        'keywords': sorted(list(all_keywords))
    }

if __name__ == "__main__":
    output_data = process_schema('schema.yml')

    if output_data:
        print("--- Generated Actions and Keywords ---")
        for action in output_data['actions']:
            print(f"\nAction: {action['name']} (SQL: {action['sql_func']})")
            print(f"  Keywords: {', '.join(action['keywords'])}")
            for p_type, cols in action['applicable_columns_by_type'].items():
                if cols:
                    print(f"  - {p_type.replace('_cols', '').replace('_geom', ' geometry').replace('_id', ' ID')}: {', '.join(cols)}")
        print(f"\n--- All Identified Keywords ---")
        print(f"{', '.join(output_data['keywords'])}")

        # Example of how you might use this data for a specific column/action
        print("\n--- Example: Filtering actions for 'customers.age' ---")
        target_col = 'customers.age'
        applicable_for_age = []
        for action in output_data['actions']:
            for p_type, cols in action['applicable_columns_by_type'].items():
                if target_col in cols:
                    applicable_for_age.append(action['name'])
        print(f"Actions applicable to '{target_col}': {', '.join(applicable_for_age)}")

        print("\n--- Example: Filtering actions for 'locations.geom_point' ---")
        target_col = 'locations.geom_point'
        applicable_for_geom_point = []
        for action in output_data['actions']:
            for p_type, cols in action['applicable_columns_by_type'].items():
                if target_col in cols:
                    applicable_for_geom_point.append(action['name'])
        print(f"Actions applicable to '{target_col}': {', '.join(applicable_for_geom_point)}")
