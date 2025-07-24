# action_templates.py

# Tier 1: Simple & Common SQL Queries
sql_action_templates = [
    {
        'name': 'Equality',
        'sql_func': '=',
        'keywords': ['=', 'is', 'equals'],
        'applies_to': {
            'numeric_cols': lambda col_info: 'id' not in col_info['metadata'] and col_info['data_type'] in ['INT', 'DECIMAL', 'FLOAT'],
            'id_cols': lambda col_info: 'id' in col_info['metadata'] and col_info['data_type'] in ['INT'],
            'text_cols': lambda col_info: col_info['data_type'] in ['VARCHAR', 'TEXT'],
            'date_time_cols': lambda col_info: col_info['data_type'] in ['DATE', 'TIMESTAMP'],
            'boolean_cols': lambda col_info: col_info['data_type'] == 'BOOLEAN'
        }
    },
    {
        'name': 'Inequality',
        'sql_func': '!=',
        'keywords': ['!=', '<>', 'not equal to'],
        'applies_to': {
            'numeric_cols': lambda col_info: 'id' not in col_info['metadata'] and col_info['data_type'] in ['INT', 'DECIMAL', 'FLOAT'],
            'text_cols': lambda col_info: col_info['data_type'] in ['VARCHAR', 'TEXT'],
            'date_time_cols': lambda col_info: col_info['data_type'] in ['DATE', 'TIMESTAMP'],
            'boolean_cols': lambda col_info: col_info['data_type'] == 'BOOLEAN'
        }
    },
    {
        'name': 'GreaterThan',
        'sql_func': '>',
        'keywords': ['>', 'greater than', 'more than'],
        'applies_to': {
            'numeric_cols': lambda col_info: 'id' not in col_info['metadata'] and col_info['data_type'] in ['INT', 'DECIMAL', 'FLOAT'],
            'date_time_cols': lambda col_info: col_info['data_type'] in ['DATE', 'TIMESTAMP']
        }
    },
    {
        'name': 'LessThan',
        'sql_func': '<',
        'keywords': ['<', 'less than', 'under'],
        'applies_to': {
            'numeric_cols': lambda col_info: 'id' not in col_info['metadata'] and col_info['data_type'] in ['INT', 'DECIMAL', 'FLOAT'],
            'date_time_cols': lambda col_info: col_info['data_type'] in ['DATE', 'TIMESTAMP']
        }
    },
    {
        'name': 'GreaterThanOrEqual',
        'sql_func': '>=',
        'keywords': ['>=', 'greater than or equal to', 'at least'],
        'applies_to': {
            'numeric_cols': lambda col_info: 'id' not in col_info['metadata'] and col_info['data_type'] in ['INT', 'DECIMAL', 'FLOAT'],
            'date_time_cols': lambda col_info: col_info['data_type'] in ['DATE', 'TIMESTAMP']
        }
    },
    {
        'name': 'LessThanOrEqual',
        'sql_func': '<=',
        'keywords': ['<=', 'less than or equal to', 'at most'],
        'applies_to': {
            'numeric_cols': lambda col_info: 'id' not in col_info['metadata'] and col_info['data_type'] in ['INT', 'DECIMAL', 'FLOAT'],
            'date_time_cols': lambda col_info: col_info['data_type'] in ['DATE', 'TIMESTAMP']
        }
    },
    {
        'name': 'InSet',
        'sql_func': 'IN',
        'keywords': ['IN', 'is one of', 'among'],
        'applies_to': {
            'all_cols': lambda col_info: True # Applies to all comparable columns
        }
    },
    {
        'name': 'IsNull',
        'sql_func': 'IS NULL',
        'keywords': ['IS NULL', 'is null', 'has no value'],
        'applies_to': {
            'all_cols': lambda col_info: True
        }
    },
    {
        'name': 'IsNotNull',
        'sql_func': 'IS NOT NULL',
        'keywords': ['IS NOT NULL', 'is not null', 'has a value'],
        'applies_to': {
            'all_cols': lambda col_info: True
        }
    },
    {
        'name': 'LikePattern',
        'sql_func': 'LIKE',
        'keywords': ['LIKE', 'contains', 'matches'],
        'applies_to': {
            'text_cols': lambda col_info: col_info['data_type'] in ['VARCHAR', 'TEXT'] and 'searchable' in col_info['metadata']
        }
    },
    {
        'name': 'Count',
        'sql_func': 'COUNT',
        'keywords': ['COUNT', 'number of', 'how many'],
        'applies_to': {
            'all_cols': lambda col_info: True,
            'distinct_cols': lambda col_info: True # Can count distinct for any column
        }
    },
    {
        'name': 'Sum',
        'sql_func': 'SUM',
        'keywords': ['SUM', 'total of'],
        'applies_to': {
            'numeric_cols': lambda col_info: col_info['data_type'] in ['INT', 'DECIMAL', 'FLOAT'] and 'monetary' in col_info['metadata']
        }
    },
    {
        'name': 'Average',
        'sql_func': 'AVG',
        'keywords': ['AVG', 'average of'],
        'applies_to': {
            'numeric_cols': lambda col_info: col_info['data_type'] in ['INT', 'DECIMAL', 'FLOAT']
        }
    },
    {
        'name': 'Minimum',
        'sql_func': 'MIN',
        'keywords': ['MIN', 'lowest', 'earliest'],
        'applies_to': {
            'numeric_cols': lambda col_info: col_info['data_type'] in ['INT', 'DECIMAL', 'FLOAT'],
            'date_time_cols': lambda col_info: col_info['data_type'] in ['DATE', 'TIMESTAMP'],
            'text_cols': lambda col_info: col_info['data_type'] in ['VARCHAR', 'TEXT'] # Alphabetical min
        }
    },
    {
        'name': 'Maximum',
        'sql_func': 'MAX',
        'keywords': ['MAX', 'highest', 'latest'],
        'applies_to': {
            'numeric_cols': lambda col_info: col_info['data_type'] in ['INT', 'DECIMAL', 'FLOAT'],
            'date_time_cols': lambda col_info: col_info['data_type'] in ['DATE', 'TIMESTAMP'],
            'text_cols': lambda col_info: col_info['data_type'] in ['VARCHAR', 'TEXT'] # Alphabetical max
        }
    },
    # Tier 2: Moderately Complex & Common SQL Queries
    {
        'name': 'Between',
        'sql_func': 'BETWEEN',
        'keywords': ['BETWEEN', 'between'],
        'applies_to': {
            'numeric_cols': lambda col_info: 'id' not in col_info['metadata'] and col_info['data_type'] in ['INT', 'DECIMAL', 'FLOAT'],
            'date_time_cols': lambda col_info: col_info['data_type'] in ['DATE', 'TIMESTAMP']
        }
    },
    {
        'name': 'OrderByAscending', # Updated rule
        'sql_func': 'ORDER BY ASC',
        'keywords': ['ORDER BY ASC', 'sorted by ascending', 'from lowest to highest'],
        'applies_to': {
            'all_cols': lambda col_info: True
        }
    },
    {
        'name': 'OrderByDescending', # Updated rule
        'sql_func': 'ORDER BY DESC',
        'keywords': ['ORDER BY DESC', 'sorted by descending', 'from highest to lowest'],
        'applies_to': {
            'all_cols': lambda col_info: True
        }
    },
    {
        'name': 'GroupBy',
        'sql_func': 'GROUP BY',
        'keywords': ['GROUP BY', 'group by'],
        'applies_to': {
            'all_cols': lambda col_info: True
        }
    },
    {
        'name': 'Having',
        'sql_func': 'HAVING',
        'keywords': ['HAVING', 'having'], # Used with aggregated results
        'applies_to': {
            'numeric_agg_cols': lambda col_info: True # Placeholder for aggregated columns, requires context
        }
    },
    {
        'name': 'Distinct',
        'sql_func': 'DISTINCT',
        'keywords': ['DISTINCT', 'unique'],
        'applies_to': {
            'all_cols': lambda col_info: True
        }
    },
    {
        'name': 'Limit',
        'sql_func': 'LIMIT',
        'keywords': ['LIMIT', 'top', 'first', 'only'],
        'applies_to': {
            'none': lambda col_info: True # Applies to the query result set, not a specific column
        }
    },
    {
        'name': 'Extract',
        'sql_func': 'EXTRACT',
        'keywords': ['EXTRACT', 'year of', 'month of', 'day of'], # More specific keywords would be needed per unit
        'applies_to': {
            'date_time_cols': lambda col_info: col_info['data_type'] in ['DATE', 'TIMESTAMP']
        }
    },
    {
        'name': 'Length',
        'sql_func': 'LENGTH',
        'keywords': ['LENGTH', 'length of'],
        'applies_to': {
            'text_cols': lambda col_info: col_info['data_type'] in ['VARCHAR', 'TEXT']
        }
    },
    {
        'name': 'Concat',
        'sql_func': 'CONCAT',
        'keywords': ['CONCAT', 'concatenate', 'combine'],
        'applies_to': {
            'text_cols': lambda col_info: col_info['data_type'] in ['VARCHAR', 'TEXT'] # Can concat two or more text columns
        }
    },
    {
        'name': 'Cast',
        'sql_func': 'CAST',
        'keywords': ['CAST', 'as'], # e.g., "cast column as text"
        'applies_to': {
            'all_cols': lambda col_info: True # Can cast most types to others
        }
    },
]

# Tier 1: Simple & Common PostGIS Queries
postgis_action_templates = [
    {
        'name': 'ST_Distance',
        'sql_func': 'ST_Distance',
        'keywords': ['ST_Distance', 'distance from', 'how far'],
        'applies_to': {
            'point_geom_cols': lambda col_info: col_info['data_type'].startswith('GEOMETRY(POINT') or col_info['data_type'].startswith('GEOGRAPHY(POINT'),
            'line_geom_cols': lambda col_info: col_info['data_type'].startswith('GEOMETRY(LINESTRING') or col_info['data_type'].startswith('GEOGRAPHY(LINESTRING'),
            'polygon_geom_cols': lambda col_info: col_info['data_type'].startswith('GEOMETRY(POLYGON') or col_info['data_type'].startswith('GEOGRAPHY(POLYGON'),
            'latitude_cols': lambda col_info: 'latitude' in col_info['metadata'] and col_info['data_type'] in ['DECIMAL', 'FLOAT'],
            'longitude_cols': lambda col_info: 'longitude' in col_info['metadata'] and col_info['data_type'] in ['DECIMAL', 'FLOAT']
        }
    },
    {
        'name': 'ST_Intersects',
        'sql_func': 'ST_Intersects',
        'keywords': ['ST_Intersects', 'intersects', 'overlaps with'],
        'applies_to': {
            'geom_cols': lambda col_info: col_info['data_type'].startswith('GEOMETRY') or col_info['data_type'].startswith('GEOGRAPHY')
        }
    },
    {
        'name': 'ST_Area',
        'sql_func': 'ST_Area',
        'keywords': ['ST_Area', 'area of'],
        'applies_to': {
            'polygon_geom_cols': lambda col_info: col_info['data_type'].startswith('GEOMETRY(POLYGON') or col_info['data_type'].startswith('GEOGRAPHY(POLYGON')
        }
    },
    {
        'name': 'ST_Length',
        'sql_func': 'ST_Length',
        'keywords': ['ST_Length', 'length of'],
        'applies_to': {
            'line_geom_cols': lambda col_info: col_info['data_type'].startswith('GEOMETRY(LINESTRING') or col_info['data_type'].startswith('GEOGRAPHY(LINESTRING')
        }
    },
    {
        'name': 'ST_X',
        'sql_func': 'ST_X',
        'keywords': ['ST_X', 'x coordinate', 'longitude of'],
        'applies_to': {
            'point_geom_cols': lambda col_info: col_info['data_type'].startswith('GEOMETRY(POINT') or col_info['data_type'].startswith('GEOGRAPHY(POINT')
        }
    },
    {
        'name': 'ST_Y',
        'sql_func': 'ST_Y',
        'keywords': ['ST_Y', 'y coordinate', 'latitude of'],
        'applies_to': {
            'point_geom_cols': lambda col_info: col_info['data_type'].startswith('GEOMETRY(POINT') or col_info['data_type'].startswith('GEOGRAPHY(POINT')
        }
    },
    {
        'name': 'ST_Within',
        'sql_func': 'ST_Within',
        'keywords': ['ST_Within', 'within', 'inside of'],
        'applies_to': {
            'geom_cols': lambda col_info: col_info['data_type'].startswith('GEOMETRY') or col_info['data_type'].startswith('GEOGRAPHY')
        }
    },
    {
        'name': 'ST_Contains',
        'sql_func': 'ST_Contains',
        'keywords': ['ST_Contains', 'contains'],
        'applies_to': {
            'geom_cols': lambda col_info: col_info['data_type'].startswith('GEOMETRY') or col_info['data_type'].startswith('GEOGRAPHY')
        }
    },
    {
        'name': 'ST_GeometryType',
        'sql_func': 'ST_GeometryType',
        'keywords': ['ST_GeometryType', 'geometry type of'],
        'applies_to': {
            'geom_cols': lambda col_info: col_info['data_type'].startswith('GEOMETRY') or col_info['data_type'].startswith('GEOGRAPHY')
        }
    },
    {
        'name': 'BoundingBoxIntersects',
        'sql_func': '&&',
        'keywords': ['&&', 'bounding box intersects'],
        'applies_to': {
            'geom_cols': lambda col_info: col_info['data_type'].startswith('GEOMETRY') # Typically for GEOMETRY, not GEOGRAPHY
        }
    },
    # Tier 2: Moderately Complex & Common PostGIS Queries
    {
        'name': 'ST_Buffer',
        'sql_func': 'ST_Buffer',
        'keywords': ['ST_Buffer', 'buffer around', 'within distance of'],
        'applies_to': {
            'geom_cols': lambda col_info: col_info['data_type'].startswith('GEOMETRY') or col_info['data_type'].startswith('GEOGRAPHY')
        }
    },
    {
        'name': 'ST_Union',
        'sql_func': 'ST_Union',
        'keywords': ['ST_Union', 'union of', 'combine areas'],
        'applies_to': {
            'geom_cols': lambda col_info: col_info['data_type'].startswith('GEOMETRY') or col_info['data_type'].startswith('GEOGRAPHY')
        }
    },
    {
        'name': 'ST_Centroid',
        'sql_func': 'ST_Centroid',
        'keywords': ['ST_Centroid', 'center point of'],
        'applies_to': {
            'polygon_geom_cols': lambda col_info: col_info['data_type'].startswith('GEOMETRY(POLYGON') or col_info['data_type'].startswith('GEOGRAPHY(POLYGON')
        }
    },
    {
        'name': 'ST_Simplify',
        'sql_func': 'ST_Simplify',
        'keywords': ['ST_Simplify', 'simplify', 'smoothen'],
        'applies_to': {
            'geom_cols': lambda col_info: col_info['data_type'].startswith('GEOMETRY') or col_info['data_type'].startswith('GEOGRAPHY')
        }
    },
    {
        'name': 'ST_Touches',
        'sql_func': 'ST_Touches',
        'keywords': ['ST_Touches', 'touches'],
        'applies_to': {
            'geom_cols': lambda col_info: col_info['data_type'].startswith('GEOMETRY') or col_info['data_type'].startswith('GEOGRAPHY')
        }
    },
    {
        'name': 'ST_Crosses',
        'sql_func': 'ST_Crosses',
        'keywords': ['ST_Crosses', 'crosses'],
        'applies_to': {
            'geom_cols': lambda col_info: col_info['data_type'].startswith('GEOMETRY') or col_info['data_type'].startswith('GEOGRAPHY')
        }
    },
    {
        'name': 'NearestNeighbor',
        'sql_func': '<->',
        'keywords': ['<->', 'nearest to', 'closest'],
        'applies_to': {
            'point_geom_cols': lambda col_info: col_info['data_type'].startswith('GEOMETRY(POINT') or col_info['data_type'].startswith('GEOGRAPHY(POINT')
        }
    },
    {
        'name': 'ST_Transform',
        'sql_func': 'ST_Transform',
        'keywords': ['ST_Transform', 'transform to SRID'],
        'applies_to': {
            'geom_cols': lambda col_info: col_info['data_type'].startswith('GEOMETRY') # Usually for GEOMETRY, not GEOGRAPHY
        }
    }
]
