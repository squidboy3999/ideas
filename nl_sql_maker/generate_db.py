import sqlite3
import os
import random
from datetime import datetime, timedelta
from shapely.geometry import Point, LineString, Polygon
from shapely.wkb import dumps, loads

DB_NAME = 'test.db'
SCHEMA_FILE = os.path.join(os.path.dirname(__file__), '../natural_language_sql/schema/schema.yaml')

# Helper function to create geometry objects and convert to WKB
def create_wkb(geom_type, coords):
    if geom_type == 'POINT':
        geom = Point(coords)
    elif geom_type == 'LINESTRING':
        geom = LineString(coords)
    elif geom_type == 'POLYGON':
        geom = Polygon(coords)
    else:
        return None
    return dumps(geom, hex=True)

def generate_dummy_data(cursor):
    # This SQL now uses SpatiaLite's `AddGeometryColumn` function
    # instead of trying to define the column type in the CREATE TABLE statement.
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username VARCHAR(50) NOT NULL,
            age INT,
            balance DECIMAL(10, 2),
            is_active BOOLEAN,
            last_login TIMESTAMP
        );
    """)
    # Add the GEOMETRY column using SpatiaLite's function
    cursor.execute("SELECT AddGeometryColumn('users', 'location', 4326, 'POINT', 2);")
    
    users_data = []
    for i in range(1, 11):
        username = f"user_{i}"
        age = random.randint(20, 60)
        balance = random.uniform(100.0, 1000.0)
        is_active = random.choice([1, 0]) # SQLite stores BOOLEAN as 0 or 1
        last_login = datetime.now() - timedelta(days=random.randint(1, 365))
        
        # Insert data without the geometry column first
        cursor.execute("INSERT INTO users (user_id, username, age, balance, is_active, last_login) VALUES (?, ?, ?, ?, ?, ?)", (i, username, age, balance, is_active, last_login))
        
        # Then, update the geometry column with the SpatiaLite-formatted WKB
        location = Point(random.uniform(-180, 180), random.uniform(-90, 90))
        cursor.execute("UPDATE users SET location = ST_GeomFromText(?, 4326) WHERE user_id = ?", (location.wkt, i))
    
    # Table 2: Sales (same as before)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sales (
            sale_id INTEGER PRIMARY KEY,
            user_id INTEGER,
            product_name TEXT,
            sale_date DATE,
            quantity INT,
            price FLOAT,
            FOREIGN KEY(user_id) REFERENCES users(user_id)
        );
    """)

    sales_data = []
    for i in range(1, 21):
        user_id = random.randint(1, 10)
        product_name = random.choice(['Laptop', 'Mouse', 'Keyboard', 'Monitor'])
        sale_datetime_obj = datetime.now() - timedelta(days=random.randint(1, 180))
        sale_date = sale_datetime_obj.strftime('%Y-%m-%d')
        quantity = random.randint(1, 5)
        price = random.uniform(50.0, 1500.0)
        sales_data.append((i, user_id, product_name, sale_date, quantity, price))

    cursor.executemany("INSERT INTO sales VALUES (?, ?, ?, ?, ?, ?)", sales_data)
    
    # Table 3: Regions (with polygon geometry)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS regions (
            region_id INTEGER PRIMARY KEY,
            name VARCHAR(50)
        );
    """)
    # Add the GEOMETRY column
    cursor.execute("SELECT AddGeometryColumn('regions', 'boundaries', 4326, 'POLYGON', 2);")
    
    regions_data = [
        (1, 'North', Polygon(((0, 0), (0, 45), (90, 45), (90, 0), (0, 0)))),
        (2, 'South', Polygon(((-90, -90), (-90, 0), (0, 0), (0, -90), (-90, -90)))),
    ]
    
    for region_id, name, polygon in regions_data:
        cursor.execute("INSERT INTO regions (region_id, name) VALUES (?, ?)", (region_id, name))
        cursor.execute("UPDATE regions SET boundaries = ST_GeomFromText(?, 4326) WHERE region_id = ?", (polygon.wkt, region_id))

    print("Dummy data generated and inserted successfully.")

def create_db():
    if os.path.exists(DB_NAME):
        os.remove(DB_NAME)
    
    conn = sqlite3.connect(DB_NAME)
    
    # Enable SpatiaLite extension loading
    conn.enable_load_extension(True)
    try:
        # Load the SpatiaLite extension
        conn.execute("SELECT load_extension('mod_spatialite')")
        print("SpatiaLite extension loaded successfully.")
    except sqlite3.OperationalError as e:
        print(f"Error loading SpatiaLite extension: {e}")
        print("Please ensure 'mod_spatialite' is correctly installed and in the system's path.")
        conn.close()
        return

    cursor = conn.cursor()
    # SpatiaLite requires its metadata tables to be initialized
    cursor.execute("SELECT InitSpatialMetaData(1)")
    
    generate_dummy_data(cursor)
    conn.commit()
    conn.close()
    print(f"Database '{DB_NAME}' created.")

if __name__ == '__main__':
    create_db()