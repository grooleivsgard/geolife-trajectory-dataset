from Database import Database

# Task1
# Write a Python database that does the following:
# 1. Connects to the MySQL server on your Ubuntu virtual machine.
def open_connection() -> Database:
    database = None
    try:
        database = Database()
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    return database;

def close_connection(database: Database):
    if database:
        try:
            database.connection.close_connection()
        except Exception as e:
            print("ERROR: Failed to close database:", e)
    else:
        print("Database is None.")
    


# 2. Creates and define the tables User, Activity and TrackPoint
def create_tables(database: Database, debug=False):
    # Table dicts
    user= {
        'name': "User",
        'attributes': ["id VARCHAR(3) NOT NULL", "has_labels BIT"], 
        'primary': "id"
    }

    activity = {
        'name' : 'Activity',
        'attributes': ['id INT NOT NULL AUTO_INCREMENT', 'user_id VARCHAR(3) NOT NULL', 'transportation_mode VARCHAR(30)', 'start_time_date DATETIME', 'end_date_time DATETIME'],
        'primary': 'id',
        'foreign': {
            'key': 'user_id',
            'references': 'User(id)'
        },
    }

    trackpoint = {
        'name' : 'TrackPoint',
        'attributes' : ['id INT NOT NULL AUTO_INCREMENT', 'activity_id INT NOT NULL', 'lat DOUBLE', 'lon DOUBLE', 'altitude INT', 'date_days DOUBLE', 'date_time DATETIME'],
        'primary' : "id",
        'foreign' : {
            'key': 'activity_id',
            'references': 'Activity(id)'
        }
    }

    # Execute queries for creating tables
    database.create_table(user['name'], user['attributes'], user['primary'], debug=debug)
    database.create_table(activity['name'], activity['attributes'], activity['primary'], activity['foreign'], debug=debug)
    database.create_table(trackpoint['name'], trackpoint['attributes'], trackpoint['primary'], trackpoint['foreign'], debug=debug)

database = open_connection()
create_tables(database, debug=True)
close_connection(database)

# 3. Inserts the data from the Geolife dataset into your MySQL database

# Execution


