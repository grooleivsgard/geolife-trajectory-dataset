import time

from Database import Database
import pandas as pd
import mysql.connector
from dataset import process_users, preprocess_activities, process_activity, process_trackpoints, read_file_to_list


# Task1
def open_connection() -> Database:
    database = None
    try:
        database = Database()
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    return database


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
    user = {
        'name': "User",
        'attributes': ["id VARCHAR(3) NOT NULL", "has_labels BIT"],
        'primary': "id"
    }

    activity = {
        'name': 'Activity',
        'attributes': ['id INT NOT NULL AUTO_INCREMENT', 'user_id VARCHAR(3) NOT NULL',
                       'transportation_mode VARCHAR(30)', 'start_date_time DATETIME', 'end_date_time DATETIME'],
        'primary': 'id',
        'foreign': {
            'key': 'user_id',
            'references': 'User(id)'
        },
    }

    trackpoint = {
        'name': 'TrackPoint',
        'attributes': ['id INT NOT NULL AUTO_INCREMENT', 'activity_id INT NOT NULL', 'lat DOUBLE', 'lon DOUBLE',
                       'altitude INT', 'date_days DOUBLE', 'date_time DATETIME'],
        'primary': "id",
        'foreign': {
            'key': 'activity_id',
            'references': 'Activity(id)'
        }
    }

    # Execute queries for creating tables
    database.create_table(user['name'], user['attributes'], user['primary'], debug=debug)
    database.create_table(activity['name'], activity['attributes'], activity['primary'], activity['foreign'],
                          debug=debug)
    database.create_table(trackpoint['name'], trackpoint['attributes'], trackpoint['primary'], trackpoint['foreign'],
                          debug=debug)


def insert_batch(database: Database, table_name, batch: list):
    try:
        database.db_connection.start_transaction()

        df = pd.DataFrame(batch)
        if 'path' in batch[0].keys():
            df = df.drop(columns='path')
            df['has_labels'] = df['has_labels'].astype(int)
        data = [tuple(row) for row in df.values]
        columns = ', '.join(df.columns)
        placeholders = ', '.join(['%s'] * len(df.columns))
        query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

        database.cursor.executemany(query, data)
        database.db_connection.commit()
    except mysql.connector.Error as e:
        print(f"An error occurred: {e}")
        database.db_connection.rollback()


def insert_row_and_get_id(database: Database, table_name, row: dict):
    if 'path' in row.keys():
        del row['path']

    # Prepare the data and query for insertion
    data = tuple(row.values())
    columns = ', '.join(row.keys())
    placeholders = ', '.join(['%s'] * len(row.keys()))
    query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

    try:
        # Start a transaction to insert and retrieve its ID
        database.db_connection.start_transaction()
        database.cursor.execute(query, data)
        last_inserted_id = database.cursor.lastrowid
        database.db_connection.commit()

        return last_inserted_id
    except mysql.connector.Error as e:
        print(f"An error occurred: {e}")
        database.db_connection.rollback()  # Rollback the transaction in case of an error
        return None


def insert_data(database: Database, data_path, labeled_ids):
    users_rows = process_users(path=data_path, labeled_ids=labeled_ids)
    insert_batch(database=database, batch=users_rows, table_name='User')
    print(f"Inserted {len(users_rows)} users into User")

    for user_row in users_rows:
        activity_rows = preprocess_activities(user_rows=users_rows)
        num_activities = len(activity_rows)

        for i, activity_row in enumerate(activity_rows):
            activity, trackpoints_df = process_activity(user_row, activity_row=activity_row)
            if not activity:
                # Reduce overhead by skipping redundant processing of activities which will not be added anyway.
                continue

            # Insert activity and retrieve its ID
            activity_id = insert_row_and_get_id(database, 'Activity', activity)
            if activity_id is None:
                exit(1337)

            if (i + 1) % 100 == 0:
                print(f"Inserted activity {activity_id} / {num_activities}")

            trackpoints = process_trackpoints(activity_id, trackpoints_df)
            insert_batch(database, 'TrackPoint', trackpoints)


def execute():
    start_time = time.time()
    data_path = './dataset/dataset/Data'
    labeled_ids = read_file_to_list('./dataset/dataset/labeled_ids.txt')
    database = open_connection()

    create_tables(database, debug=True)
    insert_data(database, data_path, labeled_ids)

    close_connection(database)
    elapsed = time.time() - start_time

    minutes = elapsed / 60
    seconds = elapsed % 60
    print(f"Time: {minutes} minutes and {seconds} seconds.")


execute()
