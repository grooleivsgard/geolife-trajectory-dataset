import time
import copy
from queue import Queue

from Database import Database
import pandas as pd
import mysql.connector
from dataset import process_users, preprocess_activities, process_activity, process_trackpoint, read_file_to_list


def time_elapsed_str(start_time):
    elapsed = time.time() - start_time
    minutes = round(elapsed / 60, 0)
    seconds = round(elapsed % 60, 0)
    return f'{minutes} minutes and {seconds} seconds.'


# Task1
def open_connection() -> Database:
    database = None
    try:
        database = Database()
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    return database


def create_tables(database: Database, debug=False):
    # Table dicts
    user = {
        'name': "User",
        'attributes': ["id VARCHAR(3) NOT NULL", "has_labels BIT"],
        'primary': "id"
    }

    activity = {
        'name': 'Activity',
        'attributes': ['id BIGINT UNSIGNED NOT NULL', 'user_id VARCHAR(3) NOT NULL',
                       'transportation_mode VARCHAR(30)', 'start_date_time DATETIME', 'end_date_time DATETIME'],
        'primary': 'id',
        'foreign': {
            'key': 'user_id',
            'references': 'User(id)'
        },
    }

    trackpoint = {
        'name': 'TrackPoint',
        'attributes': ['id INT UNSIGNED NOT NULL AUTO_INCREMENT', 'activity_id BIGINT UNSIGNED NOT NULL', 'lat DOUBLE',
                       'lon DOUBLE',
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


def insert_row_and_get_id(database: Database, table_name, row: dict):
    # Preprocess for insertion
    if 'meta' in row.keys():
        del row['meta']
        if 'has_labels' in row:
            row['has_labels'] = int(row['has_labels'])

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


def insert_data(database: Database, data_path, labeled_ids, insert_threshold=10e4):
    start_time = time.time()
    users_rows = process_users(path=data_path, labeled_ids=labeled_ids)
    database.insert_batch(batch=copy.deepcopy(users_rows), table_name='User')
    num_users = len(users_rows)
    print(f"Inserted {num_users} users into User\n")

    activity_buffer = []
    trackpoint_buffer = []

    for i, user_row in enumerate(users_rows):
        activity_rows = preprocess_activities(user_row=user_row)

        for activity_row in activity_rows:
            activity, trackpoints_df = process_activity(user_row, activity_row=activity_row)
            if not activity:  # means number of trackpoints > 2500
                continue

            activity_buffer.append(activity)

            for _, trackpoint_row in trackpoints_df.iterrows():
                trackpoint = process_trackpoint(activity['id'], trackpoint_row)
                trackpoint_buffer.append(trackpoint)

            num_activities, num_trackpoints = len(activity_buffer), len(trackpoint_buffer)

            if num_activities + num_trackpoints > insert_threshold:
                insert_time = time.time()
                num_activities, num_trackpoints = len(activity_buffer), len(trackpoint_buffer)
                print(f'\nInserting: {num_activities} activities and {num_trackpoints} trackpoints')

                # Insert activities
                database.insert_batch(table_name='Activity', batch=list(activity_buffer))
                activity_buffer.clear()

                # Insert trackpoints
                database.insert_batch(table_name='TrackPoint', batch=list(trackpoint_buffer))
                trackpoint_buffer.clear()

                print(f'\tInsertion time: {time_elapsed_str(insert_time)}\n'
                      f'\tInserts per second: {int((num_trackpoints + num_activities) / (time.time() - insert_time))}\n')

        print(f'\rUser {user_row["id"]} processed ({i} / {num_users}), Time elapsed: {time_elapsed_str(start_time)}',
              end='')

    print(f'Insertion complete - Total time: {time_elapsed_str(start_time)}')


def execute():
    if __name__ == "__main__":
        data_path = './dataset/dataset/Data'
        labeled_ids = read_file_to_list('./dataset/dataset/labeled_ids.txt')
        database = open_connection()
        database.drop(['TrackPoint', 'Activity', 'User'], debug=False)

        create_tables(database, debug=False)
        insert_data(database, data_path, labeled_ids, insert_threshold=325 * 10e2)

        database.close_connection()


# Execution
execute()
