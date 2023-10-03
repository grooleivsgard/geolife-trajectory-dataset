import time
import copy
import DbConnector
from Database import Database
import pandas as pd
import mysql.connector
from dataset import process_users, preprocess_activities, process_activity, process_trackpoint, read_file_to_list
import queries


def time_elapsed_str(start_time):
    """
    Calculate the time elapsed from the start_time to now.

    :param start_time: The start time to calculate elapsed time from.
    :return: A string representing the elapsed time in minutes and seconds.
    """
    elapsed = time.time() - start_time
    minutes = round(elapsed / 60, 0)
    seconds = round(elapsed % 60, 0)
    return f'{minutes} minutes and {seconds} seconds.'


def open_connection() -> Database:
    """
    Open a connection to the database.

    :return: A Database object representing the connection to the database.
    """
    database = None
    try:
        database = Database()
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    return database


def create_tables(database: Database, debug=False):
    """
    Create tables in the database.

    :param database: The Database object to operate on.
    :param debug: A flag to print debug information.
    """

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


def push_buffers_to_db(database, activity_buffer, trackpoint_buffer, num_activities, num_trackpoints):
    """
    Push processed activities and trackpoints to the database.

    :param database: The Database object to operate on.
    :param activity_buffer: A list of buffered activities.
    :param trackpoint_buffer: A list of buffered trackpoints.
    :param num_activities: The number of activities.
    :param num_trackpoints: The number of trackpoints.
    """
    insert_time = time.time()
    print(f'\nInserting: {num_activities} activities and {num_trackpoints} trackpoints')

    # Insert activities
    database.insert_batch(table_name='Activity', batch=list(activity_buffer))
    activity_buffer.clear()

    # Insert trackpoints
    database.insert_batch(table_name='TrackPoint', batch=list(trackpoint_buffer))
    trackpoint_buffer.clear()

    print(f'\tInsertion time: {time_elapsed_str(insert_time)}\n'
          f'\tInserts per second: {int((num_trackpoints + num_activities) / (time.time() - insert_time))}\n')


def insert_data(database: Database, data_path, labeled_ids, insert_threshold=10e4):
    """
    Insert data into the database.

    :param database: The Database object to operate on.
    :param data_path: The path to the data to be inserted.
    :param labeled_ids: A list of labeled IDs.
    :param insert_threshold: The threshold for batch insertion.
    """
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
                push_buffers_to_db(database, activity_buffer, trackpoint_buffer, num_activities, num_trackpoints)

        print(f'\rUser {user_row["id"]} processed ({i + 1} / {num_users}), Time elapsed: {time_elapsed_str(start_time)}',
            end='')

    print(f'\nInsertion complete - Total time: {time_elapsed_str(start_time)}')


def upload_data():
    """
    Execute the database operations.
    """
    if __name__ == "__main__":
        data_path = './dataset/dataset/Data'
        labeled_ids = read_file_to_list('./dataset/dataset/labeled_ids.txt')
        database = open_connection()
        # database.drop(['TrackPoint', 'Activity', 'User'], debug=False)

        # create_tables(database, debug=False)
        # insert_data(database, data_path, labeled_ids, insert_threshold=325 * 10e2)
        connector = DbConnector.DbConnector()
        cursor = connector.get_cursor()
        queries.iterate_results(cursor, 9)

        database.close_connection()


# Execution
upload_data()
