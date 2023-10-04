import time
import copy
import pandas as pd
from Database import Database
from data_processing import (process_users, preprocess_activities, process_activity, process_trackpoint,
                             read_file_to_list)
from helpers import time_elapsed_str


class Part1:
    def __init__(self):
        """
        Inits part 1
        :param database: The Database object to operate on.
        """
        self.database = Database()

    def create_tables(self, debug=False):
        """
        Create tables in the database.

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
            'attributes': ['id INT UNSIGNED NOT NULL AUTO_INCREMENT', 'activity_id BIGINT UNSIGNED NOT NULL',
                           'lat DOUBLE',
                           'lon DOUBLE', 'altitude INT', 'date_days DOUBLE', 'date_time DATETIME'],
            'primary': "id",
            'foreign': {
                'key': 'activity_id',
                'references': 'Activity(id)'
            }
        }

        # Execute queries for creating tables
        self.database.create_table(user['name'], user['attributes'], user['primary'], debug=debug)
        self.database.create_table(activity['name'], activity['attributes'], activity['primary'], activity['foreign'],
                                   debug=debug)
        self.database.create_table(trackpoint['name'], trackpoint['attributes'], trackpoint['primary'],
                                   trackpoint['foreign'], debug=debug)

    def push_buffers_to_db(self, activity_buffer, trackpoint_buffer, num_activities, num_trackpoints):
        """
        Push processed activities and trackpoints to the database.

        :param activity_buffer: A list of buffered activities.
        :param trackpoint_buffer: A list of buffered trackpoints.
        :param num_activities: The number of activities.
        :param num_trackpoints: The number of trackpoints.
        """
        insert_time = time.time()
        print(f'\nInserting: {num_activities} activities and {num_trackpoints} trackpoints')

        # Insert activities
        self.database.insert_batch(table_name='Activity', batch=list(activity_buffer))
        activity_buffer.clear()

        # Insert trackpoints
        self.database.insert_batch(table_name='TrackPoint', batch=list(trackpoint_buffer))
        trackpoint_buffer.clear()

        print(f'\tInsertion time: {time_elapsed_str(insert_time)}\n'
              f'\tInserts per second: {int((num_trackpoints + num_activities) / (time.time() - insert_time))}\n')

    def insert_data(self, data_path, labeled_ids, insert_threshold=10e4):
        """
        Insert data into the database.

        :param data_path: The path to the data to be inserted.
        :param labeled_ids: A list of labeled IDs.
        :param insert_threshold: The threshold for batch insertion.
        """
        start_time = time.time()
        users_rows = process_users(path=data_path, labeled_ids=labeled_ids)
        self.database.insert_batch(batch=copy.deepcopy(users_rows), table_name='User')
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
                    self.push_buffers_to_db(activity_buffer, trackpoint_buffer, num_activities, num_trackpoints)

            print(
                f'\rUser {user_row["id"]} processed ({i + 1} / {num_users}), Time elapsed: {time_elapsed_str(start_time)}',
                end='')

        print(f'\nInsertion complete - Total time: {time_elapsed_str(start_time)}')

    def upload_data(self):
        """
        Execute the database operations.
        """
        data_path = './dataset/dataset/Data'
        labeled_ids = read_file_to_list('./dataset/dataset/labeled_ids.txt')
        self.database.drop(['TrackPoint', 'Activity', 'User'], debug=False)
        self.create_tables(debug=False)
        self.insert_data(data_path, labeled_ids, insert_threshold=325 * 10e2)
        self.database.close_connection()
        self.database = None
