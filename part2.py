import pandas as pd
import geopandas as gpd
import time
import mysql
from tabulate import tabulate
from shapely.geometry import Point
from datetime import timedelta
from sqlalchemy import create_engine
from haversine import haversine, Unit
from helpers import time_elapsed_str
from rtree import index
from Database import Database


def format_and_print(label, result):
    # If result is a scalar (not list or tuple)
    if not isinstance(result, (list, tuple)):
        print(f"  {label} {result}")
        return

    # If result is a list or tuple
    for item in result:
        # If item is a tuple, print its values separated by commas
        if isinstance(item, tuple):
            print(f"  {label}: {' ,'.join(map(str, item))}")
        # If item is a scalar, print it directly
        else:
            print(f"  {label} {item}")


def print_question(task_num: int, question_text: str):
    print(f'Task {task_num}:')
    print(question_text)
    print("Querying... Please wait.", end='')


def print_result(result_df: pd.DataFrame or dict[str, list], floatfmt=".0f", filename=None):
    print('\r', end='')

    display = tabulate(result_df, headers='keys', tablefmt='grid', floatfmt=floatfmt, showindex=False)
    print(display + "\n")

    # Write to file if given
    if filename:
        with open(f'task_outputs/{filename}.txt', 'w') as f:
            f.write(display)


class Part2:
    def __init__(self):
        self.database = Database()
        self.cursor = self.database.cursor

    def iterate_results(self, task_num=None):
        tasks = [
            {
                "description": "Task 1: How many users, activities and trackpoints are there in the dataset (after it is "
                               "inserted into the database)?",
                "methods": [self.get_user_count, self.get_activity_count, self.get_tp_count],
                "labels": ['Total users', 'Total activities', 'Total trackpoints']},
            {
                "description": "Task 2: Find the average, maximum and minimum number of trackpoints per user.",
                "methods": [self.get_avg_tp, self.get_max_tp, self.get_min_tp],
                "labels": ['Average trackpoints', 'Maximum trackpoints', 'Minimum trackpoint:']},

            {
                "description": "Task 3: Find the top 15 users with the highest number of activities.",
                "methods": [self.get_top_15_activities],
                "labels": ['Highly active user']},

            {
                "description": "Task 4: Find all users who have taken a bus.",
                "methods": [self.get_transportation_by_bus],
                "labels": ['Bus-taking user']},

            {
                "description": "Task 5: List the top 10 users by their amount of different transportation modes.",
                "methods": [self.get_distinct_transportation_modes],
                "labels": ['Users by number of transportation modes']},

            {
                "description": "Task 6: Find activities that are registered multiple times. You should find the query "
                               "even if it gives zero result.",
                "methods": [self.get_duplicate_activities],
                "labels": ['Duplicate activities']},

            {
                "description": "Task 7a: Find the number of users that have started an activity in one day and ended the "
                               "activity the next day.",
                "methods": [self.get_count_multiple_day_activities],
                "labels": ['Users with activities spanning multiple days']},

            {  # -- task 8
                "description": "Task 7b: List the transportation mode, user id and duration for these activities.",
                "methods": [self.get_list_multiple_day_activities],
                "labels": ['Multiple-day activity']},

            {
                "description": "Task 8: Find the number of users which have been close to each other in time and space. "
                               "Close is defined as the same space (50 meters) and for the same half minute (30 seconds)",
                "methods": [self.get_users_in_proximity],
                "labels": ['Number of users in close proximity']},

            {
                "description": "Task 9: Find the top 15 users who have gained the most altitude meters. Output should be "
                               "a table with (id, total meters gained per user). Remember that some altitude-values are "
                               "invalid",
                "methods": [self.get_top_altitude_gains],
                "labels": ['Height-climbing users']},

            {
                "description": "Task 10: Find the users that have traveled the longest total distance in one day for each "
                               "transportation mode.",
                "methods": [self.get_longest_distance_per_transportation],
                "labels": ['Users with longest distance per transportation mode']},

            {
                "description": "Task 11: Find all users who have invalid activities, and the number of invalid activities "
                               "per user. An invalid activity is defined as an activity with consecutive trackpoints "
                               "where the timestamps deviate with at least 5 minutes.",
                "methods": [self.get_invalid_activities],
                "labels": ['Invalid activities per user']},

            {
                "description": "Task 12:  Find all users who have registered transportation_mode and their most used "
                               "transportation_mode.",
                "methods": [self.get_most_used_transportations],
                "labels": ['Users with their most used transportation modes']}

        ]

        if task_num:
            if 1 <= task_num <= len(tasks):  # Check if the provided task number is valid
                tasks_to_execute = [tasks[task_num - 1]]  # Get the task corresponding to the task number
            else:
                print(f"Invalid task number: {task_num}. Valid range is 1 to {len(tasks)}")
                return
        else:
            tasks_to_execute = tasks  # Execute all tasks if no specific task number is provided

        for task in tasks_to_execute:
            print(f"{task['description']}\n Answer:")
            for method, label in zip(task['methods'], task['labels']):
                result = method()
                format_and_print(label, result)
            print("\n")  # Add a newline between different tasks for better readability.

    def execute_query(self, query, params=None):
        try:
            self.cursor.execute(query, params)
            return self.cursor.fetchall()
        except mysql.connector.Error as err:
            print(f"SQL-error: {err}")

    # TASK 1
    def get_user_count(self):
        query = "SELECT COUNT(*) AS user_count FROM User;"
        return self.execute_query(query)[0][0]

    def get_activity_count(self):
        query = "SELECT COUNT(*) AS activity_count FROM Activity;"
        return self.execute_query(query)[0][0]

    def get_tp_count(self):
        query = "SELECT COUNT(*) AS tp_count FROM TrackPoint;"
        return self.execute_query(query)[0][0]

    def task_1(self):
        task_num = 1
        print_question(task_num=task_num,
                       question_text="How many users, activities and trackpoints are there in the dataset "
                                     "(after it is inserted into the database)?")

        result = {'Number of Users': [self.get_user_count()],
                  'Number of Activities': [self.get_activity_count()],
                  'Number of TrackPoints': [self.get_tp_count()]}

        print_result(result_df=result, filename=f"task_{task_num}")

    # TASK 2 - OK
    def get_avg_tp(self):
        query = '''SELECT 
                        (CAST(COUNT(tp.id) AS FLOAT) / COUNT(DISTINCT u.id)) AS avg_trackpoints_per_user
                    FROM 
                        User u 
                    LEFT JOIN 
                        Activity a ON u.id = a.user_id 
                    LEFT JOIN 
                        TrackPoint tp ON a.id = tp.activity_id;'''
        return self.execute_query(query)[0][0]

    def get_max_tp(self):
        query = '''SELECT MAX(tp_count) AS avg_tp_per_user
                        FROM (
                            SELECT u.id, SUM(tp_count) AS tp_count
                            FROM User u
                            JOIN (
                                SELECT activity_id, user_id, COUNT(*) AS tp_count
                                FROM Activity a
                                JOIN TrackPoint t ON a.id = t.activity_id
                                GROUP BY a.id, a.user_id
                            ) AS activity_tp
                            ON u.id = activity_tp.user_id
                            GROUP BY u.id
                        ) AS user_tp;'''

        return self.execute_query(query)[0][0]

    def get_min_tp(self):
        query = '''SELECT MIN(tp_count) AS avg_tp_per_user
                            FROM (
                                SELECT u.id, SUM(tp_count) AS tp_count
                                FROM User u
                                JOIN (
                                    SELECT activity_id, user_id, COUNT(*) AS tp_count
                                    FROM Activity a
                                    JOIN TrackPoint t ON a.id = t.activity_id
                                    GROUP BY a.id, a.user_id
                                ) AS activity_tp
                                ON u.id = activity_tp.user_id
                                GROUP BY u.id
                            ) AS user_tp;'''

        return self.execute_query(query)[0][0]

    def task_2(self):
        task_num = 2
        print_question(task_num=task_num,
                       question_text='Find the average, maximum and minimum number of trackpoints per user.')

        result = {'Average trackpoints per user': [self.get_avg_tp()],
                  'Maximum trackpoints per user': [self.get_max_tp()],
                  'Minimum trackpoints per user': [self.get_min_tp()]}

        print_result(result_df=result, floatfmt=".2f", filename=f"task_{task_num}")

    # TASK 3 - OK
    def get_top_15_activities(self):
        query = '''SELECT user.id,
                        COUNT(activity.id) AS number_of_activities
                    FROM User user
                    JOIN Activity activity
                    ON user.id = activity.user_id
                    GROUP BY user.id
                    ORDER BY number_of_activities DESC
                    LIMIT 15;'''
        return self.execute_query(query)

    def task_3(self):
        task_num = 3
        print_question(task_num=task_num, question_text='Find the top 15 users with the highest number of activities.')
        result = pd.DataFrame(self.get_top_15_activities(), columns=["User", 'Number of Activities'])
        print_result(result_df=result, filename=f"task_{task_num}")

    # TASK 4 - OK
    def get_transportation_by_bus(self):
        query = ''' SELECT DISTINCT user_id
                    FROM Activity
                    WHERE transportation_mode = 'bus';'''
        return self.execute_query(query)

    def task_4(self):
        task_num = 4
        print_question(task_num=task_num, question_text='Find all users who have taken a bus.')
        result = pd.DataFrame(self.get_transportation_by_bus(), columns=["User who have used a bus"])
        print_result(result, filename=f"task_{task_num}")

    # TASK 5 - OK
    def get_distinct_transportation_modes(self):
        query = '''SELECT user_id, COUNT(DISTINCT transportation_mode) AS transportation_modes
                    FROM Activity
                    GROUP BY user_id
                    ORDER BY transportation_modes DESC
                    LIMIT 10;'''
        return self.execute_query(query)

    def task_5(self):
        task_num = 5
        print_question(task_num=task_num,
                       question_text='List the top 10 users by their amount of different transportation modes.')
        result = pd.DataFrame(self.get_distinct_transportation_modes(), columns=["User", "Unique transportation modes"])
        print_result(result, filename=f"task_{task_num}")

    # TASK 6 - OK
    def get_duplicate_activities(self):
        query = '''SELECT user_id, a.id, COUNT(*) AS duplicates
                    FROM Activity a
                    GROUP BY user_id, a.id
                    HAVING COUNT(*) > 1;'''

        return self.execute_query(query)

    def task_6(self):
        task_num = 6
        print_question(task_num=task_num,
                       question_text='List the top 10 users by their amount of different transportation modes.')
        result = pd.DataFrame(self.get_distinct_transportation_modes(), columns=["User", "Unique transportation modes"])
        print_result(result, filename=f"task_{task_num}")

    # TASK 7a - OK
    def get_count_multiple_day_activities(self):
        query = '''SELECT COUNT(DISTINCT user_id) AS users_with_multiple_day_activities
                    FROM Activity
                    WHERE DATEDIFF(end_date_time, start_date_time) = 1;'''
        return self.execute_query(query)[0]

    # TASK 7b - OK
    def get_list_multiple_day_activities(self):
        # Retrieves all activities, including unlabeled transportation modes, that spans over one day
        query = '''SELECT user_id, id AS activity_id, transportation_mode, 
                          TIMESTAMPDIFF(MINUTE, start_date_time, end_date_time) AS duration_in_minutes
                    FROM Activity
                    WHERE DATEDIFF(end_date_time, start_date_time) = 1;'''
        return self.execute_query(query)

    def task_7(self):
        task_num = 7
        # a
        print_question(task_num=task_num,
                       question_text='a) Find the number of users that have started an activity in one day and ended '
                                     'the activity the next day.')

        result = {"Number of multi-day activity user": self.get_count_multiple_day_activities()}

        print_result(result, filename=f"task_{task_num}a")

        # b
        print_question(task_num=task_num,
                       question_text='b) List the transportation mode, user id and duration for these activities.')

        result = pd.DataFrame(self.get_list_multiple_day_activities(),
                              columns=['User', 'Activity ID', 'Transportation Mode', 'Activity duration'])

        print_result(result, filename=f'task_{task_num}b')

    # TASK 8
    def get_users_in_proximity(self):
        start_time = time.time()

        # 1. FILTER BY TIME
        query_time = '''
                    SELECT 
                        A1.id AS activity_id_1, 
                        A2.id AS activity_id_2, 
                        A1.user_id AS user_id_1, 
                        A2.user_id AS user_id_2
                    FROM Activity AS A1
                    JOIN Activity AS A2 ON 
                        A1.start_date_time <= A2.end_date_time + INTERVAL 30 SECOND
                        AND A1.end_date_time >= A2.start_date_time - INTERVAL 30 SECOND
                        AND A1.user_id != A2.user_id
                        AND A1.id < A2.id;
                    '''
        time_close_activities = self.execute_query(query_time)

        # 2. FETCH ALL TRACKPOINTS
        unique_activity_ids = set()
        for activity in time_close_activities:
            unique_activity_ids.add(activity[0])
            unique_activity_ids.add(activity[1])

        placeholders = ', '.join(['%s'] * len(unique_activity_ids))
        query_trackpoints = f"SELECT activity_id, lat, lon FROM TrackPoint WHERE activity_id IN ({placeholders});"
        all_trackpoints = self.execute_query(query_trackpoints, tuple(unique_activity_ids))

        # Organize trackpoints by activity id
        trackpoints_dict = {}
        for activity_id, lat, lon in all_trackpoints:
            if activity_id not in trackpoints_dict:
                trackpoints_dict[activity_id] = []
            trackpoints_dict[activity_id].append((lat, lon))

        # 3. SPATIAL FILTERING USING R-TREE
        def spatially_close(tp1_list, tp2_list):
            idx = index.Index()
            for pos, (lat, lon) in enumerate(tp1_list):
                idx.insert(pos, (lat, lon, lat, lon))

            for lat, lon in tp2_list:
                nearby = list(idx.intersection((lat - 0.0005, lon - 0.0005, lat + 0.0005, lon + 0.0005)))
                for nearby_id in nearby:
                    if haversine(tp1_list[nearby_id], (lat, lon), unit=Unit.METERS) <= 50:
                        return True
            return False

        # 4. FIND USERS IN PROXIMITY
        close_users_set = set()
        for activity_id_1, activity_id_2, user_id_1, user_id_2 in time_close_activities:
            if activity_id_1 in trackpoints_dict and activity_id_2 in trackpoints_dict:
                if spatially_close(trackpoints_dict[activity_id_1], trackpoints_dict[activity_id_2]):
                    close_users_set.add(user_id_1)
                    close_users_set.add(user_id_2)

        print(f'\rFinished. Time elapsed: {time_elapsed_str(start_time)}')
        return len(close_users_set)

    def task_8(self):
        task_num = 8
        print_question(task_num=task_num,
                       question_text='Find the number of users which have been close to each other in time and space.\n'
                                     'Close is defined as the same space (50 meters) and for the same half minute (30 '
                                     'seconds)')
        result = {"Users which have been close to another user": [self.get_users_in_proximity()]}
        print(result)
        print_result(result, filename=f"task_{task_num}")

    # TASK 9
    def get_top_altitude_gains(self):
        """
        Ensures differences between last trackpoint of previous activity and first track point of current activity is added.
        :return:
        """
        # Define the SQL query
        query = '''WITH CurrentAndPreviousAltitudes AS (
        SELECT activity.user_id,
               trackpoint.altitude AS current_altitude,
               LAG(trackpoint.altitude) OVER(
                   PARTITION BY trackpoint.activity_id ORDER BY trackpoint.date_time) AS previous_altitude
        FROM TrackPoint trackpoint
        JOIN Activity activity ON trackpoint.activity_id = activity.id
        WHERE trackpoint.altitude IS NOT NULL)   
    
        SELECT user_id AS id,
            ROUND(SUM(IF(current_altitude > previous_altitude, current_altitude - previous_altitude, 0)) * 0.3048)
                AS total_meters_gained
        FROM CurrentAndPreviousAltitudes
        WHERE previous_altitude IS NOT NULL
        GROUP BY user_id
        ORDER BY total_meters_gained DESC
        LIMIT 15;'''

        return self.execute_query(query, )

    def task_9(self):
        task_num = 9
        print_question(task_num=task_num,
                       question_text='Find the top 15 users who have gained the most altitude meters.\nOutput should '
                                     'be a table with (id, total meters gained per user). Remember that some '
                                     'altitude-values are invalid')
        result = pd.DataFrame(self.get_top_altitude_gains(), columns=['User', 'Altitude Gained'])
        print_result(result, filename=f"task_{task_num}")

    # TASK 10
    def get_longest_distance_per_transportation(self):
        query = '''
                SELECT a.user_id, a.transportation_mode, tp.lat, tp.lon, DATE(a.start_date_time) AS travel_day
                FROM Activity a
                JOIN TrackPoint tp ON a.id = tp.activity_id
                WHERE TIMESTAMPDIFF(DAY, a.start_date_time, a.end_date_time) <= 1
                ORDER BY a.user_id, a.transportation_mode, tp.date_time;
            '''
        data = self.execute_query(self.cursor, query)
        result = result_to_dicts(self.cursor, data)

        distances = []
        for i in range(1, len(result)):
            if (result[i]['user_id'] == result[i - 1]['user_id']
                    and result[i]['transportation_mode'] == result[i - 1]['transportation_mode']
                    and result[i]['travel_day'] == result[i - 1]['travel_day']):
                coord1 = (result[i - 1]['lat'], result[i - 1]['lon'])
                coord2 = (result[i]['lat'], result[i]['lon'])
                distance = haversine(coord1, coord2, unit='km')
                distances.append(
                    (result[i]['user_id'], result[i]['transportation_mode'], distance))

        # Aggregate distances by user and mode
        from collections import defaultdict
        aggregated_distances = defaultdict(float)
        for user, mode, distance in distances:
            aggregated_distances[(user, mode)] += distance

        # Find max distances by mode
        max_distances = {}
        for (user, mode), distance in aggregated_distances.items():
            if mode not in max_distances or max_distances[mode][1] < distance:
                max_distances[mode] = (user, distance)

        # Format the results
        output = []
        for mode, (user, distance) in max_distances.items():
            line = f"\nTransportation Mode: {mode}, User ID: {user}, Distance: {distance:.2f} km"
            output.append(line)

        return '\n'.join(output)

    # TASK 11
    def get_invalid_activities(self):
        query = """
        WITH TrackpointDifferences AS (
        SELECT trackpoint.activity_id,
               TIMESTAMPDIFF(MINUTE,
                             LAG(trackpoint.date_time) 
                                OVER(PARTITION BY trackpoint.activity_id ORDER BY trackpoint.date_time),
                             trackpoint.date_time) AS time_difference
        FROM TrackPoint trackpoint),
    
        InvalidActivity AS (
            SELECT activity_id
            FROM TrackpointDifferences
            WHERE time_difference >= 5
            GROUP BY activity_id
        )
    
        SELECT activity.user_id, COUNT(DISTINCT invalid_activity.activity_id) AS invalid_activity_count
        FROM InvalidActivity invalid_activity
        JOIN Activity activity ON invalid_activity.activity_id = activity.id
        GROUP BY activity.user_id
        ORDER BY activity.user_id;"""

        return self.execute_query(query)

    def task_11(self):
        task_num = 11
        print_question(task_num=task_num,
                       question_text="Find all users who have invalid activities, and the number of invalid activities "
                                     "per user.\nAn invalid activity is defined as an activity with consecutive "
                                     "trackpoints where the timestamps\ndeviate with at least 5 minutes.")
        result = pd.DataFrame(self.get_invalid_activities(), columns=['User ID', 'Invalid Activities'])
        print_result(result, filename=f"task_{task_num}")

    # TASK 12
    def get_most_used_transportations(self):
        query = '''
        WITH UserTransportModeCounts AS (
        SELECT user.id as user_id, activity.transportation_mode AS transportation_mode, COUNT(*) AS amount
        FROM User user
        JOIN Activity activity on user.id = activity.user_id
        WHERE user.has_labels IS NOT NULL AND activity.transportation_mode IS NOT NULL
        GROUP BY user.id, activity.transportation_mode
        ),
    
        RankedTransportationMode AS (
            SELECT user_id,
                   transportation_mode AS most_used_transportation_mode,
                   amount,
                   ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY amount DESC, transportation_mode) AS rn
            FROM UserTransportModeCounts
        )
        
        SELECT user_id, most_used_transportation_mode, amount
        FROM RankedTransportationMode
        WHERE rn = 1
        ORDER BY user_id;'''

        return self.execute_query(query)

    def task_12(self):
        task_num = 12
        print_question(task_num=task_num,
                       question_text="Find all users who have registered transportation_mode and their most used "
                                     "transportation_mode.")
        result = pd.DataFrame(self.get_most_used_transportations(),
                              columns=['User ID', 'Most Used Transportation Mode', 'Amount'])
        print_result(result, filename=f"task_{task_num}")
