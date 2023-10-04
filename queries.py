import pandas as pd
import geopandas as gpd
import time
from tabulate import tabulate
from shapely.geometry import Point
from datetime import timedelta
from sqlalchemy import create_engine
from haversine import haversine, Unit
import time
from execute import time_elapsed_str
from rtree import index


def iterate_results(cursor, task_num=None):
    tasks = [
        {
            "description": "Task 1: How many users, activities and trackpoints are there in the dataset (after it is "
                           "inserted into the database)?",
            "methods": [get_user_count, get_activity_count, get_tp_count],
            "labels": ['Total users', 'Total activities', 'Total trackpoints']},
        {
            "description": "Task 2: Find the average, maximum and minimum number of trackpoints per user.",
            "methods": [get_avg_tp, get_max_tp, get_min_tp],
            "labels": ['Average trackpoints', 'Maximum trackpoints', 'Minimum trackpoint:']},

        {
            "description": "Task 3: Find the top 15 users with the highest number of activities.",
            "methods": [get_top_15_activities],
            "labels": ['Highly active user']},

        {
            "description": "Task 4: Find all users who have taken a bus.",
            "methods": [get_transportation_by_bus],
            "labels": ['Bus-taking user']},

        {
            "description": "Task 5: List the top 10 users by their amount of different transportation modes.",
            "methods": [get_distinct_transportation_modes],
            "labels": ['Users by number of transportation modes']},

        {
            "description": "Task 6: Find activities that are registered multiple times. You should find the query "
                           "even if it gives zero result.",
            "methods": [get_duplicate_activities],
            "labels": ['Duplicate activities']},

        {
            "description": "Task 7a: Find the number of users that have started an activity in one day and ended the "
                           "activity the next day.",
            "methods": [get_count_multiple_day_activities],
            "labels": ['Users with activities spanning multiple days']},

        {  # -- task 8
            "description": "Task 7b: List the transportation mode, user id and duration for these activities.",
            "methods": [get_list_multiple_day_activities],
            "labels": ['Multiple-day activity']},

        {
            "description": "Task 8: Find the number of users which have been close to each other in time and space. "
                           "Close is defined as the same space (50 meters) and for the same half minute (30 seconds)",
            "methods": [get_users_in_proximity],
            "labels": ['Number of users in close proximity']},

        {
            "description": "Task 9: Find the top 15 users who have gained the most altitude meters. Output should be "
                           "a table with (id, total meters gained per user). Remember that some altitude-values are "
                           "invalid",
            "methods": [get_top_altitude_gains],
            "labels": ['Height-climbing users']},

        {
            "description": "Task 10: Find the users that have traveled the longest total distance in one day for each "
                           "transportation mode.",
            "methods": [get_longest_distance_per_transportation],
            "labels": ['Users with longest distance per transportation mode']},

        {
            "description": "Task 11: Find all users who have invalid activities, and the number of invalid activities "
                           "per user. An invalid activity is defined as an activity with consecutive trackpoints "
                           "where the timestamps deviate with at least 5 minutes.",
            "methods": [get_invalid_activities],
            "labels": ['Invalid activities per user']},

        {
            "description": "Task 12:  Find all users who have registered transportation_mode and their most used "
                           "transportation_mode.",
            "methods": [get_most_used_transportations],
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
            result = method(cursor)
            format_and_print(label, result)
        print("\n")  # Add a newline between different tasks for better readability.


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


def execute_query(cursor, query, params=None):
    try:
        cursor.execute(query, params)
        return cursor.fetchall()
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return None


if __name__ == "__main__":
    execute()


def execute_and_printall(cursor, query: str, columns: list[str]):
    cursor.execute(query)
    result = cursor.fetchall()
    df = pd.DataFrame(result, columns=columns)
    print(tabulate(df, headers='keys', tablefmt='grid', showindex=False))


# TASK 1
def get_user_count(cursor):
    query = "SELECT COUNT(*) AS user_count FROM User;"
    return execute_query(cursor, query)


def get_activity_count(cursor):
    query = "SELECT COUNT(*) AS activity_count FROM Activity;"
    return execute_query(cursor, query)


def get_tp_count(cursor):
    query = "SELECT COUNT(*) AS tp_count FROM TrackPoint;"
    return execute_query(cursor, query)


# TASK 2 - OK
def get_avg_tp(cursor):
    query = '''SELECT 
                    (CAST(COUNT(tp.id) AS FLOAT) / COUNT(DISTINCT u.id)) AS avg_trackpoints_per_user
                FROM 
                    User u 
                LEFT JOIN 
                    Activity a ON u.id = a.user_id 
                LEFT JOIN 
                    TrackPoint tp ON a.id = tp.activity_id;'''
    return execute_query(cursor, query)


def get_max_tp(cursor):
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

    return execute_query(cursor, query)


def get_min_tip(cursor):
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

    return execute_query(cursor, query)


# TASK 3 - OK
def get_top_15_activities(cursor):
    query = '''SELECT u.id,
                    COUNT(a.id) AS number_of_activities
                FROM User u
                JOIN Activity a
                ON u.id = a.user_id
                GROUP BY u.id
                ORDER BY number_of_activities DESC
                LIMIT 15;'''
    return execute_query(cursor, query)


# TASK 4 - OK
def get_transportation_by_bus(cursor):
    query = ''' SELECT DISTINCT user_id
                FROM Activity
                WHERE transportation_mode = 'bus';'''
    return execute_query(cursor, query)


# TASK 5 - OK
def get_distinct_transportation_modes(cursor):
    query = '''SELECT user_id, COUNT(DISTINCT transportation_mode) AS transportation_modes
                FROM Activity
                GROUP BY user_id
                ORDER BY transportation_modes DESC
                LIMIT 10;'''
    return execute_query(cursor, query)


# TASK 6 - OK
def get_duplicate_activities(cursor):
    query = '''SELECT user_id, a.id, COUNT(*) AS duplicates
                FROM Activity a
                GROUP BY user_id, a.id
                HAVING COUNT(*) > 1;'''

    return execute_query(cursor, query)


# TASK 7a - OK
def get_count_multiple_day_activities(cursor):
    query = '''SELECT COUNT(DISTINCT user_id) AS users_with_multiple_day_activities
                FROM Activity
                WHERE DATEDIFF(end_date_time, start_date_time) = 1;'''
    return execute_query(cursor, query)


# TASK 7b - OK
def get_list_multiple_day_activities(cursor):
    # Retrieves all activities, including unlabeled transportation modes, that spans over one day
    query = '''SELECT user_id, transportation_mode, TIMESTAMPDIFF(MINUTE, start_date_time, end_date_time) AS duration_in_minutes
                FROM Activity
                WHERE DATEDIFF(end_date_time, start_date_time) = 1;'''
    return execute_query(cursor, query)


# TASK 8
def get_users_in_proximity(cursor):
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
    time_close_activities = execute_query(cursor, query_time)

    # 2. FETCH ALL TRACKPOINTS
    unique_activity_ids = set()
    for activity in time_close_activities:
        unique_activity_ids.add(activity[0])
        unique_activity_ids.add(activity[1])

    placeholders = ', '.join(['%s'] * len(unique_activity_ids))
    query_trackpoints = f"SELECT activity_id, lat, lon FROM TrackPoint WHERE activity_id IN ({placeholders});"
    all_trackpoints = execute_query(cursor, query_trackpoints, tuple(unique_activity_ids))

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

    print(f'\rFinished. Time elapsed: {time_elapsed_str(start_time)}', end='')
    return len(close_users_set)


# TASK 9
def get_top_altitude_gains(cursor):
    """
    Ensures differences between last trackpoint of previous activity and first track point of current activity is added.
    :param cursor:
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

    execute_and_printall(cursor, query, columns=['User_id', 'Altitude Gained'])


# TASK 10
def get_longest_distance_per_transportation(cursor):
    # Define the SQL query to fetch required fields
    query = '''SELECT a.user_id, a.transportation_mode, tp.lat, tp.lon
                FROM Activity a
                JOIN TrackPoint tp ON a.id = tp.activity_id;'''

    # Read the SQL query into a DataFrame
    result = execute_query(cursor, query)

    df = pd.DataFrame(result, columns=['user_id', 'transportation_mode', 'latitude', 'longitude'])

    # Sort values by user_id, transportation_mode, date_days and latitude and longitude (assumed to be in that order)
    df.sort_values(by=['user_id', 'transportation_mode', 'latitude', 'longitude'], inplace=True)

    # Function to calculate total distance for each group
    def calculate_distance(group):
        total_distance = 0
        for i in range(1, len(group)):
            coord1 = (group.iloc[i - 1]['latitude'], group.iloc[i - 1]['longitude'])
            coord2 = (group.iloc[i]['latitude'], group.iloc[i]['longitude'])
            total_distance += haversine(coord1, coord2, unit=Unit.METERS)
        return total_distance

    # Group by user_id, transportation_mode, and date_days and apply the distance function
    df['total_distance'] = df.groupby(['user_id', 'transportation_mode']).apply(calculate_distance).reset_index(
        level=[0, 1, 2], drop=True)

    # Drop duplicates
    df.drop_duplicates(subset=['user_id', 'transportation_mode'], inplace=True)

    # For each transportation mode and date, find the user with the maximum total distance
    result_df = df.loc[df.groupby(['transportation_mode'])['total_distance'].idxmax()]

    # Select required columns and sort
    result_df = result_df[['user_id', 'transportation_mode', 'total_distance']].sort_values(
        by=['transportation_mode', 'date_days', 'total_distance'], ascending=[True, True, False])

    # Display the result
    return print(result_df)


# TASK 11
def get_invalid_activities(cursor):
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

    execute_and_printall(cursor, query, columns=['User ID', 'Invalid Activities'])


# TASK 12
def get_most_used_transportations(cursor):
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

    execute_and_printall(cursor, query, columns=['User ID', 'Most Used Transportation Mode', 'Amount'])
