import pandas as pd
import geopandas as gpd
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
            "description": "Task 1: How many users, activities and trackpoints are there in the dataset (after it is inserted into the database)?",
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
            "description": "Task 6: Find activities that are registered multiple times. You should find the query even if it gives zero result.",
            "methods": [get_duplicate_activities],
            "labels": ['Duplicate activities']},

        {
            "description": "Task 7a: Find the number of users that have started an activity in one day and ended the activity the next day.",
            "methods": [get_count_multiple_day_activities],
            "labels": ['Users with activities spanning multiple days']},

        {  # -- task 8
            "description": "Task 7b: List the transportation mode, user id and duration for these activities.",
            "methods": [get_list_multiple_day_activities],
            "labels": ['Multiple-day activity']},

        {
            "description": "Task 8: Find the number of users which have been close to each other in time and space. Close is defined as the same space (50 meters) and for the same half minute (30 seconds)",
            "methods": [get_users_in_proximity],
            "labels": ['Number of users in close proximity']},

        {
            "description": "Task 9: Find the top 15 users who have gained the most altitude meters. Output should be a table with (id, total meters gained per user). Remember that some altitude-values are invalid",
            "methods": [get_top_altitude_gains],
            "labels": ['Height-climbing users']},

        {
            "description": "Task 10: Find the users that have traveled the longest total distance in one day for each transportation mode.",
            "methods": [longest_distance_per_transportation],
            "labels": ['Users with longest distance per transportation mode']},

        {
            "description": "Task 11: Find all users who have invalid activities, and the number of invalid activities per user. An invalid activity is defined as an activity with consecutive trackpoints where the timestamps deviate with at least 5 minutes.",
            "methods": [get_invalid_activities],
            "labels": ['Invalid activities per user']},

        {
            "description": "Task 12:  Find all users who have registered transportation_mode and their most used transportation_mode.",
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


def get_min_tp(cursor):
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
                WHERE DATEDIFF(end_date_time, start_date_time) = 1
                ;'''
    result = execute_query(cursor, query)
    return len(result)


# task 8 v2


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


"""
# TASK 8

def get_users_in_proximity(cursor):
    start_time = time.time()
    def filter_by_time(cursor):
        # Retrieve activities within 30 seconds of eachother between different users.
        query = '''
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

        # This list contains pairs of activities that are within 30 seconds of eachother
        return execute_query(cursor, query)

    def filter_by_spatial_distance(trackpoints_1, trackpoints_2):
        for tp1 in trackpoints_1:
            for tp2 in trackpoints_2:
                spatial_distance = haversine(tp1[:2], tp2[:2], unit=Unit.METERS)
                if spatial_distance <= 50:
                    return True
        return False

    print("Starting filter_by_time...")
    time_close_activities = filter_by_time(cursor)
    print(f"Found {len(time_close_activities)} time-close activities")
    # Cache track points
    trackpoints_cache = {}

    print("Starting spatial filtering...")

    def get_trackpoints(activity_id):
        if activity_id not in trackpoints_cache:
            query = f"SELECT lat, lon FROM TrackPoint WHERE activity_id = {activity_id};"
            trackpoints_cache[activity_id] = execute_query(cursor, query)
        return trackpoints_cache[activity_id]

    # Only unique users
    close_users_set = set()

    # Filter based on spatial proximity
    for activity_pair in time_close_activities:
        activity_id_1 = activity_pair[0]
        activity_id_2 = activity_pair[1]
        user_id_1 = activity_pair[2]
        user_id_2 = activity_pair[3]

        trackpoints_1 = get_trackpoints(activity_id_1)
        trackpoints_2 = get_trackpoints(activity_id_2)

        if filter_by_spatial_distance(trackpoints_1, trackpoints_2):
            close_users_set.add(user_id_1)
            close_users_set.add(user_id_2)
    # Return the count of unique users
    print(f'\rFinished. Time elapsed: {time_elapsed_str(start_time)}',
          end='')

    return len(close_users_set)
"""


# TASK 9
def get_top_altitude_gains(cursor):
    # Define the SQL query
    query = '''SELECT a.user_id, tp.altitude
                    FROM TrackPoint tp
                    JOIN Activity a ON tp.activity_id = a.id;'''

    # Fetch the result into a list of tuples
    result = cursor.fetchall(execute_query(cursor, query))

    # Convert the result to a DataFrame
    df = pd.DataFrame(result, columns=['user_id', 'altitude'])

    # Convert altitude from feet to meters
    df['altitude'] = df['altitude'] * 0.3048  # 1 foot = 0.3048 meters

    # Filter out invalid altitude values
    df = df[df['altitude'] != -777 * 0.3048]  # Convert -777 feet to meters

    # Calculate the altitude gained for each row
    df['altitude_gained'] = df.groupby('user_id')['altitude'].diff().clip(lower=0)

    # Sum the altitude gained for each user
    result_df = df.groupby('user_id')['altitude_gained'].sum().reset_index()

    # Rename columns
    result_df.columns = ['id', 'total_meters_gained_per_user']

    # Get the top 15 users who have gained the most altitude meters
    top_15_users = result_df.sort_values(by='total_meters_gained_per_user', ascending=False).head(15)

    # Use tabulate to print the table

    return print(tabulate(top_15_users, headers='keys', tablefmt='grid', showindex=False))


# TASK 10
def longest_distance_per_transportation(cursor):
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
    # Define the SQL query to fetch required fields
    query = '''SELECT a.user_id, tp.activity_id,tp.date_time
                FROM Activity a
                JOIN TrackPoint tp ON a.id = tp.activity_id
                ORDER BY a.user_id, tp.activity_id, tp.date_time;'''

    # Read the SQL query
    result = execute_query(cursor, query)

    # Convert date_time to datetime object
    df = pd.DataFrame(result, columns=['user_id', 'activity_id', 'date_time'])

    # Function to find invalid activities
    def find_invalid_activities(group):
        group['invalid'] = group['date_time'].diff() >= timedelta(minutes=5)
        return group

    # Apply the function to find invalid activities
    df = df.groupby('activity_id').apply(find_invalid_activities)

    # Filter invalid activities and count them by user_id
    invalid_activities_count = df[df['invalid']].groupby('user_id')['activity_id'].nunique().reset_index(
        name='invalid_activity_count')

    # Every trackpoint row is compared with the row behind it using LAG on data_time value
    # This query takes therefor a long time to execute
    query = """
                SELECT user_id, COUNT(DISTINCT activity_id) AS fault_activity_amount
                FROM(
                    SELECT MINUTE(TIMEDIFF(startTime, prev_time)) AS time_diff, user_id, activity_id, prev_a_id
                        FROM(
                            SELECT t1.date_time AS startTime,
                                LAG(t1.date_time) OVER(ORDER BY date_time) AS prev_time,
                                user_id, activity_id,
                                LAG(t1.activity_id) OVER(ORDER BY date_time) AS prev_a_id
                            FROM TrackPoint t1
                            INNER JOIN Activity ON Activity.id = t1.activity_id
                        ) AS time_table
                    ) AS diff_table
                WHERE time_diff > 5
                AND activity_id = prev_a_id
                GROUP BY user_id
                ORDER BY user_id ASC
            """

    res = self.execute_query(query)
    print("user_id  invalid_activities\n")
    for row in res:
        print("{} {:>23}".format(row[0], row[1]))

    # Display the result
    return print(invalid_activities_count)


# TASK 12
def get_most_used_transportations(cursor):
    ### By using the CASE statement in the ORDER BY clause,
    ### we can establish a priority. In the event of a tie in the number of activities (mode_count), the CASE statement ensures that transportation modes are prioritized in the defined order.

    # Denne gir ikke alle brukere med has_labels = true,
    # men må få testet opp mot DB med verdier i transportation_mode for å finne ut av hvor feilen ligger
    query_1 = '''SELECT user_id,transportation_mode
                FROM (
                    SELECT user_id,
                           transportation_mode,
                           ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY mode_count DESC, 
                                             CASE transportation_mode
                                                  WHEN 'Walk' THEN 1
                                                  WHEN 'Bike' THEN 2
                                                  WHEN 'Bus' THEN 3
                                                  WHEN 'Car & taxi' THEN 4
                                                  WHEN 'Train' THEN 5
                                                  WHEN 'Airplane' THEN 6
                                                  WHEN 'Other' THEN 7
                                                  ELSE 8
                                             END ASC) as ranking
                    FROM (
                        SELECT a.user_id, 
                               a.transportation_mode, 
                               COUNT(*) as mode_count
                        FROM Activity a
                        JOIN User u ON a.user_id = u.id
                        WHERE transportation_mode IS NOT NULL
                          AND u.has_labels = TRUE
                        GROUP BY a.user_id, a.transportation_mode
                    ) as mode_counts
                ) as ranked_modes
                WHERE ranking = 1
                ORDER BY user_id;'''

    #### ---- evt denne:
    """
    query_2 = '''SELECT u.id as user_id,
                       COALESCE(ranked_modes.transportation_mode, 'No Activity') as transportation_mode
                FROM User u
                LEFT JOIN (
                    SELECT user_id,
                           transportation_mode
                    FROM (
                        SELECT user_id,
                               transportation_mode,
                               ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY mode_count DESC, 
                                                 CASE transportation_mode
                                                      WHEN 'Walk' THEN 1
                                                      WHEN 'Bike' THEN 2
                                                      WHEN 'Bus' THEN 3
                                                      WHEN 'Car & taxi' THEN 4
                                                      WHEN 'Train' THEN 5
                                                      WHEN 'Airplane' THEN 6
                                                      WHEN 'Other' THEN 7
                                                      ELSE 8
                                                 END ASC) as ranking
                        FROM (
                            SELECT a.user_id, 
                                   a.transportation_mode, 
                                   COUNT(*) as mode_count
                            FROM Activity a
                            WHERE a.transportation_mode IS NOT NULL
                            GROUP BY a.user_id, a.transportation_mode
                        ) as mode_counts
                    ) as ranked_modes
                    WHERE ranking = 1
                ) as ranked_modes ON u.id = ranked_modes.user_id
                WHERE u.has_labels = true
                ORDER BY u.id;'''
    """

    return execute_query(cursor, query_1)
