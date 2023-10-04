import pandas as pd
import geopandas as gpd
import time
from tabulate import tabulate
from shapely.geometry import Point
from datetime import timedelta
from sqlalchemy import create_engine


def iterate_results(cursor):
    tasks = [
        {
            "description": "Task 1: How many users, activities and trackpoints are there in the dataset (after it is inserted into the database)?",
            "methods": [get_user_count, get_activity_count, get_tp_count],
            "labels": ['Total users:', 'Total activities:', 'Total trackpoints:']},

        {"description": "Task 2: Find the average, maximum and minimum number of trackpoints per user.",
         "methods": [get_avg_tp, get_max_tp, get_min_tp],
         "labels": ['Average trackpoints:', 'Maximum trackpoints', 'Minimum trackpoints:']},

        {"description": "Task 3: Find the top 15 users with the highest number of activities.",
         "methods": [get_top_15_activities],
         "labels": ['Highly active users:']},

        {"description": "Task 4: Find all users who have taken a bus.",
         "methods": [get_transportation_by_bus],
         "labels": ['Bus-taking users:']},

        {"description": "Task 5: List the top 10 users by their amount of different transportation modes.",
         "methods": [get_distinct_transportation_modes],
         "labels": ['Users by number of transportation modes:']},

        {
            "description": "Task 6: Find activities that are registered multiple times. You should find the query even if it gives zero result.",
            "methods": [get_duplicate_activities],
            "labels": ['Duplicate activities:']},

        {
            "description": "Task 7a: Find the number of users that have started an activity in one day and ended the activity the next day.",
            "methods": [get_count_multiple_day_activities],
            "labels": ['Users with activities spanning multiple days:']},

        {"description": "Task 7b: List the transportation mode, user id and duration for these activities.",
         "methods": [get_list_multiple_day_activities],
         "labels": ['Details on activities spanning multiple days:']},

        {
            "description": "Task 8: Find the number of users which have been close to each other in time and space. Close is defined as the same space (50 meters) and for the same half minute (30 seconds)",
            "methods": [get_users_in_proximity],
            "labels": ['Number of users in close proximity:']},

        {
            "description": "Task 9: Find the top 15 users who have gained the most altitude meters. Output should be a table with (id, total meters gained per user). Remember that some altitude-values are invalid",
            "methods": [get_top_altitude_gains],
            "labels": ['Height-climbing users:']},

        {
            "description": "Task 10: Find the users that have traveled the longest total distance in one day for each transportation mode.",
            "methods": [longest_distance_per_transportation],
            "labels": ['Height-climbing users:']},

        {
            "description": "Task 11: Find all users who have invalid activities, and the number of invalid activities per user. An invalid activity is defined as an activity with consecutive trackpoints where the timestamps deviate with at least 5 minutes.",
            "methods": [get_invalid_activities],
            "labels": ['Invalid activities per user:']},

        {
            "description": "Task 12:  Find all users who have registered transportation_mode and their most used transportation_mode.",
            "methods": [get_most_used_transportations],
            "labels": ['Users with theirs most used transportation modes:']}
    ]

    for task in tasks:
        print(f"{task['description']}\n Answer:")
        for method in task['methods']:
            result = method(cursor)
            print(f"  {label}: {result}")
        print("\n")  # Add a newline between different tasks for better readability.


def execute_query(cursor, query):
    cursor.execute(query)
    result = cursor.fetchone()
    return result[0] if result else 0


def execute_and_printall(cursor, query: str, columns: list[str]):
    cursor.execute(query)
    result = cursor.fetchall()
    df = pd.DataFrame(result, columns=columns)
    print(tabulate(df, headers='keys', tablefmt='grid', showindex=False))


# TASK 1
def get_user_count(cursor):
    query = "SELECT COUNT(*) AS user_count FROM User;"
    result = execute_query(cursor, query)
    return result


def get_activity_count(cursor):
    query = "SELECT COUNT(*) AS activity_count FROM Activity;"
    return execute_query(cursor, query)


def get_tp_count(cursor):
    query = "SELECT COUNT(*) AS tp_count FROM TrackPoint;"
    return execute_query(cursor, query)


# TASK 2
def get_avg_tp(cursor):
    query = '''SELECT AVG(tp_count) AS avg_tp_per_user
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


# TASK 3
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


# TASK 4
def get_transportation_by_bus(cursor):
    query = ''' SELECT DISTINCT user_id
                FROM Activity
                WHERE transportation_mode = 'bus';'''
    return execute_query(cursor, query)


# TASK 5
def get_distinct_transportation_modes(cursor):
    query = '''SELECT user_id, COUNT(DISTINCT transportation_mode) AS transportation_modes
                FROM Activity
                GROUP BY user_id
                ORDER BY transportation_modes DESC
                LIMIT 10;'''
    return execute_query(cursor, query)


# TASK 6
def get_duplicate_activities(cursor):
    query = '''SELECT user_id, a.id, COUNT(*) AS duplicates
                FROM Activity a
                GROUP BY user_id, a.id
                HAVING COUNT(*) > 1;'''

    return execute_query(cursor, query)


# TASK 7a
def get_count_multiple_day_activities(cursor):
    query = '''SELECT COUNT(DISTINCT user_id) AS users_with_multiple_day_activities
                FROM Activity
                WHERE DATEDIFF(end_date_time, start_date_time) = 1;'''
    return execute_query(cursor, query)


# TASK 7b
def get_list_multiple_day_activities(cursor):
    query = '''SELECT user_id, transportation_mode, TIMESTAMPDIFF(MINUTE, start_date_time, end_date_time) AS duration_in_minutes
                FROM Activity
                WHERE DATEDIFF(end_date_time, start_date_time) = 1;'''
    return execute_query(cursor, query)


# TASK 8
def get_users_in_proximity(cursor):
    ### ----- MÅ ENDRES -----
    # Load Activities and Trackpoints data
    activities_query = "SELECT * FROM Activity;"
    trackpoints_query = "SELECT * FROM TrackPoint;"

    activities = pd.read_sql(execute_query(cursor, activities_query))
    trackpoints = pd.read_sql(execute_query(cursor, trackpoints_query))

    # Convert Trackpoints to GeoDataFrame
    geometry = [Point(xy) for xy in zip(trackpoints['lon'], trackpoints['lat'])]
    trackpoints_gdf = gpd.GeoDataFrame(trackpoints, geometry=geometry)

    # Empty list to hold results
    results = []

    for index, act in activities.iterrows():
        # Filter Activities that are within 30 seconds of the current activity and different user_id
        time_filter = (
                ((activities['start_date_time'] >= (act['start_date_time'] - timedelta(seconds=30))) &
                 (activities['start_date_time'] <= (act['end_date_time'] + timedelta(seconds=30)))) |
                ((activities['end_date_time'] >= (act['start_date_time'] - timedelta(seconds=30))) &
                 (activities['end_date_time'] <= (act['end_date_time'] + timedelta(seconds=30))))
        )
        user_filter = activities['user_id'] != act['user_id']

        # Get the trackpoints for the current activity
        current_trackpoints = trackpoints_gdf[trackpoints_gdf['activity_id'] == act['id']]

        for _, other_act in activities[time_filter & user_filter].iterrows():
            # Get the trackpoints for the other activity
            other_trackpoints = trackpoints_gdf[trackpoints_gdf['activity_id'] == other_act['id']]

            for _, tp1 in current_trackpoints.iterrows():
                for _, tp2 in other_trackpoints.iterrows():
                    # If the distance is less than 0.05 km (50 meters) and user_id is different, save the result
                    if tp1.geometry.distance(tp2.geometry) < 0.05 and act['user_id'] < other_act['user_id']:
                        results.append({'user1': act['user_id'], 'user2': other_act['user_id']})

    # Convert the results to a DataFrame and count occurrences
    result_df = pd.DataFrame(results)
    result_count = result_df.groupby(['user1', 'user2']).size().reset_index(name='close_encounters')

    return result_count


# TASK 9
def print_top_altitude_gains(cursor):
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
def longest_distance_per_transportation(cursor):
    # Define the SQL query to fetch required fields
    query = '''SELECT a.user_id, a.transportation_mode, tp.lat, tp.lon
                FROM Activity a
                JOIN TrackPoint tp ON a.id = tp.activity_id;'''

    # Read the SQL query into a DataFrame
    df = pd.read_sql(execute_query(cursor, query))

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

    # Read the SQL query into a DataFrame
    df = pd.read_sql(execute_query(cursor, query))

    # Convert date_time to datetime object
    df['date_time'] = pd.to_datetime(df['date_time'])

    # Function to find invalid activities
    def find_invalid_activities(group):
        group['invalid'] = group['date_time'].diff() >= timedelta(minutes=5)
        return group

    # Apply the function to find invalid activities
    df = df.groupby('activity_id').apply(find_invalid_activities)

    # Filter invalid activities and count them by user_id
    invalid_activities_count = df[df['invalid']].groupby('user_id')['activity_id'].nunique().reset_index(
        name='invalid_activity_count')

    # Display the result
    return print(invalid_activities_count)


def print_invalid_activities(cursor):
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
def print_most_used_transportations(cursor):
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
