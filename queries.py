import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from datetime import timedelta
from sqlalchemy import create_engine


def execute_query(cursor, query, label):
    cursor.execute(query)
    result = cursor.fetchone()
    return label[0] if result else 0

def get_user_count(cursor):
    query = "SELECT COUNT(*) AS user_count FROM User;"
    return execute_query(cursor, query, 'user_count')

def get_activity_count(cursor):
    query ="SELECT COUNT(*) AS activity_count FROM Activity;"
    return execute_query(cursor, query, 'activity_count')

def get_tp_count(cursor):
    query ="SELECT COUNT(*) AS tp_count FROM TrackPoint;"
    return execute_query(cursor, query, 'tp_count')

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

    return execute_query(cursor, query, 'avg_tp')

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

    return execute_query(cursor, query, 'max_tp')

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

    return execute_query(cursor, query, 'min_tp')

def get_top_15_activities(cursor):
    query = '''SELECT u.id,
                    COUNT(a.id) AS number_of_activities
                FROM User u
                JOIN Activity a
                ON u.id = a.user_id
                GROUP BY u.id
                ORDER BY number_of_activities DESC
                LIMIT 15;'''
    return execute_query(cursor, query, 'top_15_activities')

def get_transportation_by_bus(cursor):
    query = ''' SELECT DISTINCT user_id
                FROM Activity
                WHERE transportation_mode = 'bus';'''
    return execute_query(cursor, query, 'transportation_by_bus')

def get_distinct_transportation_modes(cursor):
    query = '''SELECT user_id, COUNT(DISTINCT transportation_mode) AS transportation_modes
                FROM Activity
                GROUP BY user_id
                ORDER BY transportation_modes DESC
                LIMIT 10;'''
    return execute_query(cursor, query, 'distinct_transportation_modes')


def get_duplicate_activities(cursor):
    query = '''SELECT user_id, a.id, COUNT(*) AS duplicates
                FROM Activity a
                GROUP BY user_id, a.id
                HAVING COUNT(*) > 1;'''

    return execute_query(cursor, query, 'distinct_duplicate_activities')


def get_count_multiple_day_activities(cursor):
    query = '''SELECT COUNT(DISTINCT user_id) AS users_with_multiple_day_activities
                FROM Activity
                WHERE DATEDIFF(end_date_time, start_date_time) = 1;'''
    return execute_query(cursor, query, 'count_multiple_day_activities')

def get_list_multiple_day_activities(cursor):
    query = '''SELECT user_id, transportation_mode, TIMESTAMPDIFF(MINUTE, start_date_time, end_date_time) AS duration_in_minutes
                FROM Activity
                WHERE DATEDIFF(end_date_time, start_date_time) = 1;'''
    return execute_query(cursor, query, 'list_multiple_day_activities')

def get_users_in_proximity(cursor):

    ### ----- MÅ ENDRES -----
    # Load Activities and Trackpoints data
    activities_query = "SELECT * FROM Activity"
    trackpoints_query = "SELECT * FROM TrackPoint"

    activities = pd.read_sql(execute_query(cursor, activities_query, 'all_activities'))
    trackpoints = pd.read_sql(execute_query(cursor, trackpoints_query, 'all_trackpoints'))

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

def get_top_altitude_gains(cursor):
    # Define the SQL query
    query = '''SELECT a.user_id, tp.altitude
                FROM TrackPoint tp
                JOIN Activity a ON tp.activity_id = a.id;
    '''

    # Read the SQL query into a DataFrame
    df = pd.read_sql(execute_query(cursor, query, 'altitudes_per_user'))

    # Filter out invalid altitude values
    df = df[df['altitude'] != -777]

    # Calculate the altitude gained for each row
    df['altitude_gained'] = df.groupby('user_id')['altitude'].diff().clip(lower=0)

    # Sum the altitude gained for each user
    result_df = df.groupby('user_id')['altitude_gained'].sum().reset_index()

    # Rename columns
    result_df.columns = ['id', 'total_meters_gained_per_user']

    # Get the top 15 users who have gained the most altitude meters
    top_15_users = result_df.sort_values(by='total_meters_gained_per_user', ascending=False).head(15)

    # Display the result
    return print(top_15_users.to_string(index=False))

def longest_distance_per_transportation(cursor):

    # Define the SQL query to fetch required fields
    query = '''SELECT a.user_id, a.transportation_mode, tp.lat, tp.lon
                FROM Activity a
                JOIN TrackPoint tp ON a.id = tp.activity_id;'''

    # Read the SQL query into a DataFrame
    df = pd.read_sql(execute_query(cursor, query, 'distances_by_transport'))

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
    df['total_distance'] = df.groupby(['user_id', 'transportation_mode']).apply(calculate_distance).reset_index(level=[0,1,2], drop=True)

    # Drop duplicates
    df.drop_duplicates(subset=['user_id', 'transportation_mode'], inplace=True)

    # For each transportation mode and date, find the user with the maximum total distance
    result_df = df.loc[df.groupby(['transportation_mode'])['total_distance'].idxmax()]

    # Select required columns and sort
    result_df = result_df[['user_id', 'transportation_mode','total_distance']].sort_values(by=['transportation_mode', 'date_days', 'total_distance'], ascending=[True, True, False])

    # Display the result
    return print(result_df)

def get_invalid_activities(cursor):
    # Define the SQL query to fetch required fields
    query = '''SELECT a.user_id, tp.activity_id,tp.date_time
                FROM Activity a
                JOIN TrackPoint tp ON a.id = tp.activity_id
                ORDER BY a.user_id, tp.activity_id, tp.date_time;'''

    # Read the SQL query into a DataFrame
    df = pd.read_sql(execute_query(cursor, query, 'activity_deviations'))

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

    return execute_query(cursor, query_1, 'most_used_transportations')
