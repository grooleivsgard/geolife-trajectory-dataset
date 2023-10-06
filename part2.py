import numpy as np
import pandas as pd
import time
import mysql
from tabulate import tabulate
from haversine import haversine, Unit
from helpers import time_elapsed_str
from rtree import index
from Database import Database


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

    def execute_tasks(self, task_nums: int or range[int] or list[int]):
        """
            Executes specified tasks based on provided task numbers.

            :param task_nums: An integer, range, or list of integers representing the task numbers
                              to be executed.
            """
        tasks = [self.task_1, self.task_2, self.task_3, self.task_4, self.task_5, self.task_6, self.task_7,
                 self.task_8, self.task_9, self.task_10, self.task_11, self.task_12]

        if isinstance(task_nums, int):
            task_nums = [task_nums]

        for num in task_nums:
            tasks[num - 1]()

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
                        (CAST(COUNT(TrackPoint.id) AS FLOAT) / COUNT(DISTINCT User.id)) AS avg_trackpoints_per_user
                    FROM 
                        User 
                    LEFT JOIN 
                        Activity ON User.id = Activity.user_id 
                    LEFT JOIN 
                        TrackPoint ON Activity.id = TrackPoint.activity_id;'''
        return self.execute_query(query)[0][0]

    def get_max_tp(self):
        query = '''SELECT MAX(tp_count) AS avg_tp_per_user
                        FROM (
                            SELECT User.id, SUM(tp_count) AS tp_count
                            FROM User
                            JOIN (
                                SELECT activity_id, user_id, COUNT(*) AS tp_count
                                FROM Activity
                                JOIN TrackPoint ON Activity.id = TrackPoint.activity_id
                                GROUP BY Activity.id, Activity.user_id
                            ) AS activity_tp
                            ON User.id = activity_tp.user_id
                            GROUP BY User.id
                        ) AS user_tp;'''

        return self.execute_query(query)[0][0]

    def get_min_tp(self):
        query = '''SELECT MIN(tp_count) AS avg_tp_per_user
                            FROM (
                                SELECT User.id, SUM(tp_count) AS tp_count
                                FROM User
                                JOIN (
                                    SELECT activity_id, user_id, COUNT(*) AS tp_count
                                    FROM Activity
                                    JOIN TrackPoint ON Activity.id = TrackPoint.activity_id
                                    GROUP BY Activity.id, Activity.user_id
                                ) AS activity_tp
                                ON User.id = activity_tp.user_id
                                GROUP BY User.id
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
        query = '''SELECT User.id,
                        COUNT(Activity.id) AS number_of_activities
                    FROM User
                    JOIN Activity
                    ON User.id = Activity.user_id
                    GROUP BY User.id
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
        # Activity ID is primary key, so it cannot be duplicate. We instead compare all other attributes.
        query = '''SELECT user_id, transportation_mode, start_date_time, end_date_time, 
                          COUNT(*) AS duplicates
                   FROM Activity
                   GROUP BY user_id, transportation_mode, start_date_time, end_date_time
                   HAVING COUNT(*) > 1'''

        return self.execute_query(query)

    def task_6(self):
        task_num = 6
        print_question(task_num=task_num,
                       question_text='Find activities that are registered multiple times.\n'
                                     'You should find the query even gives zero result.')
        result = pd.DataFrame(self.get_duplicate_activities(), columns=["User", "Activity ID", "Number of Duplicates"])
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
                    WHERE DATEDIFF(end_date_time, start_date_time) = 1
                    ORDER BY user_id, duration_in_minutes DESC;'''
        return self.execute_query(query)

    def task_7(self):
        task_num = 7
        # a
        print_question(task_num=task_num,
                       question_text='a) Find the number of users that have started an activity in one day and ended '
                                     'the activity the next day.')

        result = {"Number of multi-day activity users": self.get_count_multiple_day_activities()}

        print_result(result, filename=f"task_{task_num}a")

        # b
        print_question(task_num=task_num,
                       question_text='b) List the transportation mode, user id and duration for these activities.')

        result = pd.DataFrame(self.get_list_multiple_day_activities(),
                              columns=['User', 'Activity ID', 'Transportation Mode', 'Activity duration (minutes)'])

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
        query_trackpoints = f"SELECT activity_id, lat, lon, altitude FROM TrackPoint WHERE activity_id IN ({placeholders});"
        all_trackpoints = self.execute_query(query_trackpoints, tuple(unique_activity_ids))

        # Organize trackpoints by activity id
        trackpoints_dict = {}
        for activity_id, lat, lon, altitude in all_trackpoints:
            if activity_id not in trackpoints_dict:
                trackpoints_dict[activity_id] = []
            trackpoints_dict[activity_id].append((lat, lon, altitude))

        # 3. SPATIAL FILTERING USING R-TREE
        def spatially_close(tp1_list, tp2_list):
            idx = index.Index()
            for pos, (lat, lon, _) in enumerate(tp1_list):
                idx.insert(pos, (lat, lon, lat, lon))

            for lat, lon, altitude in tp2_list:
                nearby = list(idx.intersection((lat - 0.0005, lon - 0.0005, lat + 0.0005, lon + 0.0005)))
                for nearby_id in nearby:
                    coord_dist = haversine(tp1_list[nearby_id][:2], (lat, lon), unit=Unit.METERS)
                    altitude_dist = np.abs(altitude - tp1_list[nearby_id][2]) * 0.3048
                    euclidean_distance = np.sqrt(coord_dist**2 + altitude_dist**2)

                    if euclidean_distance <= 50:
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
        print_result(result, filename=f"task_{task_num}")

    # TASK 9
    def get_top_altitude_gains(self):
        """
            Retrieves the top 15 users who have gained the most altitude meters.

            This function calculates the total altitude gained by each user by comparing the altitude of consecutive
            trackpoints within the same activity. It ensures that altitude differences between the last trackpoint of
            a previous activity and the first trackpoint of a subsequent activity are not included in the calculation.
            The result is then converted from feet to meters and the top 15 users with the highest altitude gains are
            returned.

            :return: A list of the top 15 users with their respective total altitude gains in meters, ordered in descending
                     order of altitude gained.
            """
        query = '''
        WITH CurrentAndPreviousAltitudes AS (
            SELECT Activity.user_id,
                   TrackPoint.altitude AS current_altitude,
                   LAG(TrackPoint.altitude) OVER(
                       PARTITION BY TrackPoint.activity_id ORDER BY TrackPoint.date_time) AS previous_altitude
            FROM TrackPoint
            JOIN Activity ON TrackPoint.activity_id = Activity.id
            WHERE TrackPoint.altitude IS NOT NULL)   
    
        SELECT user_id AS id,
            ROUND(SUM(IF(current_altitude > previous_altitude, current_altitude - previous_altitude, 0)) * 0.3048)
                AS total_meters_gained
        FROM CurrentAndPreviousAltitudes
        WHERE previous_altitude IS NOT NULL
        GROUP BY user_id
        ORDER BY total_meters_gained DESC
        LIMIT 15;'''

        return self.execute_query(query)

    def task_9(self):
        task_num = 9
        print_question(task_num=task_num,
                       question_text='Find the top 15 users who have gained the most altitude meters.\nOutput should '
                                     'be a table with (id, total meters gained per user). Remember that some '
                                     'altitude-values are invalid')
        result = pd.DataFrame(self.get_top_altitude_gains(), columns=['User', 'Altitude Gained (meters)'])
        print_result(result, filename=f"task_{task_num}")

    # TASK 10
    def get_longest_distance_per_transportation(self):
        """
            Retrieves the users who have traveled the longest total distance in a single day for each transportation
            mode.

            This function calculates the total distance traveled by each user for each transportation mode on a given
            day by comparing the coordinates of consecutive trackpoints within the same activity and day, and then
            summing up the distances for each user and transportation mode. The function then determines the user who
            has traveled the longest distance for each transportation mode.

            :return: A list of users along with their respective transportation modes and the total distance traveled in
                     kilometers. Each entry in the list is in the format: [user_id, transportation_mode, distance]. The
                     results are ordered by transportation mode.
            """
        query = '''
                SELECT Activity.user_id, Activity.transportation_mode, TrackPoint.lat, TrackPoint.lon, 
                       DATE(Activity.start_date_time) AS travel_day
                FROM Activity
                JOIN TrackPoint ON Activity.id = TrackPoint.activity_id
                WHERE TIMESTAMPDIFF(SECOND , Activity.start_date_time, Activity.end_date_time) <= 86400 -- Seconds in a day
                ORDER BY Activity.user_id, Activity.transportation_mode, TrackPoint.date_time;
            '''
        data = self.execute_query(query)
        result = pd.DataFrame(data, columns=['user_id', 'transportation_mode', 'lat', 'lon', 'travel_day'])

        distances = []
        for i in range(1, len(result)):
            if (result['user_id'][i] == result['user_id'][i - 1]
                    and result['transportation_mode'][i] == result['transportation_mode'][i - 1]
                    and result['travel_day'][i] == result['travel_day'][i - 1]):
                coord1 = (result['lat'][i - 1], result['lon'][i - 1])
                coord2 = (result['lat'][i], result['lon'][i])
                distance = haversine(coord1, coord2, unit='km')
                distances.append(
                    (result['user_id'][i], result['transportation_mode'][i], distance))

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
            output.append([user, mode, distance])

        return output

    def task_10(self):
        task_num = 10
        print_question(task_num=task_num,
                       question_text="Find the users that have traveled the longest total distance in one "
                                     "day for each transportation mode.")
        result = pd.DataFrame(self.get_longest_distance_per_transportation(),
                              columns=['User ID', 'Transportation Mode', 'Distance in km'])
        print_result(result, filename=f"task_{task_num}")

    # TASK 11
    def get_invalid_activities(self):
        query = """
        WITH TrackpointDifferences AS (
        SELECT TrackPoint.activity_id,
               (TIMESTAMPDIFF(SECOND,
                    LAG(TrackPoint.date_time) 
                    OVER(PARTITION BY TrackPoint.activity_id ORDER BY TrackPoint.date_time),
                    TrackPoint.date_time) / 60.0) AS time_difference

        FROM TrackPoint),
    
        InvalidActivity AS (
            SELECT activity_id
            FROM TrackpointDifferences
            WHERE time_difference >= 5.0
            GROUP BY activity_id
        )
    
        SELECT Activity.user_id, COUNT(DISTINCT InvalidActivity.activity_id) AS invalid_activity_count
        FROM InvalidActivity
        JOIN Activity ON InvalidActivity.activity_id = Activity.id
        GROUP BY Activity.user_id
        ORDER BY Activity.user_id;"""

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
        SELECT User.id as user_id, Activity.transportation_mode AS transportation_mode, COUNT(*) AS amount
        FROM User
        JOIN Activity on User.id = Activity.user_id
        WHERE User.has_labels IS NOT NULL AND Activity.transportation_mode IS NOT NULL
        GROUP BY User.id, Activity.transportation_mode),
    
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
