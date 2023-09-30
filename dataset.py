import pandas as pd
import os
from queue import Queue


def read_file_to_list(file_path):
    """
    Open a text file and extract each line as a string to a list.

    :param file_path: Path to the text file.
    :return: List of strings, each representing a line in the text file.
    """
    try:
        with open(file_path, 'r') as file:
            # Reading each line
            lines_list = file.readlines()

            # Removing any leading and trailing whitespaces from each line
            lines_list = [line.strip() for line in lines_list]

        return lines_list

    except Exception as e:
        print(f"An error occurred: {e}")
        return None


def process_users(path, labeled_ids):
    user_rows = []
    with os.scandir(path) as users:
        for user in users:
            if user.is_dir():
                user_row = {
                    "id": user.name,
                    "has_labels": True if user.name in labeled_ids else False,
                    "meta": {
                        "path": user.path
                    }
                }
                user_rows.append(user_row)

    # Insert user
    return user_rows


def preprocess_activities(user_row):
    activity_rows = []
    with os.scandir(user_row['meta']['path'] + "/Trajectory") as activities:
        for activity in activities:
            if activity.is_file():
                activity = {
                    "id": int(user_row["id"] + activity.name[:-4]),
                    "user_id": user_row["id"],
                    'transportation_mode': None,
                    "meta": {
                        "name": activity.name,
                        "path": activity.path
                    }
                }
                activity_rows.append(activity)
    return activity_rows


def process_activity(user_row, activity_row):
    columns = ['lat', 'lon', 'dep1', 'alt', 'date', 'date_str', 'time_str']
    trackpoints_df = pd.read_table(activity_row['meta']['path'], skiprows=7, names=columns, delimiter=',')

    if trackpoints_df.shape[0] > 2500:  # check if rows not columns
        return None, None

    # Expand activity
    activity_row['start_date_time'] = trackpoints_df['date_str'].iloc[0] + " " + trackpoints_df['time_str'].iloc[0]
    activity_row['end_date_time'] = trackpoints_df['date_str'].iloc[-1] + " " + trackpoints_df['time_str'].iloc[-1]
    activity_row['start_date_time'] = pd.to_datetime(activity_row['start_date_time'])
    activity_row['end_date_time'] = pd.to_datetime(activity_row['end_date_time'])
    if user_row['has_labels']:
        transportations = pd.read_table(user_row['meta']['path'] + "/labels.txt")
        transportations['Start Time'] = pd.to_datetime(transportations['Start Time'])
        transportations['End Time'] = pd.to_datetime(transportations['End Time'])

        time_tolerance = pd.Timedelta(seconds=1)
        matching_transport = transportations[
            (transportations['Start Time'].between(activity_row['start_date_time'] - time_tolerance, activity_row['start_date_time'] + time_tolerance)) &
            (transportations['End Time'].between(activity_row['end_date_time'] - time_tolerance, activity_row['end_date_time'] + time_tolerance))
        ]

        if not matching_transport.empty:
            activity_row['transportation_mode'] = matching_transport['Transportation Mode'].iloc[0]
    return activity_row, trackpoints_df


def process_trackpoint(activity_id, trackpoint_row):
    return {
        'activity_id': activity_id,
        'lat': trackpoint_row['lat'],
        'lon': trackpoint_row['lon'],
        'altitude': trackpoint_row['alt'] if trackpoint_row['alt'] != -777 else None,
        'date_days': trackpoint_row['date'],
        'date_time': trackpoint_row['date_str'] + " " + trackpoint_row['time_str']
    }