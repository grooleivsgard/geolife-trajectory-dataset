import pandas as pd
import os


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


def retrieve_user(user_dir: os.DirEntry[str], labled_ID: list):
    labels = False
    if user_dir.name in labled_ID:
        labels = True

    return {
        "id": user_dir.name,
        "has_labels": labels,
        "path": user_dir.path
    }


def process_users(path, labeled_ids):
    user_rows = []
    with os.scandir(path) as users:
        for user in users:
            if user.is_dir():
                user_row = retrieve_user(user, labeled_ids)
                user_rows.append(user_row)

    # Insert user
    return user_rows


def preprocess_activities(user_row):
    activity_rows = []
    with os.scandir(user_row['path'] + "/Trajectory") as activities:
        for activity in activities:
            if activity.is_file():
                activity = {
                    "user_id": user_row["id"],
                    "path": activity.path
                }
                activity_rows.append(activity)
    return activity_rows


def process_activity(user_row, activity_row):
    columns = ['lat', 'lon', 'dep1', 'alt', 'date', 'date_str', 'time_str']
    trackpoints_df = pd.read_table(activity_row['path'], skiprows=7, names=columns, delimiter=',')

    if trackpoints_df.shape[0] > 2500:  # check if rows not columns
        return None, None

    # Build activity
    activity = {
        'user_id': activity_row['user_id'],
        'transportation_mode': None,
        'start_date_time': pd.Timestamp(trackpoints_df['date_str'].iloc[0] + " " + trackpoints_df['time_str'].iloc[0]),
        'end_date_time': pd.Timestamp(trackpoints_df['date_str'].iloc[-1] + " " + trackpoints_df['time_str'].iloc[-1]),
    }

    # Add transport type if matching
    if user_row['has_labels']:
        transportations = pd.read_table(user_row['path'] + "/labels.txt")
        transportations['Start Time'] = pd.to_datetime(transportations['Start Time'])
        transportations['End Time'] = pd.to_datetime(transportations['End Time'])

        time_tolerance = pd.Timedelta(seconds=5)
        matching_transport = transportations[
            (transportations['Start Time'].between(activity['start_date_time'] - time_tolerance, activity['start_date_time'] + time_tolerance)) &
            (transportations['End Time'].between(activity['end_date_time'] - time_tolerance, activity['end_date_time'] + time_tolerance))
        ]

        if not matching_transport.empty:
            activity['transportation_mode'] = matching_transport['Transportation Mode'].iloc[0]
            print("Match found!")

    return activity, trackpoints_df



def process_trackpoints(activity_id, trackpoints_df):
    trackpoints = []
    for _, trackpoint in trackpoints_df.iterrows():
        trackpoints.append({
            'activity_id': activity_id,
            'lat': trackpoint['lat'],
            'lon': trackpoint['lon'],
            'altitude': trackpoint['alt'] if trackpoint['alt'] != -777 else None,
            'date_days': trackpoint['date'],
            'date_time': trackpoint['date_str'] + " " + trackpoint['time_str']
        })
    return trackpoints
