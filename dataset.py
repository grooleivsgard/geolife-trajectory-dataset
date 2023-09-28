import pandas as pd
import os

columns = ['lat', 'lon', 'dep1', 'alt', 'date', 'date_str', 'time_str']

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
    labels = 'false'
    if user_dir.name in labled_ID:
        labels = "true"

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


def retrieve_activity(activity_dir: pd.DataFrame, activity_name,  user_row):
    activity = {
        "activity_id": user_row["id"] + "-" + activity_name,
        "user_id": user_row["id"],
        "start_time": activity_dir['date_str'].iloc[0] + " " + activity_dir['time_str'].iloc[0],
        "end_time": activity_dir['date_str'].iloc[-1] + " " + activity_dir['time_str'].iloc[-1],
    }

    # Check for transportation mode
    if user_row['has_labels'] == "true":
        transportations = pd.read_table(user_row['path'] + "/labels.txt")
        for index in transportations.index:
            activity['transportation_mode'] = transportations['Transportation Mode'].iloc[index] \
                if (activity['start_time'] == transportations['Start Time'].iloc[index]
                    and activity['end_time'] == transportations['End Time'].iloc[index]) \
                else "null"
    print(activity)


def process_activities(user_rows):
    for user_row in user_rows:

        with os.scandir(user_row['path'] + "/Trajectory") as activities:
            for activity in activities:
                if activity.is_file():

                    trackpoints = pd.read_table(activity.path, skiprows=7, names=columns, delimiter=',')

                    # Skip if larger than 2500 trackpoints
                    if trackpoints.shape[0] > 2500:
                        continue

                    retrive_activity(trackpoints, activity_name=activity.name,user_row=user_row)
                    exit()

def retrieve_trackpoints(trackpoints:pd.DataFrame, activity):
    trackpoint = {'activity_id':
                  ', 'lat DOUBLE', 'lon DOUBLE',
                       'altitude INT', 'date_days DOUBLE', 'date_time DATETIME'}
    for i, rows in trackpoints.iterrows():



data_path = './dataset/dataset/Data'
labeled_ids = read_file_to_list('./dataset/dataset/labeled_ids.txt')

process_users(data_path, labeled_ids)
