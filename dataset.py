import pandas as pd
import os

columns = ['lat', 'lon', 'dep1', 'alt', 'date', 'date_str', 'time_str']
#test = pd.read_table('geolife_ds/Geolife Trajectories 1.3/Data/000/Trajectory/20081023025304.plt', skiprows=7, names=columns, delimiter=',')
# Sjekk mappe 'Data' og lag rad for hvert mappenavn (= user_id).
# If user_id er i 'Data/labeled_ids.txt'
#   user.has_labels = True
# Gå inn i hver user mappe, og opprett aktivitet i table_name 'Activity' for hver '.plt' fil. Oppdater user_id, start_time, end_time.
# If user.has_labels = true { 
#   loop gjennom '/Data/user_id/labels.txt', og sjekk om noen av aktivitetene har eksakt overlapp med start_time og end_time i user.activity
#       if overlapp
#           activity.transportation_mode = (transportation mode)
#       else 
#           activity.transportation_mode = NULL
# Else
#   transportation_mode = NULL
# For hver .plt fil (1 .plt fil = en aktivitet) i Trajectory where antall rader <= 2500(unntatt headers)
#   For hver rad i .plt filen
#       Opprett ny Trackpoint med lat, long, altitude, date_days, date_time (mulig dette kan gjøres på en raskere måte?)

def insert(path):
    for dirpath, dirname, filenames in os.walk(path):
        print(f"Found directory: {dirpath}")
        
                        
#activities.append(pd.read_table(path, skiprows=7, names=columns, delimiter=',')

"""
def process_label(user, activity):
     labels = []
     if user.has_label : true
        for entry in labels.txt
            if start_time && end_time i labels.txt == start_time && end_time i activities
                update label i activity.transportation_mode til 'Transportation Mode'
    else activity.transportation_mode = NULL
        
"""

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
    
def process_users(path):
    with os.scandir(path) as users:
        for user in users:
            if user.is_dir():
                print(f"Processing user: {user.name}")
                print(f'')
                process_trajectories(user.path)

def process_trajectories(sub_dir_path):
    with os.scandir(sub_dir_path) as entries:
        for entry in entries:
            if entry.is_file():
                print(f"Processing file: {entry.name} in directory: {sub_dir_path}")
                
                
def retrieve_user(user_dir:os.DirEntry[str], labled_ID:list):
    labels = 'false'
    if user_dir.name in labled_ID:
        labels = "true"
    
    return {
        "id": user_dir.name,
        "has_labels": labels
    }
    
    
data_path = './dataset/Data'
labeled_ids= read_file_to_list('./dataset/labeled_ids.txt')

print(labeled_ids)