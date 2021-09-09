import pandas as pd 
import datetime as dt

PEOPLE_FILEPATH = "data/people.csv"
HISTORY_FILEPATH = "data/playhistory.csv"

def users_favorite_hour_map():
    input_file = pd.read_csv(HISTORY_FILEPATH,header=0,parse_dates=["datetime"])
    data_map = {}
    # Mapping
    for line in input_file.values:
        track_id,user,datetime=line
        # just a simple check on data integrity of datetime column
        if isinstance(datetime, dt.datetime):   
            # For each user calculates how many times he listened songs for each hour         
            if not user in data_map:
                data_map[user] = { datetime.hour : 1}
            elif datetime.hour in data_map[user]:
                data_map[user][datetime.hour] = data_map[user][datetime.hour]+1
            else:
                data_map[user][datetime.hour] = 1
    return data_map

def users_favorite_hour_reducer(data_map):
    output = {}
    # For each user check all hours he listened music
    for user in data_map:
        fav_hour = 0
        number_of_times = 0        
        # Select the most listened hour
        for hour in data_map[user]:
            if data_map[user][hour] > number_of_times:
                fav_hour=hour
                number_of_times = data_map[user][hour]
        output[user] = { hour : number_of_times}
    
    return output

# Combines data of favorite hour with data from people table
def user_info_with_favorite_hour(fav_hour_data):
    output = []
    input_file = pd.read_csv(PEOPLE_FILEPATH,header=0)
    # Output in a JSON array as there are several data columns
    for line in input_file.values:
        person_id,first_name,last_name,email,gender,country,dob = line
        output.append({"first_name" :first_name, "last_name" : last_name,
                       "favorite_hour" : fav_hour_data[person_id],
                       "songs listened" : list(fav_hour_data[person_id].values())[0]}) 
    return output


print(user_info_with_favorite_hour(users_favorite_hour_reducer(users_favorite_hour_map())))
