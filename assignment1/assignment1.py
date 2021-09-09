import pandas as pd 
import datetime as dt

HISTORY_FILEPATH = "data/playhistory.csv"

def songs_listened_for_date(year, month):
    input_file = pd.read_csv(HISTORY_FILEPATH,header=0,parse_dates=["datetime"])
    output = {}
    #Mapping
    for line in input_file.values:
        track_id,user,datetime=line
        #reducing 
        # just a simple check on data integrity of datetime column
        if isinstance(datetime, dt.datetime):
            if datetime.year == year and datetime.month == month:
                if not track_id in output:
                    output[track_id] = 1
                else:
                    output[track_id] = output[track_id]+1     
    sorted_keys_list = list(output.keys())
    sorted_keys_list.sort()        
    sorted_output = []
    for key in sorted_keys_list:
        sorted_output.append({key :output[key]})
    return sorted_output

YEAR = 2015
MONTH = 3
print(songs_listened_for_date(YEAR, MONTH))