import pandas as pd 
import datetime as dt

HISTORY_FILEPATH = "data/playhistory.csv"

def songs_listened_for_date(year, month):
    input_file = pd.read_csv(HISTORY_FILEPATH,header=0,parse_dates=["datetime"])
    sorted_input = input_file.sort_values(["track_id"])
    output = {}
    #Mapping
    for line in sorted_input.values:
        track_id,user,datetime=line
        #reducing
        if isinstance(datetime, dt.datetime):
            if datetime.year == year and datetime.month == month:
                if not track_id in output:
                    output[track_id] = 1
                else:
                    output[track_id] = output[track_id]+1            
    return output

YEAR = 2015
MONTH = 3
print(songs_listened_for_date(YEAR, MONTH))