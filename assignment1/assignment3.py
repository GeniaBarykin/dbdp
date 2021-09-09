import pandas as pd 
import datetime as dt

SONG_FILEPATH = "data/tracks.csv"
HISTORY_FILEPATH = "data/playhistory.csv"

def songs_per_time_period_map(start_hour, end_hour):
    input_file = pd.read_csv(HISTORY_FILEPATH,header=0,parse_dates=["datetime"])
    data_map = {}
    for line in input_file.values:
        track_id,user,datetime=line
        # First reducer and hek on data integrity of datetime column
        if isinstance(datetime, dt.datetime) and datetime.hour >= start_hour and datetime.hour < end_hour:            
            if not track_id in data_map:
                data_map[track_id] = 1
            else:
                data_map[track_id] += 1
    return data_map


def songs_per_time_period_reducer(data_map, size):   
    output = []
    lowest_val = 0
    highest_val = 0
    for i in range(size):
        output.append({str(i+1): 0})
    for song in data_map:
        # Merge algorithm 
        if data_map[song] > lowest_val:
            output.pop(0)    
            # Put highest value on top   
            if data_map[song] >= highest_val:
                output.append({song:data_map[song]})
                highest_val=list(output[size-1].values())[0]
            else:
                start_value=list(output[0].values())[0]
                # Put lowest value on bottom
                if data_map[song] < start_value:
                    output.insert(0, {song:data_map[song]})
                    lowest_val=list(output[0].values())[0]
                # Or find a place withing the list keeping it sorted
                else:
                    lowest_val=start_value
                    i = 1
                    while data_map[song] > start_value:
                        start_value=list(output[i].values())[0]
                        i+=1
                    output.insert(i, {song:data_map[song]})            
    return output


# Combines data of reducer with data from songs table
def top_songs_info(songs_per_time_period):
    output = []
    input_file = pd.read_csv(SONG_FILEPATH, header=0)
    # Output in a JSON array as there are several data columns
    merged_songs_per_time_period = {}
    for line in songs_per_time_period:
        merged_songs_per_time_period.update(line)    
    for line in input_file.values:
        track_id,artist,title,lengthSeconds = line
        if track_id in merged_songs_per_time_period:
            output.append({"title" :title, "Artist" : artist,
                       "times_played" : merged_songs_per_time_period[track_id]}) 
    return output
    # Songtitle, ArtistName, NumberOfTimesPlayed

START_HOUR = 7
END_HOUR = 8
OUTPUT_SIZE = 5               
# print()
input_file = pd.read_csv("data/tracks.csv", header=0)
    # Output in a JSON array as there are several data columns
print(top_songs_info(songs_per_time_period_reducer(songs_per_time_period_map(START_HOUR,END_HOUR), OUTPUT_SIZE)))