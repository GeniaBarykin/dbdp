import csv
from datetime import datetime as dt
import operator

from map_reduce_lib import *


def map_line_to_song_between_7_8(line):
    output = []

    if len(line) == 3: # This means the line contains playhistory information.
        track_id, user, datetime = line
        dateObj = dt.strptime(datetime, "%Y-%m-%d %H:%M:%S")

        if dateObj.hour == 7:
            output.append((track_id, 1))

        return output

    else: # This means the line contains track information.
        track_id, artist, title, lengthSeconds = line
        output.append((track_id, (track_id, artist, title, lengthSeconds)))

        return output


def reduce_song_count(key_value_item):


    track_id, value = key_value_item

    sum = 0
    track_info = None

    for v in value:
        if isinstance(v, int):
            sum += v
        else:
            track_info = v

    return (track_info, sum)


if __name__ == '__main__':
    # Read data into separate lines.
    with open('data/playhistory.csv') as ph_input, open('data/tracks.csv', "r", encoding="UTF-8") as t_input:
        playhistory = csv.reader(ph_input, delimiter=',')
        next(playhistory, None)  # skip the headers
        tracks = csv.reader(t_input, delimiter=',')
        next(tracks, None)  # skip the headers
        
        # read in all the data
        lines = list(playhistory)
        lines.extend(list(tracks))

        # Execute MapReduce job in parallel.
        map_reduce = MapReduce(map_line_to_song_between_7_8, reduce_song_count, 8)
        song_counts = map_reduce(lines, debug=True)

        # Sort the result in reverse order
        song_counts.sort(key=operator.itemgetter(1))
        song_counts.reverse()
        top5 = song_counts[:5]

        for track_info, count in top5:
            # song_title perfored by artist_name: number_of_times_played
            print(track_info[2] + " performed by " + track_info[1] + ": " + str(count))
