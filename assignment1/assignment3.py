import csv
from datetime import datetime as dt
import sys
import operator

sys.path.append("../")
from map_reduce_lib import *


def map_line_to_song_between_7_8(line):
    output = []

    track_id, user, datetime = line
    dateObj = dt.strptime(datetime, "%Y-%m-%d %H:%M:%S")

    if dateObj.hour == 7:
        output.append((track_id, 1))
    print(output)
    return output

def reduce_song_count(input):

    track_id, occurrences = input

    return (track_id, sum(occurrences))


if __name__ == '__main__':
    # Read data into separate lines.
    with open('data/playhistory.csv') as file_input:
        playhistory = csv.reader(file_input, delimiter=',')
        next(playhistory, None)  # skip the headers
        lines = list(playhistory)

        # Execute MapReduce job in parallel.
        map_reduce = MapReduce(map_line_to_song_between_7_8, reduce_song_count, 8)
        song_counts = map_reduce(lines, debug=True)

        # Sort the result in reverse order
        song_counts.sort(key=operator.itemgetter(1))
        song_counts.reverse()
        top5 = song_counts[:5]

        with open('data/tracks.csv', "r", encoding="UTF-8") as file_input:
            tracks_reader = csv.reader(file_input, delimiter=',')
            next(tracks_reader, None)  # skip the headers
            tracks = list(tracks_reader)

            print('Top 5 songs between 7AM and 8AM:')
            for track_id, count in top5:

                for t_id, t_artist, t_title, t_length in tracks:
                    if t_id == track_id:
                        # song_title perfored by artist_name: number_of_times_played
                        print(t_title + " performed by " + t_artist + ": " + str(count))
