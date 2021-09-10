import csv
from datetime import datetime as dt
import sys
import operator
from map_reduce_lib import *

HISTORY_FILEPATH = "data/playhistory.csv"
YEAR = 2015
MONTH = 3

def map_lines_to_words(line):
    """ Map function to get tracks for the certain date
    """
    process_print('is processing `%s`' % line)

    output = []
    track_id,user,datetime=line
    
    dateObj = dt.strptime(datetime, "%Y-%m-%d %H:%M:%S")
    if dateObj.year == YEAR and dateObj.month == MONTH:
        output.append((track_id, 1))
       
    return output

def reduce_word_count(key_value_item):
    """ Reduce function for the word count job. 
    Converts partitioned data (key, [value]) to a summary of form (key, value).
    """
    track_id, occurrences = key_value_item
    return (track_id, sum(occurrences))

if __name__ == '__main__':
   
    # Read data into separate lines.
    with open (HISTORY_FILEPATH, 'r') as input_file:
        tracks_reader = csv.reader(input_file, delimiter=',')
        next(tracks_reader, None)  # skip the headers
        tracks = list(tracks_reader)
    
        # Execute MapReduce job in parallel.
        map_reduce = MapReduce(map_lines_to_words, reduce_word_count, 8)
        date_counts = map_reduce(tracks, debug=True)
        print('Songs played on ' + str(MONTH) + '/'+ str(YEAR) )
        for track_id, played_times in date_counts:
            print("Song id " + track_id + ", played times:"+ str(played_times))
    # # Sort the result in reverse order
    # word_counts.sort(key=operator.itemgetter(1))
    # word_counts.reverse()
    # top20 = word_counts[:20]
    
    # print('Top 20 words by frequency:')
    # for word, count in top20:
    #     print('{:10s}: {}'.format(word, count))