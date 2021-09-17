import csv
from datetime import datetime as dt
from map_reduce_lib import *

PEOPLE_FILEPATH = "data/people.csv"
HISTORY_FILEPATH = "data/playhistory.csv"
TRACKS_FILEPATH = "data/tracks.csv"

#1
def mapper_track_to_users_and_authors(line):
    if len(line) == 3:
        track_id,user_id,datetime=line        
        return [(track_id, int(user_id))]
    elif len(line) == 4:
        track_id,artist,title,lengthSeconds=line
        return [(track_id, artist)] 
    elif len(line) == 7:
        user_id,first_name,last_name,email,gender,country,dob=line
        return [(int(user_id), (first_name, last_name))]

def reducer_authors_to_users(key_value_item):
    some_id, data = key_value_item 
    output = None    
    users_listened_author = []
    if isinstance(some_id, str):    
        for element in data:           
            if isinstance(element, int): 
                users_listened_author.append(element)
            if isinstance(element, str):
                author=element
        return (author,users_listened_author)
    elif isinstance(some_id, int): 
        return (some_id, data.pop(0))
    
#2
def mapper_user_to_author(line):
    some_id, data = line
    output = []
    if isinstance(some_id, str): 
        for user in data :
            output.append((user, some_id))
    elif isinstance(some_id, int):
        output.append((some_id, data))
    return output

def reducer_user_fav_author(key_value_item):
    user_id, data = key_value_item
    initials = data.pop(0)
    authors = data
    listened_authors_count={}
    for author in authors:
        if author in listened_authors_count:
            listened_authors_count[author]+=1
        else:
            listened_authors_count[author]=1 
    fav_author= None
    times_listened = 0
    for author in listened_authors_count.keys():
        if listened_authors_count[author] > times_listened:
            fav_author= author
            times_listened = listened_authors_count[author]
    return (initials[0], initials[1], fav_author, times_listened)
    
if __name__ == '__main__':
    data=[]
    # Read data into separate lines.
    with open (HISTORY_FILEPATH, 'r') as history_input_file, open (TRACKS_FILEPATH, 'r', encoding="UTF-8") as tracks_input_file:
        with open (PEOPLE_FILEPATH, 'r') as people_input_file:
            people_reader = csv.reader(people_input_file, delimiter=',')
            next(people_reader, None)  # skip the headers
            data =list(people_reader)
            history_reader = csv.reader(history_input_file, delimiter=',')
            next(history_reader, None)  # skip the headers
            data +=list(history_reader)        
            tracks_reader = csv.reader(tracks_input_file, delimiter=',')
            next(tracks_reader, None)  # skip the headers
            data +=list(tracks_reader)            
            # # Execute MapReduce job in parallel.
            map_reduce = MapReduce(mapper_track_to_users_and_authors, reducer_authors_to_users, 8)
            data = map_reduce(data, debug=True)
            map_reduce = MapReduce(mapper_user_to_author, reducer_user_fav_author, 8)
            data = map_reduce(data, debug=True)
    for line in data:
        print(line)