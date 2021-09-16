import csv
from datetime import datetime as dt
from map_reduce_lib import *

PEOPLE_FILEPATH = "data/people.csv"
HISTORY_FILEPATH = "data/playhistory.csv"
TRACKS_FILEPATH = "data/tracks.csv"

#1
def mapper_track_to_users_and_authors(line):
    output = []
    if len(line) == 3:
        track_id,user_id,datetime=line        
        output.append((track_id, int(user_id)))
    elif len(line) == 4:
        track_id,artist,title,lengthSeconds=line
        output.append((track_id, artist))   
        
    return output

def reducer_authors_to_users(key_value_item):
    track_id, data = key_value_item 
    users_listened_author=[]
    author = None
    for element in data:           
        if isinstance(element, int): 
            users_listened_author.append(element)
        if isinstance(element, str):
            author=element
    return (author,users_listened_author)
#2
def mapper_user_to_author(line):
    author, users = line
    output = []
    for user in users:
        output.append((user, author))
    return output

def reducer_user_fav_author(key_value_item):
    user, authors = key_value_item
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
    return (user, fav_author, times_listened)

#3
def mapper_user_to_info(line):
    output = []  
    if len(line) == 3:
        user_id, author, times_listened = line      
        output.append((user_id, (author, times_listened)))             
    elif len(line) == 7:
        user_id,first_name,last_name,email,gender,country,dob=line
        output.append((int(user_id), (first_name, last_name))) 
    return output

def reducer_user_info(key_value_item): 
    output = []
    user_id, data = key_value_item
    output.append({"fist_name" : data[1][0],"second_name" : data[1][1], "fav_author" : data[0][0],"times_listened" : data[0][1]})
    return output

if __name__ == '__main__':
    data=[]
    # Read data into separate lines.
    with open (HISTORY_FILEPATH, 'r') as history_input_file, open (TRACKS_FILEPATH, 'r', encoding="UTF-8") as tracks_input_file:
        history_reader = csv.reader(history_input_file, delimiter=',')
        next(history_reader, None)  # skip the headers
        data =list(history_reader)        
        tracks_reader = csv.reader(tracks_input_file, delimiter=',')
        next(tracks_reader, None)  # skip the headers
        data+=list(tracks_reader)            
        # # Execute MapReduce job in parallel.
        map_reduce = MapReduce(mapper_track_to_users_and_authors, reducer_authors_to_users, 8)
        data = map_reduce(data, debug=True)
        map_reduce = MapReduce(mapper_user_to_author, reducer_user_fav_author, 8)
        data = map_reduce(data, debug=True)
        with open (PEOPLE_FILEPATH, 'r') as people_input_file:
            people_reader = csv.reader(people_input_file, delimiter=',')
            next(people_reader, None)  # skip the headers
            
            data+=list(people_reader)
            map_reduce = MapReduce(mapper_user_to_info, reducer_user_info, 8)
            data = map_reduce(data, debug=True)
    for line in data:
        print(line)