import csv
from datetime import datetime as dt
from map_reduce_lib import *

PEOPLE_FILEPATH = "data/people.csv"
HISTORY_FILEPATH = "data/playhistory.csv"

def mapper(line):
    """ Map function to get tracks for the certain date
    """
    process_print('is processing `%s`' % line)
    output = []
    if len(line) == 3:
        track_id,user_id,datetime=line        
        dateObj = dt.strptime(datetime, "%Y-%m-%d %H:%M:%S")
        output.append((user_id, dateObj.hour))
    elif len(line) == 7:
        user_id,first_name,last_name,email,gender,country,dob=line
        output.append((user_id,{first_name,last_name}))       
    return output

def reducer(key_value_item):
    """ Reduce function for the word count job. 
    Converts partitioned data (key, [value]) to a summary of form (key, value).
    """  
    user_id, data = key_value_item 
    user_info = None
    user_hours = {}
    for element in data:
        if isinstance(element, set): 
            user_info=element
        else:
            if not element in user_hours.keys():
                user_hours[element]=1
            else:
                user_hours[element]+=1
    max_listened_value = max(user_hours.values())
    fav_hour = None
    for key in user_hours.keys():
        if user_hours[key]==max_listened_value:
            fav_hour = key
            break       
    return (user_info.pop(),user_info.pop(),fav_hour, max_listened_value)

if __name__ == '__main__':
    data=[]
    # Read data into separate lines.
    with open (HISTORY_FILEPATH, 'r') as history_input_file, open (PEOPLE_FILEPATH, 'r') as people_input_file:
        history_reader = csv.reader(history_input_file, delimiter=',')
        next(history_reader, None)  # skip the headers
        data = list(history_reader)        
        people_reader = csv.reader(people_input_file, delimiter=',')
        next(people_reader, None)  # skip the headers
        data+=list(people_reader)            
        # Execute MapReduce job in parallel.
        
        map_reduce = MapReduce(mapper, reducer, 8)
        data = map_reduce(data, debug=True)
    for line in data:
        print(line)