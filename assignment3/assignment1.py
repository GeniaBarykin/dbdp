import csv
from datetime import datetime as dt
from map_reduce_lib import MapReduce

PATH = "access_log/access_log"

def mapper(line):
    output=[]
    return output

def reducer(key_value_item):
    output=[]
    return output

if __name__ == '__main__':
    data=[]
    with open(PATH, 'r') as f:
        reader = csv.reader(f,delimiter=' ')
        data=list(reader)
        map_reduce = MapReduce(mapper, reducer, 8)
        data = map_reduce(data, debug=True)
            

