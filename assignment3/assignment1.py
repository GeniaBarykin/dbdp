import csv
from datetime import datetime as dt
from map_reduce_lib import MapReduce

PATH = "access_log/access_log"


def mapper(line):
    output = []
    
    ip_address = line.split(" ")[0]
    output.append((ip_address, 1))

    return output 


def reducer(key_value_item):
    ip_address, occurances = key_value_item
    return (ip_address, sum(occurances))

if __name__ == '__main__':
    data = []
    with open(PATH, 'r') as f:
        lines = f.readlines()

        map_reduce = MapReduce(mapper, reducer, 8)
        data = map_reduce(lines, debug=True)



    for ip_address, visits in data:
        print(f"{ip_address} visited {visits} times.")
