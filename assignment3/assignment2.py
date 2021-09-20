import csv
from datetime import datetime as dt
from map_reduce_lib import MapReduce

PATH = "access_log/access_log"

def mapper(line):
    output=[]
    line=line.split(" ")
    i = 2  
    try:
        ip=line[0]
        associates = line[2]
        date=None 
        while line[i]=='' or (i < len(line) and not line[i][0]=="["):        
            i += 1
        date=line[i][1:]
        month=date[3: 6:]
        year=date[7: 11:]  
        date=dt.strptime(month+":"+year, "%b:%Y")
    except:
        date="Line error "
        print(date,line,i,line[i][0])
        
    output.append((date, ip))
    return output

def reducer(key_value_item):
    output=[]
    date, ip = key_value_item
    occurances = {}
    max_occ=0
    max_key=None
    for adress in ip:
        if adress not in occurances:
            occurances[adress]=1
        else:
            occurances[adress]+=1
        if occurances[adress]>max_occ:
                max_occ=occurances[adress]
                max_key=adress 
    try:   
        output.append((date.month, date.year,max_key,max_occ))  
    except:
        output.append(("Error date",max_key,max_occ))
        print("reducer error ", ip, date)
    return output

if __name__ == '__main__':
    data=[]
    with open(PATH, 'r') as f:
        data=f.readlines()
        map_reduce = MapReduce(mapper, reducer, 16)
        data = map_reduce(data, debug=True)
        print(data)
            

